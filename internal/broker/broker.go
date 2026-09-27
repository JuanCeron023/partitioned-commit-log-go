// Package broker implements StreamHive's topic/partition model and consumer
// group offset tracking on top of the commit log.
//
// A topic is a set of partitions; each partition is an independent commit log
// that preserves ordering. A message's partition is chosen by hashing its key
// (so all events for the same entity land in order on one partition) or round
// -robin when no key is given. Consumer groups track a committed offset per
// partition, giving each group an independent, resumable read position — the
// same semantics that make Kafka useful for decoupling microservices.
package broker

import (
	"fmt"
	"hash/fnv"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/portfolio/streamhive/internal/commitlog"
)

// Message is what producers send and consumers receive.
type Message struct {
	Key       string    `json:"key,omitempty"`
	Value     []byte    `json:"value"`
	Partition int       `json:"partition"`
	Offset    uint64    `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

type topic struct {
	name       string
	partitions []*commitlog.Log
	rr         atomic.Uint64 // round-robin counter for keyless messages
}

// Broker is the top-level message broker.
type Broker struct {
	dir    string
	mu     sync.RWMutex
	topics map[string]*topic

	// group offsets: group -> topic -> partition -> committed offset
	offMu   sync.RWMutex
	offsets map[string]map[string]map[int]uint64

	produced atomic.Int64
}

// New creates a broker that stores partition logs under dir.
func New(dir string) *Broker {
	return &Broker{
		dir:     dir,
		topics:  make(map[string]*topic),
		offsets: make(map[string]map[string]map[int]uint64),
	}
}

// CreateTopic creates a topic with the given number of partitions. It is
// idempotent: creating an existing topic returns nil.
func (b *Broker) CreateTopic(name string, partitions int) error {
	if partitions <= 0 {
		partitions = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[name]; ok {
		return nil
	}
	t := &topic{name: name, partitions: make([]*commitlog.Log, partitions)}
	for i := 0; i < partitions; i++ {
		path := filepath.Join(b.dir, fmt.Sprintf("%s-%d.log", name, i))
		lg, err := commitlog.Open(path)
		if err != nil {
			return err
		}
		t.partitions[i] = lg
	}
	b.topics[name] = t
	return nil
}

// Produce appends a message to a topic. The partition is chosen by key hash,
// or round-robin if key is empty. Returns the partition and offset written.
func (b *Broker) Produce(topicName, key string, value []byte) (int, uint64, error) {
	b.mu.RLock()
	t, ok := b.topics[topicName]
	b.mu.RUnlock()
	if !ok {
		return 0, 0, fmt.Errorf("unknown topic %q", topicName)
	}

	var part int
	if key != "" {
		part = int(hashKey(key) % uint32(len(t.partitions)))
	} else {
		part = int(t.rr.Add(1)-1) % len(t.partitions)
	}
	off, err := t.partitions[part].Append(value)
	if err != nil {
		return 0, 0, err
	}
	b.produced.Add(1)
	return part, off, nil
}

// Consume reads up to max messages for a consumer group starting at its
// committed offset for the given partition. It does NOT auto-commit; the caller
// commits after successful processing (at-least-once).
func (b *Broker) Consume(group, topicName string, partition, max int) ([]Message, error) {
	b.mu.RLock()
	t, ok := b.topics[topicName]
	b.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown topic %q", topicName)
	}
	if partition < 0 || partition >= len(t.partitions) {
		return nil, fmt.Errorf("partition %d out of range", partition)
	}

	start := b.committedOffset(group, topicName, partition)
	recs, err := t.partitions[partition].ReadFrom(start, max)
	if err != nil {
		return nil, err
	}
	out := make([]Message, len(recs))
	for i, r := range recs {
		out[i] = Message{Value: r.Value, Partition: partition, Offset: r.Offset}
	}
	return out, nil
}

// Commit advances a group's committed offset for a partition to offset+1, so
// the next Consume starts after the last processed message.
func (b *Broker) Commit(group, topicName string, partition int, offset uint64) {
	b.offMu.Lock()
	defer b.offMu.Unlock()
	if b.offsets[group] == nil {
		b.offsets[group] = map[string]map[int]uint64{}
	}
	if b.offsets[group][topicName] == nil {
		b.offsets[group][topicName] = map[int]uint64{}
	}
	b.offsets[group][topicName][partition] = offset + 1
}

func (b *Broker) committedOffset(group, topicName string, partition int) uint64 {
	b.offMu.RLock()
	defer b.offMu.RUnlock()
	if g, ok := b.offsets[group]; ok {
		if tp, ok := g[topicName]; ok {
			return tp[partition]
		}
	}
	return 0
}

// --- Introspection for the dashboard ---------------------------------------

// PartitionInfo describes one partition's depth.
type PartitionInfo struct {
	Partition int    `json:"partition"`
	EndOffset uint64 `json:"end_offset"`
}

// TopicInfo describes a topic and its partitions.
type TopicInfo struct {
	Name       string          `json:"name"`
	Partitions []PartitionInfo `json:"partitions"`
}

// GroupLag reports how far behind a consumer group is on each partition.
type GroupLag struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Committed uint64 `json:"committed"`
	EndOffset uint64 `json:"end_offset"`
	Lag       uint64 `json:"lag"`
}

// Stats is the full broker snapshot for the dashboard.
type Stats struct {
	Produced int64       `json:"produced"`
	Topics   []TopicInfo `json:"topics"`
	Lags     []GroupLag  `json:"lags"`
}

// Stats gathers a consistent view of topics, partition depths, and group lag.
func (b *Broker) Stats() Stats {
	b.mu.RLock()
	topics := make([]TopicInfo, 0, len(b.topics))
	type tp struct {
		name string
		logs []*commitlog.Log
	}
	var order []tp
	for name, t := range b.topics {
		order = append(order, tp{name, t.partitions})
	}
	b.mu.RUnlock()

	endOf := map[string]map[int]uint64{}
	for _, t := range order {
		ti := TopicInfo{Name: t.name}
		endOf[t.name] = map[int]uint64{}
		for i, lg := range t.logs {
			end := lg.NextOffset()
			endOf[t.name][i] = end
			ti.Partitions = append(ti.Partitions, PartitionInfo{Partition: i, EndOffset: end})
		}
		topics = append(topics, ti)
	}

	var lags []GroupLag
	b.offMu.RLock()
	for group, byTopic := range b.offsets {
		for tname, byPart := range byTopic {
			for part, committed := range byPart {
				end := endOf[tname][part]
				var lag uint64
				if end > committed {
					lag = end - committed
				}
				lags = append(lags, GroupLag{
					Group: group, Topic: tname, Partition: part,
					Committed: committed, EndOffset: end, Lag: lag,
				})
			}
		}
	}
	b.offMu.RUnlock()

	return Stats{Produced: b.produced.Load(), Topics: topics, Lags: lags}
}

// Close closes all partition logs.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range b.topics {
		for _, lg := range t.partitions {
			_ = lg.Close()
		}
	}
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
