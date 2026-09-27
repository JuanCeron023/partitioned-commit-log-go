// Package commitlog implements a durable, append-only log — the storage
// primitive underneath every partition in StreamHive (and every log-based
// broker like Kafka).
//
// Records are length-prefixed and written sequentially to a segment file, so
// appends are O(1) and sequential (fast on disk). An in-memory offset index
// maps each record's monotonically increasing offset to its byte position,
// giving O(1) reads by offset. This is the same core idea Kafka uses; keeping
// it small and explicit makes the design legible.
package commitlog

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// Record is a single message in the log.
type Record struct {
	Offset uint64
	Value  []byte
}

// Log is a single-file append-only commit log with an in-memory index.
type Log struct {
	mu       sync.RWMutex
	f        *os.File
	index    []int64 // offset -> byte position in file
	nextOff  uint64
	writePos int64
}

// Open opens (or creates) a commit log at path and rebuilds its index by
// scanning existing records, so it recovers cleanly after a restart.
func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{f: f}
	if err := l.rebuildIndex(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return l, nil
}

// rebuildIndex walks the file to reconstruct the offset->position index.
func (l *Log) rebuildIndex() error {
	if _, err := l.f.Seek(0, 0); err != nil {
		return err
	}
	var pos int64
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(l.f, hdr); err != nil {
			break // EOF (or partial trailing write) — stop
		}
		n := binary.BigEndian.Uint64(hdr)
		l.index = append(l.index, pos)
		l.nextOff++
		skip := int64(8 + n)
		if _, err := l.f.Seek(skip-8, io.SeekCurrent); err != nil {
			break
		}
		pos += skip
	}
	l.writePos = pos
	if _, err := l.f.Seek(pos, 0); err != nil {
		return err
	}
	return nil
}

// Append writes value and returns its assigned offset. Records are framed as
// [8-byte big-endian length][value].
func (l *Log) Append(value []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	hdr := make([]byte, 8)
	binary.BigEndian.PutUint64(hdr, uint64(len(value)))
	if _, err := l.f.WriteAt(hdr, l.writePos); err != nil {
		return 0, err
	}
	if _, err := l.f.WriteAt(value, l.writePos+8); err != nil {
		return 0, err
	}

	off := l.nextOff
	l.index = append(l.index, l.writePos)
	l.writePos += int64(8 + len(value))
	l.nextOff++
	return off, nil
}

// Read returns the record at the given offset.
func (l *Log) Read(offset uint64) (Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if offset >= l.nextOff {
		return Record{}, fmt.Errorf("offset %d out of range (next=%d)", offset, l.nextOff)
	}
	pos := l.index[offset]
	hdr := make([]byte, 8)
	if _, err := l.f.ReadAt(hdr, pos); err != nil {
		return Record{}, err
	}
	n := binary.BigEndian.Uint64(hdr)
	buf := make([]byte, n)
	if _, err := l.f.ReadAt(buf, pos+8); err != nil {
		return Record{}, err
	}
	return Record{Offset: offset, Value: buf}, nil
}

// ReadFrom returns up to max records starting at offset (inclusive). Fewer are
// returned if the log ends first; this is how a consumer polls forward.
func (l *Log) ReadFrom(offset uint64, max int) ([]Record, error) {
	l.mu.RLock()
	end := l.nextOff
	l.mu.RUnlock()

	var out []Record
	for off := offset; off < end && len(out) < max; off++ {
		r, err := l.Read(off)
		if err != nil {
			return out, err
		}
		out = append(out, r)
	}
	return out, nil
}

// NextOffset returns the offset that will be assigned to the next append (i.e.
// the current length of the log).
func (l *Log) NextOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOff
}

// Close closes the underlying file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
