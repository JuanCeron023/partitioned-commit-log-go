package broker

import (
	"fmt"
	"testing"
)

func newTestBroker(t *testing.T) *Broker {
	t.Helper()
	b := New(t.TempDir())
	t.Cleanup(b.Close)
	return b
}

func TestProduceConsumeOrdering(t *testing.T) {
	b := newTestBroker(t)
	if err := b.CreateTopic("orders", 1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, _, err := b.Produce("orders", "", []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	msgs, err := b.Consume("g1", "orders", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(msgs))
	}
	for i, m := range msgs {
		if string(m.Value) != fmt.Sprintf("msg-%d", i) {
			t.Fatalf("out of order at %d: %s", i, m.Value)
		}
	}
}

func TestKeyRoutingIsStable(t *testing.T) {
	b := newTestBroker(t)
	_ = b.CreateTopic("events", 4)
	p1, _, _ := b.Produce("events", "user-42", []byte("a"))
	p2, _, _ := b.Produce("events", "user-42", []byte("b"))
	if p1 != p2 {
		t.Fatalf("same key routed to different partitions: %d vs %d", p1, p2)
	}
}

func TestConsumerGroupOffsets(t *testing.T) {
	b := newTestBroker(t)
	_ = b.CreateTopic("t", 1)
	for i := 0; i < 3; i++ {
		b.Produce("t", "", []byte(fmt.Sprintf("%d", i)))
	}

	msgs, _ := b.Consume("g", "t", 0, 10)
	if len(msgs) != 3 {
		t.Fatalf("expected 3, got %d", len(msgs))
	}
	// Commit the first message only.
	b.Commit("g", "t", 0, msgs[0].Offset)

	// Next consume should skip the committed message.
	again, _ := b.Consume("g", "t", 0, 10)
	if len(again) != 2 || string(again[0].Value) != "1" {
		t.Fatalf("expected to resume at offset 1, got %d msgs starting %q", len(again), again[0].Value)
	}

	// A different group is independent and sees everything.
	other, _ := b.Consume("other", "t", 0, 10)
	if len(other) != 3 {
		t.Fatalf("independent group should see all 3, got %d", len(other))
	}
}
