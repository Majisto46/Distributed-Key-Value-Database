package cdc

import (
	"testing"
	"time"
)

func TestCDC_PublishAndSubscribe(t *testing.T) {
	b := NewBroadcaster(100)

	sub := b.Subscribe(0)
	defer sub.Close()

	if b.SubscriberCount() != 1 {
		t.Fatalf("expected 1 subscriber, got %d", b.SubscriberCount())
	}

	// Publish 3 events
	b.Publish(EventPut, []byte("user:1"), []byte(`{"name":"Alice"}`))
	b.Publish(EventPut, []byte("user:2"), []byte(`{"name":"Bob"}`))
	b.Publish(EventDelete, []byte("user:1"), nil)

	var received []Event
	for i := 0; i < 3; i++ {
		select {
		case e := <-sub.Events():
			received = append(received, e)
		case <-time.After(1 * time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}

	if len(received) != 3 {
		t.Fatalf("expected 3 events, got %d", len(received))
	}
	if received[0].Type != EventPut || string(received[0].Key) != "user:1" {
		t.Errorf("unexpected event 0: %v", received[0])
	}
	if received[2].Type != EventDelete || string(received[2].Key) != "user:1" {
		t.Errorf("unexpected event 2: %v", received[2])
	}
}

func TestCDC_HistoricalReplay(t *testing.T) {
	b := NewBroadcaster(100)

	// Publish before subscriber joins
	e1 := b.Publish(EventPut, []byte("k1"), []byte("v1"))
	e2 := b.Publish(EventPut, []byte("k2"), []byte("v2"))
	_ = b.Publish(EventPut, []byte("k3"), []byte("v3"))

	// Subscriber joins starting from e2.Sequence
	sub := b.Subscribe(e2.Sequence)
	defer sub.Close()

	// Should immediately receive e2 and e3 from history replay
	select {
	case replayed := <-sub.Events():
		if replayed.Sequence != e2.Sequence || string(replayed.Key) != "k2" {
			t.Fatalf("expected replayed event 2, got %v", replayed)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for replayed event 2")
	}

	select {
	case replayed := <-sub.Events():
		if string(replayed.Key) != "k3" {
			t.Fatalf("expected replayed event 3, got %v", replayed)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for replayed event 3")
	}

	_ = e1
}

