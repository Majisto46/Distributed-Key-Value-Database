package cdc

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// EventType categorizes change events published to the CDC stream.
type EventType byte

const (
	EventPut    EventType = 1
	EventDelete EventType = 2
)

func (e EventType) String() string {
	switch e {
	case EventPut:
		return "PUT"
	case EventDelete:
		return "DELETE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", e)
	}
}

// Event represents an individual mutation emitted by the database state machine.
type Event struct {
	Sequence  uint64    `json:"sequence"`
	Timestamp int64     `json:"timestamp"`
	Type      EventType `json:"type"`
	Key       []byte    `json:"key"`
	Value     []byte    `json:"value,omitempty"`
}

// Subscription delivers a continuous stream of events to an active consumer.
type Subscription struct {
	ch       chan Event
	cancelCh chan struct{}
	b        *Broadcaster
	subID    uint64
	closed   bool
	mu       sync.Mutex
}

// Events returns the receive-only channel yielding CDC events.
func (s *Subscription) Events() <-chan Event {
	return s.ch
}

// Close unregisters the subscription and closes delivery channels.
func (s *Subscription) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true
	close(s.cancelCh)
	s.b.removeSubscriber(s.subID)
}

// Broadcaster manages event publishing and fan-out distribution to subscribers.
type Broadcaster struct {
	mu           sync.RWMutex
	subscribers  map[uint64]*Subscription
	nextSubID    uint64
	seqCounter   uint64
	history      []Event // Circular buffer for event replay
	historyLimit int
}

// NewBroadcaster initializes a CDC Broadcaster with a buffer history limit.
func NewBroadcaster(historyLimit int) *Broadcaster {
	if historyLimit <= 0 {
		historyLimit = 10000
	}
	return &Broadcaster{
		subscribers:  make(map[uint64]*Subscription),
		historyLimit: historyLimit,
	}
}

// Publish broadcasts a new mutation event to all active subscribers.
func (b *Broadcaster) Publish(eventType EventType, key, value []byte) Event {
	seq := atomic.AddUint64(&b.seqCounter, 1)

	kCopy := make([]byte, len(key))
	copy(kCopy, key)

	var vCopy []byte
	if value != nil {
		vCopy = make([]byte, len(value))
		copy(vCopy, value)
	}

	event := Event{
		Sequence:  seq,
		Timestamp: time.Now().UnixNano(),
		Type:      eventType,
		Key:       kCopy,
		Value:     vCopy,
	}

	b.mu.Lock()
	// Record in history buffer
	b.history = append(b.history, event)
	if len(b.history) > b.historyLimit {
		b.history = b.history[len(b.history)-b.historyLimit:]
	}

	// Fan out to active subscribers
	for _, sub := range b.subscribers {
		select {
		case sub.ch <- event:
		default:
			// Non-blocking drop/skip for stalled subscribers to prevent write blocking
		}
	}
	b.mu.Unlock()

	return event
}

// Subscribe attaches a consumer starting from fromSeq.
// If fromSeq is 0, only future events are delivered.
// If fromSeq > 0, historical events with Sequence >= fromSeq are replayed first.
func (b *Broadcaster) Subscribe(fromSeq uint64) *Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextSubID++
	subID := b.nextSubID

	sub := &Subscription{
		ch:       make(chan Event, 256),
		cancelCh: make(chan struct{}),
		b:        b,
		subID:    subID,
	}

	// Replay history if requested
	if fromSeq > 0 {
		for _, e := range b.history {
			if e.Sequence >= fromSeq {
				select {
				case sub.ch <- e:
				default:
				}
			}
		}
	}

	b.subscribers[subID] = sub
	return sub
}

func (b *Broadcaster) removeSubscriber(subID uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subscribers, subID)
}

// SubscriberCount returns active subscriptions count.
func (b *Broadcaster) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers)
}

