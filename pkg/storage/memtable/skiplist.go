package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"github.com/distributed-kv/engine/pkg/storage/wal"
)

const (
	defaultMaxLevel    = 16
	defaultProbability = 0.5
)

// Node represents an element in the multi-level SkipList.
type Node struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Type      wal.RecordType
	forward   []*Node
}

// Entry is the user-facing view of a retrieved key-value record.
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Type      wal.RecordType
}

// SkipList is a probabilistic balanced data structure providing O(log N)
// search, insertion, and ordered sequential iteration.
type SkipList struct {
	mu          sync.RWMutex
	head        *Node
	maxLevel    int
	level       int
	p           float64
	count       int64
	sizeBytes   int64
	random      *rand.Rand
	randomMu    sync.Mutex
}

// NewSkipList initializes a new SkipList with default parameters.
func NewSkipList() *SkipList {
	return NewSkipListWithParams(defaultMaxLevel, defaultProbability)
}

// NewSkipListWithParams initializes a SkipList with custom max level and coin-flip probability.
func NewSkipListWithParams(maxLevel int, p float64) *SkipList {
	head := &Node{
		forward: make([]*Node, maxLevel),
	}
	src := rand.NewSource(time.Now().UnixNano())
	return &SkipList{
		head:     head,
		maxLevel: maxLevel,
		level:    1,
		p:        p,
		random:   rand.New(src),
	}
}

// randomLevel generates a random height for a new node using geometric distribution.
func (s *SkipList) randomLevel() int {
	s.randomMu.Lock()
	defer s.randomMu.Unlock()

	lvl := 1
	for lvl < s.maxLevel && s.random.Float64() < s.p {
		lvl++
	}
	return lvl
}

// Put inserts or updates a key with its value, timestamp, and mutation type.
// Returns the delta in memory bytes.
func (s *SkipList) Put(key, value []byte, timestamp int64, recType wal.RecordType) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	update := make([]*Node, s.maxLevel)
	curr := s.head

	// Traverse from top level down to level 0
	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && bytes.Compare(curr.forward[i].Key, key) < 0 {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	// At level 0, check if the exact key already exists
	target := curr.forward[0]
	if target != nil && bytes.Equal(target.Key, key) {
		// Key already exists — update value and metadata in-place
		oldSize := int64(len(target.Value))
		newSize := int64(len(value))
		delta := newSize - oldSize

		target.Value = make([]byte, len(value))
		copy(target.Value, value)
		target.Timestamp = timestamp
		target.Type = recType
		s.sizeBytes += delta
		return delta
	}

	// Key does not exist — generate random level for the new node
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	var valCopy []byte
	if value != nil {
		valCopy = make([]byte, len(value))
		copy(valCopy, value)
	}

	newNode := &Node{
		Key:       keyCopy,
		Value:     valCopy,
		Timestamp: timestamp,
		Type:      recType,
		forward:   make([]*Node, lvl),
	}

	// Splice pointers across all assigned levels
	for i := 0; i < lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	s.count++
	// Approximate memory: key bytes + value bytes + node struct & pointers overhead
	addedBytes := int64(len(key) + len(value) + 32 + (lvl * 8))
	s.sizeBytes += addedBytes
	return addedBytes
}

// Get finds a key in the SkipList. Returns Entry and true if found, false otherwise.
func (s *SkipList) Get(key []byte) (*Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	curr := s.head
	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && bytes.Compare(curr.forward[i].Key, key) < 0 {
			curr = curr.forward[i]
		}
	}

	target := curr.forward[0]
	if target != nil && bytes.Equal(target.Key, key) {
		return &Entry{
			Key:       target.Key,
			Value:     target.Value,
			Timestamp: target.Timestamp,
			Type:      target.Type,
		}, true
	}

	return nil, false
}

// Size returns the approximate memory size in bytes stored in the SkipList.
func (s *SkipList) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sizeBytes
}

// Count returns the number of distinct keys stored.
func (s *SkipList) Count() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

// Iterator returns an ordered iterator over all entries in the SkipList.
func (s *SkipList) Iterator() *Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Snapshot nodes at level 0 for consistent iteration
	var nodes []*Node
	curr := s.head.forward[0]
	for curr != nil {
		nodes = append(nodes, curr)
		curr = curr.forward[0]
	}

	return &Iterator{
		nodes: nodes,
		idx:   -1,
	}
}

// Iterator enables sequential ordered traversal of keys.
type Iterator struct {
	nodes []*Node
	idx   int
}

// Valid returns true if the iterator currently points to a valid entry.
func (it *Iterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.nodes)
}

// Next advances the iterator to the next entry.
func (it *Iterator) Next() {
	if it.idx < len(it.nodes) {
		it.idx++
	}
}

// Seek positions the iterator at the first key >= target.
func (it *Iterator) Seek(target []byte) {
	low := 0
	high := len(it.nodes) - 1
	result := len(it.nodes)

	for low <= high {
		mid := low + (high-low)/2
		if bytes.Compare(it.nodes[mid].Key, target) >= 0 {
			result = mid
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	it.idx = result
}

// SeekToFirst rewinds the iterator to the beginning.
func (it *Iterator) SeekToFirst() {
	it.idx = 0
}

// Entry returns the entry at the current iterator position.
func (it *Iterator) Entry() *Entry {
	if !it.Valid() {
		return nil
	}
	n := it.nodes[it.idx]
	return &Entry{
		Key:       n.Key,
		Value:     n.Value,
		Timestamp: n.Timestamp,
		Type:      n.Type,
	}
}

