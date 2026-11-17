package memtable

import (
	"sync/atomic"

	"github.com/distributed-kv/engine/pkg/storage/wal"
)

// MemTable represents an active in-memory buffer storing recent mutations.
type MemTable struct {
	sl        *SkipList
	sizeBytes int64
	walSeq    uint64
}

// New creates a new MemTable associated with an active WAL sequence number.
func New(walSeq uint64) *MemTable {
	return &MemTable{
		sl:     NewSkipList(),
		walSeq: walSeq,
	}
}

// Put inserts or updates a key in the MemTable.
func (m *MemTable) Put(key, value []byte, timestamp int64) {
	delta := m.sl.Put(key, value, timestamp, wal.RecordTypePut)
	atomic.AddInt64(&m.sizeBytes, delta)
}

// Delete writes a tombstone for the key in the MemTable.
func (m *MemTable) Delete(key []byte, timestamp int64) {
	delta := m.sl.Put(key, nil, timestamp, wal.RecordTypeDelete)
	atomic.AddInt64(&m.sizeBytes, delta)
}

// Get queries the MemTable for a key. Returns the Entry and true if found, false otherwise.
func (m *MemTable) Get(key []byte) (*Entry, bool) {
	return m.sl.Get(key)
}

// Iterator returns an ordered iterator over the MemTable contents.
func (m *MemTable) Iterator() *Iterator {
	return m.sl.Iterator()
}

// Size returns the approximate memory footprint of the MemTable in bytes.
func (m *MemTable) Size() int64 {
	return atomic.LoadInt64(&m.sizeBytes)
}

// Count returns the number of unique keys in the MemTable.
func (m *MemTable) Count() int64 {
	return m.sl.Count()
}

// WALSequence returns the WAL segment sequence associated with this MemTable.
func (m *MemTable) WALSequence() uint64 {
	return m.walSeq
}

