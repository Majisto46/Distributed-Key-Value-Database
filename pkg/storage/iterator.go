package storage

import (
	"bytes"
	"container/heap"

	"github.com/distributed-kv/engine/pkg/storage/memtable"
	"github.com/distributed-kv/engine/pkg/storage/sstable"
	"github.com/distributed-kv/engine/pkg/storage/wal"
)

// InternalIterator abstracts a single source of ordered records (MemTable or SSTable).
type InternalIterator interface {
	Valid() bool
	Next()
	Seek(target []byte)
	SeekToFirst()
	Record() *wal.Record
}

// memTableIterAdapter adapts a memtable.Iterator to InternalIterator.
type memTableIterAdapter struct {
	it *memtable.Iterator
}

func (a *memTableIterAdapter) Valid() bool { return a.it.Valid() }
func (a *memTableIterAdapter) Next()       { a.it.Next() }
func (a *memTableIterAdapter) Seek(target []byte) {
	a.it.Seek(target)
}
func (a *memTableIterAdapter) SeekToFirst() {
	a.it.SeekToFirst()
}
func (a *memTableIterAdapter) Record() *wal.Record {
	e := a.it.Entry()
	if e == nil {
		return nil
	}
	return &wal.Record{
		Timestamp: e.Timestamp,
		Type:      e.Type,
		Key:       e.Key,
		Value:     e.Value,
	}
}

// sstableIterAdapter adapts a sstable.Iterator to InternalIterator.
type sstableIterAdapter struct {
	it *sstable.Iterator
}

func (a *sstableIterAdapter) Valid() bool               { return a.it.Valid() }
func (a *sstableIterAdapter) Next()                     { a.it.Next() }
func (a *sstableIterAdapter) Seek(target []byte)        { a.it.Seek(target) }
func (a *sstableIterAdapter) SeekToFirst()              { a.it.Seek(nil) }
func (a *sstableIterAdapter) Record() *wal.Record       { return a.it.Record() }

type mergeItem struct {
	rec     *wal.Record
	iterIdx int
	layer   int // lower number = newer layer (0: activeMem, 1: immMem, 2+: sstables)
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].rec.Key, h[j].rec.Key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].layer < h[j].layer
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(mergeItem))
}
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// EngineIterator merges and orders records across active MemTable, immutable MemTable,
// and all SSTables in real-time.
type EngineIterator struct {
	iters      []InternalIterator
	layers     []int
	h          mergeHeap
	currentRec *wal.Record
	isValid    bool
}

// NewIterator creates a unified merging iterator across the storage engine.
func (e *Engine) NewIterator() (*EngineIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.isClosed {
		return nil, ErrEngineClosed
	}

	var iters []InternalIterator
	var layers []int

	// Layer 0: Active MemTable
	if e.activeMem != nil {
		iters = append(iters, &memTableIterAdapter{it: e.activeMem.Iterator()})
		layers = append(layers, 0)
	}

	// Layer 1: Immutable MemTable
	if e.immMem != nil {
		iters = append(iters, &memTableIterAdapter{it: e.immMem.Iterator()})
		layers = append(layers, 1)
	}

	// Layers 2+: SSTables (index 0 is newest SSTable)
	for i, sst := range e.sstables {
		it, err := sst.Iterator()
		if err != nil {
			return nil, err
		}
		iters = append(iters, &sstableIterAdapter{it: it})
		layers = append(layers, 2+i)
	}

	mergeIt := &EngineIterator{
		iters:  iters,
		layers: layers,
	}

	return mergeIt, nil
}

// Seek positions the iterator at the first record with Key >= target.
func (it *EngineIterator) Seek(target []byte) {
	it.h = mergeHeap{}
	heap.Init(&it.h)

	for i, iter := range it.iters {
		iter.Seek(target)
		if iter.Valid() {
			heap.Push(&it.h, mergeItem{
				rec:     iter.Record(),
				iterIdx: i,
				layer:   it.layers[i],
			})
		}
	}

	it.advanceToNextUnique()
}

// Valid returns true if the iterator currently points to a valid record.
func (it *EngineIterator) Valid() bool {
	return it.isValid
}

// Record returns the record at the current position.
func (it *EngineIterator) Record() *wal.Record {
	return it.currentRec
}

// Next advances the iterator to the next distinct key.
func (it *EngineIterator) Next() {
	if !it.isValid {
		return
	}
	it.advanceToNextUnique()
}

// advanceToNextUnique pops the min key from the heap and discards older shadow versions of the same key.
func (it *EngineIterator) advanceToNextUnique() {
	if it.h.Len() == 0 {
		it.isValid = false
		it.currentRec = nil
		return
	}

	top := heap.Pop(&it.h).(mergeItem)
	chosenRec := top.rec

	// Advance the iterator that provided this record
	sourceIter := it.iters[top.iterIdx]
	sourceIter.Next()
	if sourceIter.Valid() {
		heap.Push(&it.h, mergeItem{
			rec:     sourceIter.Record(),
			iterIdx: top.iterIdx,
			layer:   top.layer,
		})
	}

	// If there are duplicate older versions of the same key in the heap, advance them and discard
	for it.h.Len() > 0 && bytes.Equal(it.h[0].rec.Key, chosenRec.Key) {
		dup := heap.Pop(&it.h).(mergeItem)
		dupIter := it.iters[dup.iterIdx]
		dupIter.Next()
		if dupIter.Valid() {
			heap.Push(&it.h, mergeItem{
				rec:     dupIter.Record(),
				iterIdx: dup.iterIdx,
				layer:   dup.layer,
			})
		}
	}

	it.currentRec = chosenRec
	it.isValid = true
}

