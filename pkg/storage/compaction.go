package storage

import (
	"bytes"
	"container/heap"
	"fmt"

	"github.com/distributed-kv/engine/pkg/storage/sstable"
	"github.com/distributed-kv/engine/pkg/storage/wal"
)

// iteratorItem is a wrapper for a min-heap entry during multi-way merge.
type iteratorItem struct {
	rec     *wal.Record
	iterIdx int
	sstIdx  int // lower index = newer SSTable
}

// iterHeap implements a priority queue of iterator items sorted by (Key ASC, sstIdx ASC).
// Because lower sstIdx corresponds to newer SSTables, the newest record for any key appears first!
type iterHeap []iteratorItem

func (h iterHeap) Len() int { return len(h) }
func (h iterHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].rec.Key, h[j].rec.Key)
	if cmp != 0 {
		return cmp < 0 // ascending by key
	}
	// Same key: pick the one from the newer SSTable (lower sstIdx)
	return h[i].sstIdx < h[j].sstIdx
}
func (h iterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *iterHeap) Push(x interface{}) {
	*h = append(*h, x.(iteratorItem))
}
func (h *iterHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// CompactSSTables performs a multi-way merge sort across input SSTables,
// discarding obsolete duplicate versions and purged tombstones.
// It writes the compacted output to outPath and returns the count of preserved records.
func CompactSSTables(tables []*sstable.Reader, outPath string) (int64, error) {
	if len(tables) == 0 {
		return 0, nil
	}

	// Initialize iterators for each SSTable
	var iters []*sstable.Iterator
	for _, table := range tables {
		it, err := table.Iterator()
		if err != nil {
			return 0, fmt.Errorf("failed to create iterator for %s: %w", table.Path(), err)
		}
		iters = append(iters, it)
	}

	// Priority queue for K-way merge
	h := &iterHeap{}
	heap.Init(h)

	for i, it := range iters {
		if it.Valid() {
			heap.Push(h, iteratorItem{
				rec:     it.Record(),
				iterIdx: i,
				sstIdx:  i, // 0 is newest
			})
		}
	}

	opts := sstable.DefaultWriterOptions()
	opts.ExpectedKeys = len(tables) * 1000
	writer, err := sstable.NewWriter(outPath, opts)
	if err != nil {
		return 0, fmt.Errorf("failed to create compaction writer: %w", err)
	}

	var lastKey []byte
	var compactedCount int64

	for h.Len() > 0 {
		top := heap.Pop(h).(iteratorItem)
		rec := top.rec

		// Advance the iterator that produced this record
		it := iters[top.iterIdx]
		it.Next()
		if it.Valid() {
			heap.Push(h, iteratorItem{
				rec:     it.Record(),
				iterIdx: top.iterIdx,
				sstIdx:  top.sstIdx,
			})
		}

		// If this key is identical to the last emitted key, it's an older obsolete version!
		if lastKey != nil && bytes.Equal(rec.Key, lastKey) {
			continue // Drop older version
		}

		lastKey = make([]byte, len(rec.Key))
		copy(lastKey, rec.Key)

		// If the newest version is a tombstone (RecordTypeDelete), purge it entirely!
		if rec.Type == wal.RecordTypeDelete {
			continue // Tombstone successfully eliminated dead data
		}

		if err := writer.Append(rec); err != nil {
			writer.Close()
			return 0, fmt.Errorf("failed to append compacted record: %w", err)
		}
		compactedCount++
	}

	if err := writer.Close(); err != nil {
		return 0, fmt.Errorf("failed to finalize compacted sstable: %w", err)
	}

	return compactedCount, nil
}
