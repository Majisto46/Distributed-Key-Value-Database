package sstable

import (
	"bytes"
	"errors"
	"io"

	"github.com/distributed-kv/engine/pkg/storage/wal"
)

// Iterator sequentially traverses all records in an SSTable.
type Iterator struct {
	reader      *Reader
	blockIdx    int
	blockReader *bytes.Reader
	currentRec  *wal.Record
	isValid     bool
}

// NewIterator creates an iterator over the entire SSTable.
func (r *Reader) Iterator() (*Iterator, error) {
	it := &Iterator{
		reader:   r,
		blockIdx: 0,
	}
	it.loadBlock(0)
	it.Next()
	return it, nil
}

// loadBlock reads the block at idx into memory.
func (it *Iterator) loadBlock(idx int) bool {
	if idx >= len(it.reader.index) {
		it.blockReader = nil
		return false
	}

	entry := it.reader.index[idx]
	buf := make([]byte, entry.Length)
	if _, err := it.reader.file.ReadAt(buf, int64(entry.Offset)); err != nil {
		it.blockReader = nil
		return false
	}

	it.blockIdx = idx
	it.blockReader = bytes.NewReader(buf)
	return true
}

// Valid returns true if the iterator currently points to a record.
func (it *Iterator) Valid() bool {
	return it.isValid
}

// Next moves to the next record in the SSTable.
func (it *Iterator) Next() {
	if it.blockReader == nil {
		it.isValid = false
		it.currentRec = nil
		return
	}

	rec, err := wal.DecodeRecord(it.blockReader)
	if err == nil {
		it.currentRec = rec
		it.isValid = true
		return
	}

	if errors.Is(err, io.EOF) {
		// End of current block, move to next block
		nextBlock := it.blockIdx + 1
		if it.loadBlock(nextBlock) {
			it.Next()
			return
		}
	}

	// Reached end of all blocks
	it.isValid = false
	it.currentRec = nil
}

// Record returns the current record.
func (it *Iterator) Record() *wal.Record {
	return it.currentRec
}

// Seek positions the iterator at the first record with Key >= target.
func (it *Iterator) Seek(target []byte) {
	blockIdx := it.reader.searchBlock(target)
	if blockIdx < 0 {
		blockIdx = 0
	}

	if !it.loadBlock(blockIdx) {
		it.isValid = false
		return
	}

	for {
		rec, err := wal.DecodeRecord(it.blockReader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				blockIdx++
				if it.loadBlock(blockIdx) {
					continue
				}
			}
			it.isValid = false
			return
		}

		if bytes.Compare(rec.Key, target) >= 0 {
			it.currentRec = rec
			it.isValid = true
			return
		}
	}
}

