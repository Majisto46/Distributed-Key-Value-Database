package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/distributed-kv/engine/pkg/storage/bloom"
	"github.com/distributed-kv/engine/pkg/storage/wal"
)

// Reader provides random and sequential read access to an on-disk SSTable.
type Reader struct {
	file    *os.File
	path    string
	footer  *Footer
	index   []IndexEntry
	filter  *bloom.Filter
	minKey  []byte
	maxKey  []byte
}

// OpenReader opens an existing SSTable, parses its footer, and caches the sparse index & Bloom filter.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sstable file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if fi.Size() < FooterSize {
		f.Close()
		return nil, ErrInvalidSSTable
	}

	// 1. Read Footer from the last 40 bytes
	footerBuf := make([]byte, FooterSize)
	if _, err := f.ReadAt(footerBuf, fi.Size()-FooterSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	footer, err := DecodeFooter(footerBuf)
	if err != nil {
		f.Close()
		return nil, err
	}

	// 2. Read and parse Sparse Index
	indexBuf := make([]byte, footer.IndexLength)
	if _, err := f.ReadAt(indexBuf, int64(footer.IndexOffset)); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to read index block: %w", err)
	}

	index, err := DecodeIndex(indexBuf)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to decode index: %w", err)
	}

	// 3. Read and parse Bloom Filter
	bloomBuf := make([]byte, footer.BloomLength)
	if _, err := f.ReadAt(bloomBuf, int64(footer.BloomOffset)); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to read bloom filter: %w", err)
	}

	filter, err := bloom.DecodeFilter(bloomBuf)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to decode bloom filter: %w", err)
	}

	var minK, maxK []byte
	if len(index) > 0 {
		minK = index[0].FirstKey
	}

	return &Reader{
		file:   f,
		path:   path,
		footer: footer,
		index:  index,
		filter: filter,
		minKey: minK,
		maxKey: maxK,
	}, nil
}

// Get queries for key in this SSTable.
// Returns wal.Record if found, or ErrKeyNotFound if absent.
func (r *Reader) Get(key []byte) (*wal.Record, error) {
	// 1. Fast-path: Bloom filter check in RAM (Zero disk I/O)
	if !r.filter.MayContain(key) {
		return nil, ErrKeyNotFound
	}

	// 2. Binary search Sparse Index to find candidate block
	blockIdx := r.searchBlock(key)
	if blockIdx < 0 {
		return nil, ErrKeyNotFound
	}

	// 3. Read candidate block from disk (Single disk seek & read)
	targetBlock := r.index[blockIdx]
	blockData := make([]byte, targetBlock.Length)
	if _, err := r.file.ReadAt(blockData, int64(targetBlock.Offset)); err != nil {
		return nil, fmt.Errorf("failed to read block %d: %w", blockIdx, err)
	}

	// 4. Scan records within block
	reader := bytes.NewReader(blockData)
	for {
		rec, err := wal.DecodeRecord(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		cmp := bytes.Compare(rec.Key, key)
		if cmp == 0 {
			return rec, nil
		}
		if cmp > 0 {
			// Records in block are sorted; if we passed key, it doesn't exist
			break
		}
	}

	return nil, ErrKeyNotFound
}

// searchBlock finds the index of the block that could contain key.
// Returns the highest index where index[i].FirstKey <= key.
func (r *Reader) searchBlock(key []byte) int {
	low := 0
	high := len(r.index) - 1
	result := -1

	for low <= high {
		mid := low + (high-low)/2
		if bytes.Compare(r.index[mid].FirstKey, key) <= 0 {
			result = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return result
}

// Close releases the file handle.
func (r *Reader) Close() error {
	return r.file.Close()
}

// Path returns the filesystem path of the SSTable.
func (r *Reader) Path() string {
	return r.path
}

// IndexEntries returns the cached sparse index entries.
func (r *Reader) IndexEntries() []IndexEntry {
	return r.index
}

