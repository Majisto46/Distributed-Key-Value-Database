package sstable

import (
	"bytes"
	"fmt"
	"os"

	"github.com/distributed-kv/engine/pkg/storage/bloom"
	"github.com/distributed-kv/engine/pkg/storage/wal"
)

// WriterOptions configures SSTable creation.
type WriterOptions struct {
	BlockSize         int     // Target size in bytes for each data block (default 4KB)
	ExpectedKeys      int     // Hint for Bloom filter allocation
	FalsePositiveRate float64 // Bloom filter error rate (e.g. 0.01)
}

// DefaultWriterOptions provides production defaults.
func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		BlockSize:         4 * 1024, // 4KB blocks
		ExpectedKeys:      1000,
		FalsePositiveRate: 0.01,
	}
}

// Writer constructs an SSTable file on disk.
type Writer struct {
	opts         WriterOptions
	file         *os.File
	filter       *bloom.Filter
	index        []IndexEntry
	blockBuf     bytes.Buffer
	blockFirstKey []byte
	currentOffset uint64
	recordCount   int64
	minKey        []byte
	maxKey        []byte
}

// NewWriter initializes an SSTable writer to create a new .sst file.
func NewWriter(path string, opts WriterOptions) (*Writer, error) {
	if opts.BlockSize <= 0 {
		opts.BlockSize = 4 * 1024
	}
	if opts.ExpectedKeys <= 0 {
		opts.ExpectedKeys = 1000
	}
	if opts.FalsePositiveRate <= 0 {
		opts.FalsePositiveRate = 0.01
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create sstable file: %w", err)
	}

	return &Writer{
		opts:   opts,
		file:   f,
		filter: bloom.NewFilter(opts.ExpectedKeys, opts.FalsePositiveRate),
	}, nil
}

// Append writes a sorted key-value record to the SSTable.
// Invariant: Keys MUST be added in strictly non-decreasing sorted order.
func (w *Writer) Append(rec *wal.Record) error {
	// Track min and max keys for file-level bounds
	if w.minKey == nil {
		w.minKey = make([]byte, len(rec.Key))
		copy(w.minKey, rec.Key)
	}
	w.maxKey = make([]byte, len(rec.Key))
	copy(w.maxKey, rec.Key)

	// Add key to Bloom filter
	w.filter.Add(rec.Key)

	if len(w.blockFirstKey) == 0 {
		w.blockFirstKey = make([]byte, len(rec.Key))
		copy(w.blockFirstKey, rec.Key)
	}

	encoded := rec.Encode()
	w.blockBuf.Write(encoded)
	w.recordCount++

	// Flush block if threshold exceeded
	if w.blockBuf.Len() >= w.opts.BlockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	return nil
}

// flushBlock writes the current buffered block to disk and records an index entry.
func (w *Writer) flushBlock() error {
	if w.blockBuf.Len() == 0 {
		return nil
	}

	blockBytes := w.blockBuf.Bytes()
	blockLen := uint64(len(blockBytes))

	if _, err := w.file.Write(blockBytes); err != nil {
		return fmt.Errorf("failed to write data block: %w", err)
	}

	w.index = append(w.index, IndexEntry{
		FirstKey: w.blockFirstKey,
		Offset:   w.currentOffset,
		Length:   blockLen,
	})

	w.currentOffset += blockLen
	w.blockBuf.Reset()
	w.blockFirstKey = nil
	return nil
}

// Close finalizes all data blocks, writes the sparse index, bloom filter, and footer.
func (w *Writer) Close() error {
	// Flush any remaining buffered data
	if err := w.flushBlock(); err != nil {
		w.file.Close()
		return err
	}

	// 1. Write Sparse Index
	indexBytes := EncodeIndex(w.index)
	indexOffset := w.currentOffset
	indexLen := uint64(len(indexBytes))

	if _, err := w.file.Write(indexBytes); err != nil {
		w.file.Close()
		return fmt.Errorf("failed to write index: %w", err)
	}
	w.currentOffset += indexLen

	// 2. Write Bloom Filter
	bloomBytes := w.filter.Encode()
	bloomOffset := w.currentOffset
	bloomLen := uint64(len(bloomBytes))

	if _, err := w.file.Write(bloomBytes); err != nil {
		w.file.Close()
		return fmt.Errorf("failed to write bloom filter: %w", err)
	}
	w.currentOffset += bloomLen

	// 3. Write Footer
	footer := &Footer{
		IndexOffset: indexOffset,
		IndexLength: indexLen,
		BloomOffset: bloomOffset,
		BloomLength: bloomLen,
		Magic:       MagicNumber,
	}

	footerBytes := footer.Encode()
	if _, err := w.file.Write(footerBytes); err != nil {
		w.file.Close()
		return fmt.Errorf("failed to write footer: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		w.file.Close()
		return err
	}

	return w.file.Close()
}

