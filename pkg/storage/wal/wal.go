package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SyncPolicy defines when data is flushed to persistent disk storage.
type SyncPolicy int

const (
	// SyncAlways calls fsync after every individual write (maximum durability, lowest throughput).
	SyncAlways SyncPolicy = iota
	// SyncBatch calls fsync after buffer size or interval threshold.
	SyncBatch
	// SyncNone relies on OS page cache writeback (maximum throughput, risk of losing last buffered writes on OS crash).
	SyncNone
)

// Options holds configuration for the WAL.
type Options struct {
	Dir         string     // Directory where WAL segment files are stored
	MaxFileSize int64      // Maximum size in bytes before rotating to a new segment
	SyncPolicy  SyncPolicy // Flushing strategy
}

// DefaultOptions returns standard configuration for WAL.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:         dir,
		MaxFileSize: 16 * 1024 * 1024, // 16MB per segment
		SyncPolicy:  SyncAlways,
	}
}

// WAL manages sequential append-only log segments.
type WAL struct {
	mu          sync.Mutex
	opts        Options
	activeFile  *os.File
	activeSeq   uint64
	activeSize  int64
	writer      *bufio.Writer
	isClosed    bool
}

// Open creates or resumes a WAL in the specified directory.
func Open(opts Options) (*WAL, error) {
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal dir: %w", err)
	}

	w := &WAL{
		opts: opts,
	}

	// Discover existing segments
	segments, err := listSegments(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("failed to list wal segments: %w", err)
	}

	var activeSeq uint64 = 1
	if len(segments) > 0 {
		activeSeq = segments[len(segments)-1]
	}

	segmentPath := segmentFileName(opts.Dir, activeSeq)
	file, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open active wal segment: %w", err)
	}

	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat wal segment: %w", err)
	}

	w.activeFile = file
	w.activeSeq = activeSeq
	w.activeSize = fi.Size()
	w.writer = bufio.NewWriterSize(file, 64*1024)

	// If the existing file already exceeded max size, rotate immediately
	if w.activeSize >= opts.MaxFileSize {
		if err := w.rotate(); err != nil {
			w.Close()
			return nil, fmt.Errorf("failed initial wal rotation: %w", err)
		}
	}

	return w, nil
}

// Write appends a single record to the active WAL segment.
func (w *WAL) Write(recType RecordType, key, val []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.isClosed {
		return fmt.Errorf("wal is closed")
	}

	record := &Record{
		Timestamp: time.Now().UnixNano(),
		Type:      recType,
		Key:       key,
		Value:     val,
	}

	data := record.Encode()
	recordLen := int64(len(data))

	// Rotate segment if write would exceed MaxFileSize
	if w.activeSize+recordLen > w.opts.MaxFileSize && w.activeSize > 0 {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("wal rotation failed: %w", err)
		}
	}

	if _, err := w.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write to buffer: %w", err)
	}

	w.activeSize += recordLen

	if w.opts.SyncPolicy == SyncAlways {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("buffer flush failed: %w", err)
		}
		if err := w.activeFile.Sync(); err != nil {
			return fmt.Errorf("file fsync failed: %w", err)
		}
	}

	return nil
}

// Sync forces all unwritten in-memory buffers to be synced to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.isClosed {
		return nil
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.activeFile.Sync()
}

// rotate closes the current active segment and opens a new incremented segment.
// Must be called while holding w.mu.
func (w *WAL) rotate() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.activeFile.Sync(); err != nil {
		return err
	}
	if err := w.activeFile.Close(); err != nil {
		return err
	}

	w.activeSeq++
	newPath := segmentFileName(w.opts.Dir, w.activeSeq)
	file, err := os.OpenFile(newPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	w.activeFile = file
	w.activeSize = 0
	w.writer.Reset(file)
	return nil
}

// Close flushes and closes the active WAL segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.isClosed {
		return nil
	}
	w.isClosed = true

	var flushErr, syncErr, closeErr error
	if w.writer != nil {
		flushErr = w.writer.Flush()
	}
	if w.activeFile != nil {
		syncErr = w.activeFile.Sync()
		closeErr = w.activeFile.Close()
	}

	if flushErr != nil {
		return flushErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// PurgeOldSegments deletes segment files strictly older than upToSeq.
// This is called after an SSTable flush has successfully persisted data to disk.
func (w *WAL) PurgeOldSegments(upToSeq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments, err := listSegments(w.opts.Dir)
	if err != nil {
		return err
	}

	for _, seq := range segments {
		if seq < upToSeq && seq < w.activeSeq {
			path := segmentFileName(w.opts.Dir, seq)
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// ActiveSequence returns the sequence number of the current segment.
func (w *WAL) ActiveSequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.activeSeq
}

// Recover reads all WAL segments in sequence and applies valid records via a callback handler.
// If a torn write is encountered at EOF (e.g. from an unclean shutdown), it recovers all valid
// records up to that point and reports the torn write cleanly.
func Recover(dir string, applyFunc func(rec *Record) error) error {
	segments, err := listSegments(dir)
	if err != nil {
		return fmt.Errorf("failed to list segments during recovery: %w", err)
	}

	for _, seq := range segments {
		path := segmentFileName(dir, seq)
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open segment %s: %w", path, err)
		}

		reader := bufio.NewReaderSize(file, 64*1024)
		for {
			rec, err := DecodeRecord(reader)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break // Normal EOF
				}
				if errors.Is(err, ErrTornWrite) {
					// Stop at the torn write — everything up to here was verified by CRC32
					break
				}
				file.Close()
				return fmt.Errorf("corrupt record in segment %s: %w", path, err)
			}

			if err := applyFunc(rec); err != nil {
				file.Close()
				return fmt.Errorf("apply callback failed on record: %w", err)
			}
		}
		file.Close()
	}

	return nil
}

func segmentFileName(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%08d.wal", seq))
}

func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var seqs []uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".wal")
		seq, err := strconv.ParseUint(base, 10, 64)
		if err == nil {
			seqs = append(seqs, seq)
		}
	}

	sort.Slice(seqs, func(i, j int) bool {
		return seqs[i] < seqs[j]
	})

	return seqs, nil
}
