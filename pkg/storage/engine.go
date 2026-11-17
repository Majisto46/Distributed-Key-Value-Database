package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distributed-kv/engine/pkg/storage/memtable"
	"github.com/distributed-kv/engine/pkg/storage/sstable"
	"github.com/distributed-kv/engine/pkg/storage/wal"
)

var (
	// ErrKeyNotFound indicates the requested key does not exist or has been deleted.
	ErrKeyNotFound = errors.New("storage: key not found")
	// ErrEngineClosed is returned when an operation is performed on a closed engine.
	ErrEngineClosed = errors.New("storage: engine closed")
)

// EngineOptions configures the LSM-Tree storage engine.
type EngineOptions struct {
	DataDir            string         // Directory where data files are stored
	MemTableThreshold  int64          // Max byte size for MemTable before flushing (e.g. 4MB)
	WALSyncPolicy      wal.SyncPolicy // SyncAlways, SyncBatch, SyncNone
	SSTableBlockSize   int            // Target SSTable block size (e.g. 4KB)
}

// DefaultEngineOptions returns standard production defaults.
func DefaultEngineOptions(dataDir string) EngineOptions {
	return EngineOptions{
		DataDir:           dataDir,
		MemTableThreshold: 4 * 1024 * 1024, // 4MB
		WALSyncPolicy:     wal.SyncAlways,
		SSTableBlockSize:  4 * 1024,
	}
}

// Engine implements an LSM-Tree storage engine with WAL, MemTable, SSTables, and Compaction.
type Engine struct {
	mu           sync.RWMutex
	opts         EngineOptions
	walInstance  *wal.WAL
	activeMem    *memtable.MemTable
	immMem       *memtable.MemTable
	sstables     []*sstable.Reader // Index 0 is newest, N-1 is oldest
	sstSeq       uint64
	isClosed     bool
	flushMu      sync.Mutex // Serializes background flushes
}

// Open initializes or recovers an LSM-tree database from the given directory.
func Open(opts EngineOptions) (*Engine, error) {
	if err := os.MkdirAll(opts.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	walDir := filepath.Join(opts.DataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal dir: %w", err)
	}

	e := &Engine{
		opts: opts,
	}

	// 1. Discover and load existing SSTables
	sstFiles, err := listSSTables(opts.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to discover sstables: %w", err)
	}

	// SSTables ordered newest (highest seq) to oldest (lowest seq)
	for i := len(sstFiles) - 1; i >= 0; i-- {
		sstPath := filepath.Join(opts.DataDir, sstFiles[i].name)
		reader, err := sstable.OpenReader(sstPath)
		if err != nil {
			e.Close()
			return nil, fmt.Errorf("failed to open sstable %s: %w", sstPath, err)
		}
		e.sstables = append(e.sstables, reader)
		if sstFiles[i].seq > e.sstSeq {
			e.sstSeq = sstFiles[i].seq
		}
	}

	// 2. Open active WAL
	walOpts := wal.DefaultOptions(walDir)
	walOpts.SyncPolicy = opts.WALSyncPolicy
	activeWAL, err := wal.Open(walOpts)
	if err != nil {
		e.Close()
		return nil, fmt.Errorf("failed to open wal: %w", err)
	}
	e.walInstance = activeWAL

	// 3. Rebuild active MemTable from existing WAL records (Crash Recovery)
	activeMem := memtable.New(activeWAL.ActiveSequence())
	err = wal.Recover(walDir, func(rec *wal.Record) error {
		if rec.Type == wal.RecordTypePut {
			activeMem.Put(rec.Key, rec.Value, rec.Timestamp)
		} else if rec.Type == wal.RecordTypeDelete {
			activeMem.Delete(rec.Key, rec.Timestamp)
		}
		return nil
	})
	if err != nil {
		e.Close()
		return nil, fmt.Errorf("wal crash recovery replay failed: %w", err)
	}
	e.activeMem = activeMem

	return e, nil
}

// Put writes or updates a key-value pair.
func (e *Engine) Put(key, value []byte) error {
	return e.writeRecord(wal.RecordTypePut, key, value)
}

// Delete marks a key as deleted with a tombstone.
func (e *Engine) Delete(key []byte) error {
	return e.writeRecord(wal.RecordTypeDelete, key, nil)
}

// writeRecord appends to WAL first, then applies to active MemTable.
func (e *Engine) writeRecord(recType wal.RecordType, key, value []byte) error {
	e.mu.Lock()
	if e.isClosed {
		e.mu.Unlock()
		return ErrEngineClosed
	}

	// 1. Write to Write-Ahead Log (durability)
	if err := e.walInstance.Write(recType, key, value); err != nil {
		e.mu.Unlock()
		return fmt.Errorf("wal write failed: %w", err)
	}

	// 2. Write to in-memory MemTable
	now := time.Now().UnixNano()
	if recType == wal.RecordTypePut {
		e.activeMem.Put(key, value, now)
	} else {
		e.activeMem.Delete(key, now)
	}

	// 3. Check if MemTable needs to flush
	needsFlush := e.activeMem.Size() >= e.opts.MemTableThreshold && e.immMem == nil
	if needsFlush {
		e.immMem = e.activeMem
		e.activeMem = memtable.New(e.walInstance.ActiveSequence())
		e.mu.Unlock()

		// Trigger flush
		go e.flushMemTable()
		return nil
	}

	e.mu.Unlock()
	return nil
}

// Get queries for the latest value of key.
func (e *Engine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	if e.isClosed {
		e.mu.RUnlock()
		return nil, ErrEngineClosed
	}

	// 1. Check active MemTable
	if entry, found := e.activeMem.Get(key); found {
		e.mu.RUnlock()
		if entry.Type == wal.RecordTypeDelete {
			return nil, ErrKeyNotFound
		}
		res := make([]byte, len(entry.Value))
		copy(res, entry.Value)
		return res, nil
	}

	// 2. Check immutable MemTable (if flushing)
	if e.immMem != nil {
		if entry, found := e.immMem.Get(key); found {
			e.mu.RUnlock()
			if entry.Type == wal.RecordTypeDelete {
				return nil, ErrKeyNotFound
			}
			res := make([]byte, len(entry.Value))
			copy(res, entry.Value)
			return res, nil
		}
	}

	// Snapshot active SSTables for reading
	tables := make([]*sstable.Reader, len(e.sstables))
	copy(tables, e.sstables)
	e.mu.RUnlock()

	// 3. Check SSTables from newest to oldest
	for _, table := range tables {
		rec, err := table.Get(key)
		if err == nil {
			if rec.Type == wal.RecordTypeDelete {
				return nil, ErrKeyNotFound
			}
			res := make([]byte, len(rec.Value))
			copy(res, rec.Value)
			return res, nil
		}
		if errors.Is(err, sstable.ErrKeyNotFound) {
			continue
		}
		return nil, err
	}

	return nil, ErrKeyNotFound
}

// Flush synchronously flushes the active MemTable to a new SSTable file.
func (e *Engine) Flush() error {
	e.mu.Lock()
	if e.isClosed {
		e.mu.Unlock()
		return ErrEngineClosed
	}

	if e.activeMem.Count() == 0 {
		e.mu.Unlock()
		return nil
	}

	e.immMem = e.activeMem
	e.activeMem = memtable.New(e.walInstance.ActiveSequence())
	e.mu.Unlock()

	return e.flushMemTable()
}

// flushMemTable writes the immutable MemTable to a new SSTable file on disk.
func (e *Engine) flushMemTable() error {
	e.flushMu.Lock()
	defer e.flushMu.Unlock()

	e.mu.RLock()
	imm := e.immMem
	if imm == nil || imm.Count() == 0 {
		e.mu.RUnlock()
		return nil
	}
	e.mu.RUnlock()

	seq := atomic.AddUint64(&e.sstSeq, 1)
	sstName := fmt.Sprintf("%08d.sst", seq)
	sstPath := filepath.Join(e.opts.DataDir, sstName)

	writerOpts := sstable.WriterOptions{
		BlockSize:         e.opts.SSTableBlockSize,
		ExpectedKeys:      int(imm.Count()),
		FalsePositiveRate: 0.01,
	}

	writer, err := sstable.NewWriter(sstPath, writerOpts)
	if err != nil {
		return fmt.Errorf("failed to create sstable writer: %w", err)
	}

	it := imm.Iterator()
	it.SeekToFirst()
	for ; it.Valid(); it.Next() {
		entry := it.Entry()
		rec := &wal.Record{
			Timestamp: entry.Timestamp,
			Type:      entry.Type,
			Key:       entry.Key,
			Value:     entry.Value,
		}
		if err := writer.Append(rec); err != nil {
			writer.Close()
			return fmt.Errorf("failed to write record during flush: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to finalize sstable flush: %w", err)
	}

	// Open reader for the newly flushed SSTable
	reader, err := sstable.OpenReader(sstPath)
	if err != nil {
		return fmt.Errorf("failed to open reader for new sstable: %w", err)
	}

	// Atomically prepend new SSTable and clear immMem
	e.mu.Lock()
	e.sstables = append([]*sstable.Reader{reader}, e.sstables...)
	e.immMem = nil
	walSeqToPurge := imm.WALSequence()
	e.mu.Unlock()

	// Purge older WAL segments now that data is persisted in SSTable
	_ = e.walInstance.PurgeOldSegments(walSeqToPurge)
	return nil
}

// Compact merges all current SSTables into a single consolidated SSTable,
// eliminating tombstones and overwritten versions.
func (e *Engine) Compact() error {
	e.flushMu.Lock()
	defer e.flushMu.Unlock()

	e.mu.Lock()
	if e.isClosed {
		e.mu.Unlock()
		return ErrEngineClosed
	}

	if len(e.sstables) <= 1 {
		e.mu.Unlock()
		return nil // Nothing to compact
	}

	oldTables := make([]*sstable.Reader, len(e.sstables))
	copy(oldTables, e.sstables)
	e.mu.Unlock()

	seq := atomic.AddUint64(&e.sstSeq, 1)
	outName := fmt.Sprintf("%08d.sst", seq)
	outPath := filepath.Join(e.opts.DataDir, outName)

	_, err := CompactSSTables(oldTables, outPath)
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Open compacted reader
	newReader, err := sstable.OpenReader(outPath)
	if err != nil {
		return fmt.Errorf("failed to open compacted sstable: %w", err)
	}

	e.mu.Lock()
	e.sstables = []*sstable.Reader{newReader}
	e.mu.Unlock()

	// Close and delete obsolete SSTable files
	for _, t := range oldTables {
		path := t.Path()
		t.Close()
		_ = os.Remove(path)
	}

	return nil
}

// Close gracefully flushes data, closes WAL, and releases all file handles.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.isClosed {
		e.mu.Unlock()
		return nil
	}
	e.isClosed = true
	e.mu.Unlock()

	// Flush remaining in-memory writes
	if e.activeMem != nil && e.activeMem.Count() > 0 {
		e.immMem = e.activeMem
		_ = e.flushMemTable()
	}

	var firstErr error
	if e.walInstance != nil {
		if err := e.walInstance.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, sst := range e.sstables {
		if err := sst.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

type sstFileInfo struct {
	name string
	seq  uint64
}

func listSSTables(dir string) ([]sstFileInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []sstFileInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sst") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".sst")
		seq, err := strconv.ParseUint(base, 10, 64)
		if err == nil {
			files = append(files, sstFileInfo{
				name: entry.Name(),
				seq:  seq,
			})
		}
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].seq < files[j].seq
	})

	return files, nil
}

