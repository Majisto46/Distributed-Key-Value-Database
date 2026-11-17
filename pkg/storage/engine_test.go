package storage

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/distributed-kv/engine/pkg/storage/wal"
)

func TestEngine_BasicCRUD(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_test_crud_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultEngineOptions(tmpDir)
	engine, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer engine.Close()

	// Put
	if err := engine.Put([]byte("name"), []byte("Alice")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Get
	val, err := engine.Get([]byte("name"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(val) != "Alice" {
		t.Fatalf("expected Alice, got %s", string(val))
	}

	// Update
	if err := engine.Put([]byte("name"), []byte("Bob")); err != nil {
		t.Fatalf("update failed: %v", err)
	}
	val, err = engine.Get([]byte("name"))
	if err != nil || string(val) != "Bob" {
		t.Fatalf("expected Bob, got %s (err: %v)", string(val), err)
	}

	// Delete
	if err := engine.Delete([]byte("name")); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	_, err = engine.Get([]byte("name"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestEngine_MemTableFlush(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_test_flush_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultEngineOptions(tmpDir)
	opts.MemTableThreshold = 1024 // 1KB to force flush quickly

	engine, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	// Write 100 entries, which will trigger flush to SSTable
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("user_%04d", i)
		v := fmt.Sprintf("payload_data_%04d", i)
		if err := engine.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	// Force synchronous flush for any remaining records
	if err := engine.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Check that .sst files exist in directory
	ssts, err := listSSTables(tmpDir)
	if err != nil || len(ssts) == 0 {
		t.Fatalf("expected sstable files on disk, found %d (err: %v)", len(ssts), err)
	}

	// Reopen engine and read all 100 entries from SSTables
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer reopened.Close()

	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("user_%04d", i)
		expectedVal := fmt.Sprintf("payload_data_%04d", i)
		val, err := reopened.Get([]byte(k))
		if err != nil {
			t.Fatalf("failed to get %s: %v", k, err)
		}
		if string(val) != expectedVal {
			t.Fatalf("key %s: got %s, expected %s", k, string(val), expectedVal)
		}
	}
}

func TestEngine_CrashRecovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_test_recovery_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultEngineOptions(tmpDir)
	opts.MemTableThreshold = 10 * 1024 * 1024 // 10MB (MemTable will NOT flush)

	engine, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	// Write mutations to WAL and MemTable
	entries := map[string]string{
		"session:101": "token_abc",
		"session:102": "token_def",
		"session:103": "token_ghi",
	}
	for k, v := range entries {
		if err := engine.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	// Delete one key before crash
	if err := engine.Delete([]byte("session:102")); err != nil {
		t.Fatal(err)
	}

	// Simulate sudden crash: close the WAL cleanly (or simulate process kill without flushing MemTable to SSTable)
	// We deliberately do NOT call engine.Close() (which flushes MemTable).
	// We only sync and close the WAL handle to simulate process death where RAM is lost.
	engine.mu.Lock()
	_ = engine.walInstance.Sync()
	_ = engine.walInstance.Close()
	engine.mu.Unlock()

	// Ensure no SSTables exist
	sstFiles, _ := listSSTables(tmpDir)
	if len(sstFiles) != 0 {
		t.Fatalf("expected 0 SSTables before recovery, found %d", len(sstFiles))
	}

	// Reopen database: it must replay the WAL and restore activeMem
	recoveredEngine, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine after crash: %v", err)
	}
	defer recoveredEngine.Close()

	// Check session:101 exists
	v101, err := recoveredEngine.Get([]byte("session:101"))
	if err != nil || string(v101) != "token_abc" {
		t.Fatalf("expected session:101 = token_abc, got %s (err: %v)", string(v101), err)
	}

	// Check session:103 exists
	v103, err := recoveredEngine.Get([]byte("session:103"))
	if err != nil || string(v103) != "token_ghi" {
		t.Fatalf("expected session:103 = token_ghi, got %s (err: %v)", string(v103), err)
	}

	// Check deleted session:102 returns ErrKeyNotFound
	_, err = recoveredEngine.Get([]byte("session:102"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for deleted key, got %v", err)
	}
}

func TestEngine_Compaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_test_compaction_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultEngineOptions(tmpDir)
	engine, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Flush 1: initial values
	engine.Put([]byte("k1"), []byte("v1_old"))
	engine.Put([]byte("k2"), []byte("v2_initial"))
	engine.Put([]byte("k3"), []byte("v3_to_delete"))
	if err := engine.Flush(); err != nil {
		t.Fatal(err)
	}

	// Flush 2: update k1, delete k3, add k4
	engine.Put([]byte("k1"), []byte("v1_newest"))
	engine.Delete([]byte("k3"))
	engine.Put([]byte("k4"), []byte("v4_fresh"))
	if err := engine.Flush(); err != nil {
		t.Fatal(err)
	}

	// Check pre-compaction state
	ssts, _ := listSSTables(tmpDir)
	if len(ssts) != 2 {
		t.Fatalf("expected 2 SSTables before compaction, got %d", len(ssts))
	}

	// Run Compaction
	if err := engine.Compact(); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	// Post-compaction: should now be exactly 1 consolidated SSTable!
	sstsAfter, _ := listSSTables(tmpDir)
	if len(sstsAfter) != 1 {
		t.Fatalf("expected 1 consolidated SSTable after compaction, got %d", len(sstsAfter))
	}

	// Verify values after compaction
	v1, err := engine.Get([]byte("k1"))
	if err != nil || string(v1) != "v1_newest" {
		t.Fatalf("expected v1_newest, got %s", string(v1))
	}

	v2, err := engine.Get([]byte("k2"))
	if err != nil || string(v2) != "v2_initial" {
		t.Fatalf("expected v2_initial, got %s", string(v2))
	}

	_, err = engine.Get([]byte("k3"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for deleted k3, got %v", err)
	}

	v4, err := engine.Get([]byte("k4"))
	if err != nil || string(v4) != "v4_fresh" {
		t.Fatalf("expected v4_fresh, got %s", string(v4))
	}
}

func TestEngine_ConcurrentAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_test_concurrent_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultEngineOptions(tmpDir)
	opts.WALSyncPolicy = wal.SyncNone // Faster for pure stress concurrency test
	engine, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	const numGoroutines = 8
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Writers
	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				k := fmt.Sprintf("ck_%d_%d", gid, i)
				v := fmt.Sprintf("cv_%d_%d", gid, i)
				_ = engine.Put([]byte(k), []byte(v))
			}
		}(g)
	}

	// Readers
	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				k := fmt.Sprintf("ck_%d_%d", gid, i)
				_, _ = engine.Get([]byte(k))
			}
		}(g)
	}

	wg.Wait()
}
