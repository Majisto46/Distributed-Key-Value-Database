package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/distributed-kv/engine/pkg/storage/wal"
)

func TestSSTable_WriteAndRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstable_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	sstPath := filepath.Join(tmpDir, "000001.sst")
	opts := WriterOptions{
		BlockSize:         256, // small block size to force multiple blocks
		ExpectedKeys:      500,
		FalsePositiveRate: 0.01,
	}

	w, err := NewWriter(sstPath, opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	totalKeys := 300
	for i := 0; i < totalKeys; i++ {
		rec := &wal.Record{
			Timestamp: int64(1000 + i),
			Type:      wal.RecordTypePut,
			Key:       []byte(fmt.Sprintf("key_%05d", i)),
			Value:     []byte(fmt.Sprintf("value_payload_%05d", i)),
		}
		if err := w.Append(rec); err != nil {
			t.Fatalf("failed to append record %d: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Open Reader
	r, err := OpenReader(sstPath)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer r.Close()

	if len(r.IndexEntries()) <= 1 {
		t.Fatalf("expected multiple blocks in index, got %d", len(r.IndexEntries()))
	}

	// 1. Verify all written keys exist and have exact values
	for i := 0; i < totalKeys; i++ {
		targetKey := fmt.Sprintf("key_%05d", i)
		rec, err := r.Get([]byte(targetKey))
		if err != nil {
			t.Fatalf("failed to get %s: %v", targetKey, err)
		}
		expectedVal := fmt.Sprintf("value_payload_%05d", i)
		if string(rec.Value) != expectedVal {
			t.Fatalf("key %s: got val %s, expected %s", targetKey, string(rec.Value), expectedVal)
		}
	}

	// 2. Verify non-existent keys return ErrKeyNotFound
	missingKeys := []string{"key_99999", "non_existent_key", "aaa_before_first"}
	for _, mk := range missingKeys {
		_, err := r.Get([]byte(mk))
		if err != ErrKeyNotFound {
			t.Fatalf("expected ErrKeyNotFound for %s, got %v", mk, err)
		}
	}

	// 3. Test Iterator
	it, err := r.Iterator()
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	count := 0
	for ; it.Valid(); it.Next() {
		expectedKey := fmt.Sprintf("key_%05d", count)
		if string(it.Record().Key) != expectedKey {
			t.Fatalf("iterator item %d: got %s, expected %s", count, string(it.Record().Key), expectedKey)
		}
		count++
	}

	if count != totalKeys {
		t.Fatalf("iterator counted %d keys, expected %d", count, totalKeys)
	}

	// 4. Test Iterator Seek
	it.Seek([]byte("key_00150"))
	if !it.Valid() || string(it.Record().Key) != "key_00150" {
		t.Fatalf("seek to key_00150 failed, got valid=%v", it.Valid())
	}
}

