package wal

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestWAL_BasicWriteAndRecover(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test_basic_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultOptions(tmpDir)
	opts.SyncPolicy = SyncAlways

	w, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	entries := []struct {
		recType RecordType
		key     string
		val     string
	}{
		{RecordTypePut, "user:1", `{"name":"Alice"}`},
		{RecordTypePut, "user:2", `{"name":"Bob"}`},
		{RecordTypeDelete, "user:1", ""},
		{RecordTypePut, "user:3", `{"name":"Charlie"}`},
	}

	for _, e := range entries {
		if err := w.Write(e.recType, []byte(e.key), []byte(e.val)); err != nil {
			t.Fatalf("failed to write %s: %v", e.key, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("failed to close wal: %v", err)
	}

	// Recover and assert all records match exactly in order
	var recovered []*Record
	err = Recover(tmpDir, func(rec *Record) error {
		recovered = append(recovered, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Fatalf("expected %d records, got %d", len(entries), len(recovered))
	}

	for i, expected := range entries {
		actual := recovered[i]
		if actual.Type != expected.recType {
			t.Errorf("entry %d: type mismatch, got %v want %v", i, actual.Type, expected.recType)
		}
		if string(actual.Key) != expected.key {
			t.Errorf("entry %d: key mismatch, got %s want %s", i, string(actual.Key), expected.key)
		}
		if string(actual.Value) != expected.val {
			t.Errorf("entry %d: val mismatch, got %s want %s", i, string(actual.Value), expected.val)
		}
		if actual.Timestamp <= 0 {
			t.Errorf("entry %d: timestamp should be positive", i)
		}
	}
}

func TestWAL_SegmentRotation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test_rotation_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := Options{
		Dir:         tmpDir,
		MaxFileSize: 120, // small size to force frequent rotation
		SyncPolicy:  SyncAlways,
	}

	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	// Write 10 records, which should trigger multiple rotations
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key_%04d", i)
		v := fmt.Sprintf("val_%08d", i)
		if err := w.Write(RecordTypePut, []byte(k), []byte(v)); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}

	seq := w.ActiveSequence()
	if seq <= 1 {
		t.Fatalf("expected active sequence to be > 1 due to rotation, got %d", seq)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Ensure recovery spans all segments properly
	count := 0
	err = Recover(tmpDir, func(rec *Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("recovery across segments failed: %v", err)
	}
	if count != 10 {
		t.Fatalf("expected 10 recovered records, got %d", count)
	}
}

func TestWAL_TornWriteHandling(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test_torn_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultOptions(tmpDir)
	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Write(RecordTypePut, []byte("key1"), []byte("val1")); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(RecordTypePut, []byte("key2"), []byte("val2")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a truncated / torn record directly to the file to simulate power cut
	segPath := segmentFileName(tmpDir, 1)
	f, err := os.OpenFile(segPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}

	incompleteRecord := (&Record{
		Timestamp: 123456789,
		Type:      RecordTypePut,
		Key:       []byte("key_crash"),
		Value:     []byte("value_crash"),
	}).Encode()

	// Only write half the record!
	_, err = f.Write(incompleteRecord[:len(incompleteRecord)-5])
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Recovery should recover key1 and key2 without crashing, stopping at the torn write
	var recoveredKeys []string
	err = Recover(tmpDir, func(rec *Record) error {
		recoveredKeys = append(recoveredKeys, string(rec.Key))
		return nil
	})
	if err != nil {
		t.Fatalf("recovery failed on torn write: %v", err)
	}

	if len(recoveredKeys) != 2 || recoveredKeys[0] != "key1" || recoveredKeys[1] != "key2" {
		t.Fatalf("unexpected recovery result on torn write: %v", recoveredKeys)
	}
}

func TestWAL_CorruptionDetection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test_corrupt_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultOptions(tmpDir)
	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Write(RecordTypePut, []byte("good_key"), []byte("good_value")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt a byte in the payload
	segPath := segmentFileName(tmpDir, 1)
	data, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatal(err)
	}

	// Flip a bit in the payload (byte 22)
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(segPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	err = Recover(tmpDir, func(rec *Record) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected recovery to fail on corrupted record, but got nil error")
	}
}

func TestWAL_ConcurrentWrites(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test_concurrent_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultOptions(tmpDir)
	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 8
	const writesPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				key := fmt.Sprintf("thread_%d_key_%d", gid, i)
				val := fmt.Sprintf("thread_%d_val_%d", gid, i)
				if err := w.Write(RecordTypePut, []byte(key), []byte(val)); err != nil {
					t.Errorf("concurrent write failed: %v", err)
				}
			}
		}(g)
	}

	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	totalRecovered := 0
	err = Recover(tmpDir, func(rec *Record) error {
		totalRecovered++
		return nil
	})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if totalRecovered != goroutines*writesPerGoroutine {
		t.Fatalf("expected %d recovered records, got %d", goroutines*writesPerGoroutine, totalRecovered)
	}
}
