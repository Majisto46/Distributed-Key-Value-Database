package memtable

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/distributed-kv/engine/pkg/storage/wal"
)

func TestMemTable_BasicPutGetDelete(t *testing.T) {
	mem := New(1)

	now := time.Now().UnixNano()
	mem.Put([]byte("alpha"), []byte("val_alpha"), now)
	mem.Put([]byte("beta"), []byte("val_beta"), now+1)
	mem.Put([]byte("gamma"), []byte("val_gamma"), now+2)

	// Verify lookups
	entry, found := mem.Get([]byte("beta"))
	if !found {
		t.Fatal("expected beta to be found")
	}
	if string(entry.Value) != "val_beta" {
		t.Fatalf("expected val_beta, got %s", string(entry.Value))
	}
	if entry.Type != wal.RecordTypePut {
		t.Fatalf("expected RecordTypePut, got %v", entry.Type)
	}

	// Overwrite beta
	mem.Put([]byte("beta"), []byte("val_beta_v2"), now+3)
	entry, found = mem.Get([]byte("beta"))
	if !found || string(entry.Value) != "val_beta_v2" {
		t.Fatalf("expected updated val_beta_v2, got %s", string(entry.Value))
	}

	// Delete alpha (tombstone)
	mem.Delete([]byte("alpha"), now+4)
	entry, found = mem.Get([]byte("alpha"))
	if !found {
		t.Fatal("expected alpha entry to still exist as tombstone")
	}
	if entry.Type != wal.RecordTypeDelete {
		t.Fatalf("expected RecordTypeDelete tombstone, got %v", entry.Type)
	}

	// Non-existent key
	_, found = mem.Get([]byte("zeta"))
	if found {
		t.Fatal("expected zeta to not exist")
	}
}

func TestMemTable_Iterator(t *testing.T) {
	mem := New(1)
	keys := []string{"orange", "apple", "banana", "grape", "mango"}
	for i, k := range keys {
		mem.Put([]byte(k), []byte("v_"+k), int64(i+1))
	}

	it := mem.Iterator()
	it.SeekToFirst()

	var ordered []string
	for ; it.Valid(); it.Next() {
		ordered = append(ordered, string(it.Entry().Key))
	}

	expectedOrder := []string{"apple", "banana", "grape", "mango", "orange"}
	if len(ordered) != len(expectedOrder) {
		t.Fatalf("expected %d elements, got %d", len(expectedOrder), len(ordered))
	}
	for i := range expectedOrder {
		if ordered[i] != expectedOrder[i] {
			t.Errorf("at index %d: expected %s, got %s", i, expectedOrder[i], ordered[i])
		}
	}

	// Test Seek
	it.Seek([]byte("c"))
	if !it.Valid() || string(it.Entry().Key) != "grape" {
		t.Fatalf("seek('c') should point to 'grape', got %v", it.Entry())
	}
}

func TestMemTable_Concurrency(t *testing.T) {
	mem := New(1)
	const writers = 8
	const readers = 8
	const items = 100

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	// Writers
	for w := 0; w < writers; w++ {
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < items; i++ {
				k := fmt.Sprintf("key_%d_%d", wid, i)
				v := fmt.Sprintf("val_%d_%d", wid, i)
				mem.Put([]byte(k), []byte(v), time.Now().UnixNano())
			}
		}(w)
	}

	// Readers
	for r := 0; r < readers; r++ {
		go func(rid int) {
			defer wg.Done()
			for i := 0; i < items; i++ {
				k := fmt.Sprintf("key_%d_%d", rid%writers, i)
				mem.Get([]byte(k))
			}
		}(r)
	}

	wg.Wait()

	if mem.Count() != int64(writers*items) {
		t.Fatalf("expected %d keys, got %d", writers*items, mem.Count())
	}
}

