package mvcc

import (
	"errors"
	"os"
	"testing"

	"github.com/distributed-kv/engine/pkg/storage"
)

func setupTestMVCC(t *testing.T) (*MVCC, func()) {
	tmpDir, err := os.MkdirTemp("", "mvcc_test_*")
	if err != nil {
		t.Fatal(err)
	}

	opts := storage.DefaultEngineOptions(tmpDir)
	engine, err := storage.Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	m := NewMVCC(engine)
	cleanup := func() {
		engine.Close()
		os.RemoveAll(tmpDir)
	}

	return m, cleanup
}

func TestMVCC_SnapshotIsolation(t *testing.T) {
	m, cleanup := setupTestMVCC(t)
	defer cleanup()

	// Initial write: account:1 = $100
	t1 := m.NewTransaction()
	t1.Set([]byte("account:1"), []byte("100"))
	if _, err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit failed: %v", err)
	}

	// Long-running reader transaction starts (takes snapshot)
	readerTxn := m.NewTransaction()

	// While readerTxn is open, a writer transaction updates account:1 = $500
	writerTxn := m.NewTransaction()
	writerTxn.Set([]byte("account:1"), []byte("500"))
	if _, err := writerTxn.Commit(); err != nil {
		t.Fatalf("writerTxn commit failed: %v", err)
	}

	// Reader transaction reads account:1
	// MUST see $100 due to Snapshot Isolation (readers never see writes committed after their readTS)
	val, err := readerTxn.Get([]byte("account:1"))
	if err != nil {
		t.Fatalf("readerTxn get failed: %v", err)
	}
	if string(val) != "100" {
		t.Fatalf("Snapshot Isolation violated: expected $100, got %s", string(val))
	}

	// A new transaction starting NOW must see the updated $500
	newTxn := m.NewTransaction()
	newVal, err := newTxn.Get([]byte("account:1"))
	if err != nil || string(newVal) != "500" {
		t.Fatalf("expected $500 for new transaction, got %s (err: %v)", string(newVal), err)
	}
}

func TestMVCC_WriteConflict(t *testing.T) {
	m, cleanup := setupTestMVCC(t)
	defer cleanup()

	// Setup initial inventory
	initTxn := m.NewTransaction()
	initTxn.Set([]byte("inventory:laptop"), []byte("10"))
	if _, err := initTxn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Two concurrent transactions start simultaneously
	txnA := m.NewTransaction()
	txnB := m.NewTransaction()

	// Both read inventory
	vA, _ := txnA.Get([]byte("inventory:laptop"))
	vB, _ := txnB.Get([]byte("inventory:laptop"))
	if string(vA) != "10" || string(vB) != "10" {
		t.Fatal("both should have read 10")
	}

	// Both prepare to update inventory
	txnA.Set([]byte("inventory:laptop"), []byte("9"))
	txnB.Set([]byte("inventory:laptop"), []byte("8"))

	// Txn A commits first
	if _, err := txnA.Commit(); err != nil {
		t.Fatalf("txnA should succeed, got %v", err)
	}

	// Txn B tries to commit -> MUST be aborted due to write conflict!
	_, err := txnB.Commit()
	if !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("expected ErrWriteConflict for Txn B, got: %v", err)
	}
}

func TestMVCC_PointInTimeDeletes(t *testing.T) {
	m, cleanup := setupTestMVCC(t)
	defer cleanup()

	// Write document
	t1 := m.NewTransaction()
	t1.Set([]byte("doc:42"), []byte("content_v1"))
	t1CommitTS, err := t1.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Delete document in second transaction
	t2 := m.NewTransaction()
	t2.Delete([]byte("doc:42"))
	t2CommitTS, err := t2.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Point-in-time read at t1CommitTS: MUST find document
	val, err := m.Get([]byte("doc:42"), t1CommitTS)
	if err != nil || string(val) != "content_v1" {
		t.Fatalf("expected content_v1 at ts %d, got %s (err: %v)", t1CommitTS, string(val), err)
	}

	// Point-in-time read at t2CommitTS: MUST return ErrKeyNotFound (tombstone)
	_, err = m.Get([]byte("doc:42"), t2CommitTS)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound at ts %d, got %v", t2CommitTS, err)
	}
}

