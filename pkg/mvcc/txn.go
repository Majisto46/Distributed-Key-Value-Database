package mvcc

import (
	"fmt"
	"sync"
)

// Txn represents an atomic transaction operating under Snapshot Isolation.
type Txn struct {
	mu       sync.Mutex
	mvcc     *MVCC
	readTS   uint64
	commitTS uint64
	writes   map[string][]byte
	deletes  map[string]bool
	reads    map[string]bool
	isClosed bool
}

// NewTransaction begins a new transaction with a consistent snapshot at the current timestamp.
func (m *MVCC) NewTransaction() *Txn {
	return &Txn{
		mvcc:    m,
		readTS:  m.oracle.NextTimestamp(),
		writes:  make(map[string][]byte),
		deletes: make(map[string]bool),
		reads:   make(map[string]bool),
	}
}

// ReadTimestamp returns the snapshot timestamp of this transaction.
func (t *Txn) ReadTimestamp() uint64 {
	return t.readTS
}

// Get queries for a key within this transaction's snapshot, observing local writes.
func (t *Txn) Get(userKey []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isClosed {
		return nil, ErrTxnClosed
	}

	kStr := string(userKey)

	// 1. Check local delete buffer
	if t.deletes[kStr] {
		return nil, ErrKeyNotFound
	}

	// 2. Check local write buffer
	if val, ok := t.writes[kStr]; ok {
		res := make([]byte, len(val))
		copy(res, val)
		return res, nil
	}

	// 3. Track read for conflict detection
	t.reads[kStr] = true

	// 4. Query MVCC snapshot
	return t.mvcc.Get(userKey, t.readTS)
}

// Set buffers a write for userKey.
func (t *Txn) Set(userKey, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isClosed {
		return ErrTxnClosed
	}

	kStr := string(userKey)
	delete(t.deletes, kStr)

	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	t.writes[kStr] = valCopy

	return nil
}

// Delete buffers a deletion for userKey.
func (t *Txn) Delete(userKey []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isClosed {
		return ErrTxnClosed
	}

	kStr := string(userKey)
	delete(t.writes, kStr)
	t.deletes[kStr] = true

	return nil
}

// Commit attempts to commit the transaction under First-Committer-Wins Snapshot Isolation.
// If any key written by this transaction was committed by another transaction after our readTS,
// the commit aborts with ErrWriteConflict.
func (t *Txn) Commit() (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isClosed {
		return 0, ErrTxnClosed
	}

	// Read-only transactions commit trivially without acquiring commit lock
	if len(t.writes) == 0 && len(t.deletes) == 0 {
		t.isClosed = true
		return t.readTS, nil
	}

	// Acquire global commit lock to serialize commit validation and timestamp allocation
	t.mvcc.commitMu.Lock()
	defer t.mvcc.commitMu.Unlock()

	// 1. Conflict Detection (First-Committer-Wins)
	// For every key we want to modify, ensure no one committed a write with TS > readTS
	for kStr := range t.writes {
		if latestVal, ok := t.mvcc.latestKeys.Load(kStr); ok {
			latestTS := latestVal.(uint64)
			if latestTS > t.readTS {
				t.isClosed = true
				return 0, fmt.Errorf("%w: key '%s' modified at ts %d > readTS %d",
					ErrWriteConflict, kStr, latestTS, t.readTS)
			}
		}
	}

	for kStr := range t.deletes {
		if latestVal, ok := t.mvcc.latestKeys.Load(kStr); ok {
			latestTS := latestVal.(uint64)
			if latestTS > t.readTS {
				t.isClosed = true
				return 0, fmt.Errorf("%w: key '%s' modified at ts %d > readTS %d",
					ErrWriteConflict, kStr, latestTS, t.readTS)
			}
		}
	}

	// 2. Allocate Commit Timestamp
	t.commitTS = t.mvcc.oracle.NextTimestamp()

	// 3. Atomically write mutations to the storage engine
	for kStr, val := range t.writes {
		if err := t.mvcc.Put([]byte(kStr), val, t.commitTS); err != nil {
			return 0, fmt.Errorf("failed to commit put: %w", err)
		}
	}

	for kStr := range t.deletes {
		if err := t.mvcc.Delete([]byte(kStr), t.commitTS); err != nil {
			return 0, fmt.Errorf("failed to commit delete: %w", err)
		}
	}

	t.isClosed = true
	return t.commitTS, nil
}

// Rollback discards all uncommitted buffered writes.
func (t *Txn) Rollback() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isClosed = true
	t.writes = nil
	t.deletes = nil
}

