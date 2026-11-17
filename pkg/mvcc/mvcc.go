package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/distributed-kv/engine/pkg/storage"
)

const (
	// ValueTypePut indicates a live committed value.
	ValueTypePut byte = 1
	// ValueTypeDelete indicates an MVCC tombstone marker.
	ValueTypeDelete byte = 2
)

var (
	// ErrKeyNotFound indicates no visible version of the key exists at the read timestamp.
	ErrKeyNotFound = errors.New("mvcc: key not found at snapshot timestamp")
	// ErrWriteConflict indicates another transaction committed a write to this key after read timestamp.
	ErrWriteConflict = errors.New("mvcc: write conflict detected (first-committer-wins)")
	// ErrTxnClosed is returned when operating on an already committed or aborted transaction.
	ErrTxnClosed = errors.New("mvcc: transaction already closed")
)

// MVCC orchestrates multi-version snapshot reads and atomic multi-key transactions.
type MVCC struct {
	engine     *storage.Engine
	oracle     *Oracle
	commitMu   sync.Mutex        // Serializes commit validation and timestamp assignment
	latestKeys sync.Map          // key (string) -> latest commit timestamp (uint64)
}

// NewMVCC wraps a storage engine with MVCC transactional capabilities.
func NewMVCC(engine *storage.Engine) *MVCC {
	return &MVCC{
		engine: engine,
		oracle: NewOracle(),
	}
}

// Oracle returns the timestamp generator.
func (m *MVCC) Oracle() *Oracle {
	return m.oracle
}

// Get reads the latest visible value of userKey at or before readTS.
func (m *MVCC) Get(userKey []byte, readTS uint64) ([]byte, error) {
	it, err := m.engine.NewIterator()
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}

	seekTarget := EncodeKey(userKey, readTS)
	it.Seek(seekTarget)

	if !it.Valid() {
		return nil, ErrKeyNotFound
	}

	foundKey, foundTS, err := DecodeKey(it.Record().Key)
	if err != nil {
		return nil, err
	}

	// If the found record has a different user key or ts > readTS, no visible version exists
	if !bytes.Equal(foundKey, userKey) || foundTS > readTS {
		return nil, ErrKeyNotFound
	}

	valType, rawVal, err := decodeValue(it.Record().Value)
	if err != nil {
		return nil, err
	}

	if valType == ValueTypeDelete {
		return nil, ErrKeyNotFound // Tombstoned
	}

	res := make([]byte, len(rawVal))
	copy(res, rawVal)
	return res, nil
}

// Put writes an MVCC version directly at commitTS.
func (m *MVCC) Put(userKey, value []byte, commitTS uint64) error {
	internalKey := EncodeKey(userKey, commitTS)
	valPayload := encodeValue(ValueTypePut, value)
	if err := m.engine.Put(internalKey, valPayload); err != nil {
		return err
	}
	m.latestKeys.Store(string(userKey), commitTS)
	return nil
}

// Delete writes an MVCC tombstone directly at commitTS.
func (m *MVCC) Delete(userKey []byte, commitTS uint64) error {
	internalKey := EncodeKey(userKey, commitTS)
	valPayload := encodeValue(ValueTypeDelete, nil)
	if err := m.engine.Put(internalKey, valPayload); err != nil {
		return err
	}
	m.latestKeys.Store(string(userKey), commitTS)
	return nil
}

func encodeValue(valType byte, raw []byte) []byte {
	buf := make([]byte, 1+len(raw))
	buf[0] = valType
	copy(buf[1:], raw)
	return buf
}

func decodeValue(buf []byte) (byte, []byte, error) {
	if len(buf) < 1 {
		return 0, nil, errors.New("mvcc: invalid value payload")
	}
	valType := buf[0]
	raw := buf[1:]
	return valType, raw, nil
}

