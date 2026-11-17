package mvcc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

var (
	// ErrInvalidInternalKey is returned when an internal MVCC key is malformed.
	ErrInvalidInternalKey = errors.New("mvcc: invalid internal key encoding")
)

// EncodeKey converts a user-facing key and commit timestamp into a memcomparable
// internal key suitable for LSM-tree storage.
//
// Invariant & Sorting Guarantees:
// 1. All versions of Key A sort contiguously.
// 2. Different keys sort in strict lexicographical order (Key A < Key B).
// 3. For any given key, newer versions (higher ts) sort BEFORE older versions (lower ts).
//
// Format:
// [Escaped UserKey bytes] [0x00 0x00 (Delimiter)] [8 bytes BigEndian: (MaxUint64 - ts)]
func EncodeKey(userKey []byte, ts uint64) []byte {
	var buf bytes.Buffer

	// Escape 0x00 bytes in userKey as 0x00 0xFF
	for _, b := range userKey {
		if b == 0x00 {
			buf.WriteByte(0x00)
			buf.WriteByte(0xFF)
		} else {
			buf.WriteByte(b)
		}
	}

	// Delimiter: 0x00 0x00
	buf.WriteByte(0x00)
	buf.WriteByte(0x00)

	// Inverted timestamp: MaxUint64 - ts ensures higher timestamps have lower byte values
	invertedTS := math.MaxUint64 - ts
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, invertedTS)
	buf.Write(tsBytes)

	return buf.Bytes()
}

// DecodeKey parses an internal key back into its original user key and timestamp.
func DecodeKey(internalKey []byte) ([]byte, uint64, error) {
	if len(internalKey) < 10 { // at least 2 delimiter bytes + 8 ts bytes
		return nil, 0, ErrInvalidInternalKey
	}

	n := len(internalKey)
	tsBytes := internalKey[n-8:]
	invertedTS := binary.BigEndian.Uint64(tsBytes)
	ts := math.MaxUint64 - invertedTS

	// Everything before the last 8 bytes must end with 0x00 0x00
	payload := internalKey[:n-8]
	if len(payload) < 2 || payload[len(payload)-2] != 0x00 || payload[len(payload)-1] != 0x00 {
		return nil, 0, ErrInvalidInternalKey
	}

	escapedKey := payload[:len(payload)-2]
	var userKey []byte

	// Unescape 0x00 0xFF -> 0x00
	for i := 0; i < len(escapedKey); i++ {
		if escapedKey[i] == 0x00 {
			if i+1 < len(escapedKey) && escapedKey[i+1] == 0xFF {
				userKey = append(userKey, 0x00)
				i++
			} else {
				return nil, 0, ErrInvalidInternalKey
			}
		} else {
			userKey = append(userKey, escapedKey[i])
		}
	}

	return userKey, ts, nil
}

