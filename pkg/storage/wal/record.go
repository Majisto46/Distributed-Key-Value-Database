package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// RecordType distinguishes mutations in the WAL.
type RecordType byte

const (
	// RecordTypePut represents an insertion or update of a key-value pair.
	RecordTypePut RecordType = 1
	// RecordTypeDelete represents a tombstone marker for key deletion.
	RecordTypeDelete RecordType = 2
)

func (t RecordType) String() string {
	switch t {
	case RecordTypePut:
		return "PUT"
	case RecordTypeDelete:
		return "DELETE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

// HeaderSize is the fixed size of a record header in bytes:
// CRC32 (4B) + Timestamp (8B) + RecordType (1B) + KeyLen (4B) + ValLen (4B) = 21 Bytes.
const HeaderSize = 4 + 8 + 1 + 4 + 4

var (
	// ErrCorruptRecord is returned when the CRC32 checksum does not match the payload.
	ErrCorruptRecord = errors.New("wal: record checksum mismatch (corrupted data)")
	// ErrTornWrite is returned when the record ends prematurely at EOF.
	ErrTornWrite = errors.New("wal: incomplete record at EOF (torn write)")
)

// Record represents a single atomic mutation logged to disk.
type Record struct {
	Timestamp int64      // Unix nanoseconds when the mutation occurred
	Type      RecordType // Put or Delete
	Key       []byte     // User key
	Value     []byte     // User value (empty for deletes)
}

// Encode serializes the record into its binary on-disk representation:
// [CRC32 (4B)][Timestamp (8B)][Type (1B)][KeyLen (4B)][ValLen (4B)][Key (N B)][Val (M B)]
func (r *Record) Encode() []byte {
	kLen := len(r.Key)
	vLen := len(r.Value)
	totalLen := HeaderSize + kLen + vLen

	buf := make([]byte, totalLen)

	// Encode payload fields starting at offset 4 (after CRC32 slot)
	binary.BigEndian.PutUint64(buf[4:12], uint64(r.Timestamp))
	buf[12] = byte(r.Type)
	binary.BigEndian.PutUint32(buf[13:17], uint32(kLen))
	binary.BigEndian.PutUint32(buf[17:21], uint32(vLen))

	copy(buf[21:21+kLen], r.Key)
	copy(buf[21+kLen:], r.Value)

	// Checksum covers everything from byte 4 to end (the entire payload)
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], checksum)

	return buf
}

// DecodeRecord reads and validates a single record from the provided io.Reader.
// If an uncorrupted EOF is hit before any bytes are read, io.EOF is returned.
// If partial bytes are read before EOF, ErrTornWrite is returned.
func DecodeRecord(reader io.Reader) (*Record, error) {
	headerBuf := make([]byte, HeaderSize)
	_, err := io.ReadFull(reader, headerBuf)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrTornWrite
		}
		return nil, err
	}

	expectedCRC := binary.BigEndian.Uint32(headerBuf[0:4])
	timestamp := int64(binary.BigEndian.Uint64(headerBuf[4:12]))
	recType := RecordType(headerBuf[12])
	kLen := binary.BigEndian.Uint32(headerBuf[13:17])
	vLen := binary.BigEndian.Uint32(headerBuf[17:21])

	// Read key and value payload
	payloadLen := int(kLen + vLen)
	payloadBuf := make([]byte, payloadLen)
	if payloadLen > 0 {
		_, err := io.ReadFull(reader, payloadBuf)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, ErrTornWrite
			}
			return nil, err
		}
	}

	// Validate CRC32 checksum over (header minus CRC) + payload
	crcHash := crc32.NewIEEE()
	crcHash.Write(headerBuf[4:])
	crcHash.Write(payloadBuf)
	actualCRC := crcHash.Sum32()

	if actualCRC != expectedCRC {
		return nil, fmt.Errorf("%w: expected 0x%08x, got 0x%08x", ErrCorruptRecord, expectedCRC, actualCRC)
	}

	key := make([]byte, kLen)
	copy(key, payloadBuf[:kLen])

	value := make([]byte, vLen)
	copy(value, payloadBuf[kLen:])

	return &Record{
		Timestamp: timestamp,
		Type:      recType,
		Key:       key,
		Value:     value,
	}, nil
}

