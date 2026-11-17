package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MagicNumber identifies a valid SSTable file ("SSTBLV01" in ASCII).
const MagicNumber uint64 = 0x535354424C563031

// FooterSize is the fixed size of the file trailer in bytes:
// IndexOffset (8B) + IndexLen (8B) + BloomOffset (8B) + BloomLen (8B) + Magic (8B) = 40B.
const FooterSize = 40

var (
	// ErrInvalidSSTable is returned when the magic number or structure is corrupt.
	ErrInvalidSSTable = errors.New("sstable: invalid magic number or corrupted footer")
	// ErrKeyNotFound is returned when a key does not exist in the SSTable.
	ErrKeyNotFound = errors.New("sstable: key not found")
)

// Footer stores file offsets for metadata components.
type Footer struct {
	IndexOffset uint64
	IndexLength uint64
	BloomOffset uint64
	BloomLength uint64
	Magic       uint64
}

// Encode serializes the 40-byte footer.
func (f *Footer) Encode() []byte {
	buf := make([]byte, FooterSize)
	binary.BigEndian.PutUint64(buf[0:8], f.IndexOffset)
	binary.BigEndian.PutUint64(buf[8:16], f.IndexLength)
	binary.BigEndian.PutUint64(buf[16:24], f.BloomOffset)
	binary.BigEndian.PutUint64(buf[24:32], f.BloomLength)
	binary.BigEndian.PutUint64(buf[32:40], f.Magic)
	return buf
}

// DecodeFooter parses the 40-byte footer.
func DecodeFooter(buf []byte) (*Footer, error) {
	if len(buf) < FooterSize {
		return nil, ErrInvalidSSTable
	}
	f := &Footer{
		IndexOffset: binary.BigEndian.Uint64(buf[0:8]),
		IndexLength: binary.BigEndian.Uint64(buf[8:16]),
		BloomOffset: binary.BigEndian.Uint64(buf[16:24]),
		BloomLength: binary.BigEndian.Uint64(buf[24:32]),
		Magic:       binary.BigEndian.Uint64(buf[32:40]),
	}
	if f.Magic != MagicNumber {
		return nil, fmt.Errorf("%w: got 0x%x, expected 0x%x", ErrInvalidSSTable, f.Magic, MagicNumber)
	}
	return f, nil
}

// IndexEntry points to a data block on disk starting with FirstKey.
type IndexEntry struct {
	FirstKey []byte
	Offset   uint64
	Length   uint64
}

// EncodeIndex serializes the sparse index slice into bytes.
func EncodeIndex(entries []IndexEntry) []byte {
	var buf bytes.Buffer
	count := uint32(len(entries))
	binary.Write(&buf, binary.BigEndian, count)

	for _, e := range entries {
		kLen := uint32(len(e.FirstKey))
		binary.Write(&buf, binary.BigEndian, kLen)
		buf.Write(e.FirstKey)
		binary.Write(&buf, binary.BigEndian, e.Offset)
		binary.Write(&buf, binary.BigEndian, e.Length)
	}
	return buf.Bytes()
}

// DecodeIndex parses a serialized sparse index slice from bytes.
func DecodeIndex(data []byte) ([]IndexEntry, error) {
	reader := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	entries := make([]IndexEntry, count)
	for i := uint32(0); i < count; i++ {
		var kLen uint32
		if err := binary.Read(reader, binary.BigEndian, &kLen); err != nil {
			return nil, err
		}
		firstKey := make([]byte, kLen)
		if _, err := io.ReadFull(reader, firstKey); err != nil {
			return nil, err
		}
		var offset, length uint64
		if err := binary.Read(reader, binary.BigEndian, &offset); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			return nil, err
		}
		entries[i] = IndexEntry{
			FirstKey: firstKey,
			Offset:   offset,
			Length:   length,
		}
	}
	return entries, nil
}

