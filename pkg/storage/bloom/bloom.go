package bloom

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// Filter represents a Bloom filter backed by a byte slice bitset.
type Filter struct {
	bits        []byte
	numBits     uint64
	numHashes   uint32
}

// NewFilter creates a Bloom filter optimized for n expected keys with the specified
// target false positive rate (e.g. 0.01 for 1%).
func NewFilter(expectedKeys int, falsePositiveRate float64) *Filter {
	if expectedKeys <= 0 {
		expectedKeys = 1
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}

	// m = - (n * ln(p)) / (ln(2)^2)
	n := float64(expectedKeys)
	m := - (n * math.Log(falsePositiveRate)) / (math.Ln2 * math.Ln2)
	numBits := uint64(math.Ceil(m))
	if numBits < 64 {
		numBits = 64
	}

	// k = (m / n) * ln(2)
	k := uint32(math.Round((float64(numBits) / n) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	numBytes := (numBits + 7) / 8
	return &Filter{
		bits:      make([]byte, numBytes),
		numBits:   numBits,
		numHashes: k,
	}
}

// Add inserts a key into the Bloom filter.
func (f *Filter) Add(key []byte) {
	h1, h2 := hashPair(key)
	for i := uint32(0); i < f.numHashes; i++ {
		// Kirsch-Mitzenmacher: gi(x) = h1(x) + i * h2(x)
		combined := (uint64(h1) + uint64(i)*uint64(h2)) % f.numBits
		byteIndex := combined / 8
		bitIndex := combined % 8
		f.bits[byteIndex] |= 1 << bitIndex
	}
}

// MayContain tests whether a key might be in the set.
// If false: the key is guaranteed NOT in the set (0% false negatives).
// If true: the key MIGHT be in the set (with target false positive probability).
func (f *Filter) MayContain(key []byte) bool {
	h1, h2 := hashPair(key)
	for i := uint32(0); i < f.numHashes; i++ {
		combined := (uint64(h1) + uint64(i)*uint64(h2)) % f.numBits
		byteIndex := combined / 8
		bitIndex := combined % 8
		if (f.bits[byteIndex] & (1 << bitIndex)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the Bloom filter to bytes:
// [NumBits (8B)][NumHashes (4B)][Bitset Bytes (N B)]
func (f *Filter) Encode() []byte {
	buf := make([]byte, 12+len(f.bits))
	binary.BigEndian.PutUint64(buf[0:8], f.numBits)
	binary.BigEndian.PutUint32(buf[8:12], f.numHashes)
	copy(buf[12:], f.bits)
	return buf
}

// DecodeFilter reconstructs a Bloom filter from serialized bytes.
func DecodeFilter(data []byte) (*Filter, error) {
	if len(data) < 12 {
		return nil, ErrInvalidBloomData
	}
	numBits := binary.BigEndian.Uint64(data[0:8])
	numHashes := binary.BigEndian.Uint32(data[8:12])
	bits := make([]byte, len(data)-12)
	copy(bits, data[12:])

	return &Filter{
		bits:      bits,
		numBits:   numBits,
		numHashes: numHashes,
	}, nil
}

// ErrInvalidBloomData is returned when decoding corrupted Bloom filter bytes.
var ErrInvalidBloomData = bloomError("invalid bloom filter byte slice")

type bloomError string

func (e bloomError) Error() string { return string(e) }

// hashPair produces two independent 32-bit hashes using FNV-1a with distinct seeds.
func hashPair(data []byte) (uint32, uint32) {
	h1 := fnv.New32a()
	h1.Write(data)
	sum1 := h1.Sum32()

	// Alter input with offset to get independent hash
	h2 := fnv.New32()
	h2.Write(data)
	sum2 := h2.Sum32()
	if sum2 == 0 {
		sum2 = 1
	}

	return sum1, sum2
}

