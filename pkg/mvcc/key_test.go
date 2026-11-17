package mvcc

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeKey(t *testing.T) {
	testCases := []struct {
		userKey []byte
		ts      uint64
	}{
		{[]byte("simple_key"), 100},
		{[]byte("key_with_\x00_null_bytes_\x00\x00"), 99999999},
		{[]byte(""), 1},
		{[]byte("unicode_🔥_rocks"), 42},
	}

	for _, tc := range testCases {
		encoded := EncodeKey(tc.userKey, tc.ts)
		decodedKey, decodedTS, err := DecodeKey(encoded)
		if err != nil {
			t.Fatalf("failed to decode key %v: %v", tc.userKey, err)
		}
		if !bytes.Equal(decodedKey, tc.userKey) {
			t.Fatalf("key mismatch: got %v, expected %v", decodedKey, tc.userKey)
		}
		if decodedTS != tc.ts {
			t.Fatalf("ts mismatch: got %d, expected %d", decodedTS, tc.ts)
		}
	}
}

func TestKeyOrdering(t *testing.T) {
	// Invariant 1: For the same key, higher timestamp sorts BEFORE lower timestamp
	kOld := EncodeKey([]byte("user:1"), 100)
	kNew := EncodeKey([]byte("user:1"), 200)
	if bytes.Compare(kNew, kOld) >= 0 {
		t.Fatalf("expected newer version (ts=200) to sort before older version (ts=100)")
	}

	// Invariant 2: Different keys sort in alphabetical order
	kApple := EncodeKey([]byte("apple"), 500)
	kBanana := EncodeKey([]byte("banana"), 100)
	if bytes.Compare(kApple, kBanana) >= 0 {
		t.Fatalf("expected 'apple' to sort before 'banana'")
	}

	// Invariant 3: Prefix keys sort correctly
	kCat := EncodeKey([]byte("cat"), 1000)
	kCaterpillar := EncodeKey([]byte("caterpillar"), 50)
	if bytes.Compare(kCat, kCaterpillar) >= 0 {
		t.Fatalf("expected 'cat' to sort before 'caterpillar'")
	}
}

