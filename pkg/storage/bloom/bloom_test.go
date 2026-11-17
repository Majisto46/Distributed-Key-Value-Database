package bloom

import (
	"fmt"
	"testing"
)

func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	n := 1000
	f := NewFilter(n, 0.01)

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key_%d", i)
		f.Add([]byte(key))
	}

	// 0% false negatives: every added key must be found
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key_%d", i)
		if !f.MayContain([]byte(key)) {
			t.Fatalf("false negative detected for %s", key)
		}
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	n := 5000
	targetFPR := 0.01
	f := NewFilter(n, targetFPR)

	for i := 0; i < n; i++ {
		f.Add([]byte(fmt.Sprintf("present_%d", i)))
	}

	falsePositives := 0
	trials := 10000
	for i := 0; i < trials; i++ {
		absentKey := fmt.Sprintf("absent_%d", i)
		if f.MayContain([]byte(absentKey)) {
			falsePositives++
		}
	}

	actualRate := float64(falsePositives) / float64(trials)
	t.Logf("Measured false positive rate: %.4f (target: %.4f)", actualRate, targetFPR)

	// Allow reasonable statistical variance (e.g. within 3x of target)
	if actualRate > 0.03 {
		t.Fatalf("false positive rate too high: got %.4f, target was %.4f", actualRate, targetFPR)
	}
}

func TestBloomFilter_Serialization(t *testing.T) {
	f := NewFilter(100, 0.01)
	f.Add([]byte("hello"))
	f.Add([]byte("world"))

	encoded := f.Encode()
	decoded, err := DecodeFilter(encoded)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if !decoded.MayContain([]byte("hello")) {
		t.Fatal("decoded filter missing 'hello'")
	}
	if !decoded.MayContain([]byte("world")) {
		t.Fatal("decoded filter missing 'world'")
	}
	if decoded.MayContain([]byte("non_existent_random_key_12345")) {
		t.Log("Note: probabilistic false positive (acceptable)")
	}
}

