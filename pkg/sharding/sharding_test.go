package sharding

import (
	"fmt"
	"math"
	"testing"
)

func TestHashRing_UniformDistribution(t *testing.T) {
	ring := NewHashRing(128)
	nodes := []string{"shard-1", "shard-2", "shard-3", "shard-4"}
	for _, n := range nodes {
		if err := ring.AddNode(n); err != nil {
			t.Fatal(err)
		}
	}

	const totalKeys = 10000
	counts := make(map[string]int)

	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("user_key_%08d", i))
		node, err := ring.GetNode(key)
		if err != nil {
			t.Fatalf("failed to route key: %v", err)
		}
		counts[node]++
	}

	expectedPerNode := totalKeys / len(nodes) // 2500 keys per node
	t.Logf("Key distribution across 4 shards (target ~%d per shard):", expectedPerNode)

	for _, n := range nodes {
		count := counts[n]
		pct := (float64(count) / float64(totalKeys)) * 100.0
		t.Logf("  %s: %d keys (%.2f%%)", n, count, pct)

		// Assert no shard has less than 18% or more than 32% (uniformity proof)
		if count < int(float64(expectedPerNode)*0.70) || count > int(float64(expectedPerNode)*1.30) {
			t.Fatalf("shard %s distribution skew too high: %d keys (%.2f%%)", n, count, pct)
		}
	}
}

func TestHashRing_MinimalDataMovementOnAddNode(t *testing.T) {
	oldRing := NewHashRing(128)
	newRing := NewHashRing(128)

	initialNodes := []string{"shard-A", "shard-B", "shard-C"}
	for _, n := range initialNodes {
		_ = oldRing.AddNode(n)
		_ = newRing.AddNode(n)
	}

	// Add 4th node only to newRing
	_ = newRing.AddNode("shard-D")

	const totalKeys = 6000
	var keys [][]byte
	for i := 0; i < totalKeys; i++ {
		keys = append(keys, []byte(fmt.Sprintf("account_%06d", i)))
	}

	report, err := CalculateRebalance(keys, oldRing, newRing)
	if err != nil {
		t.Fatalf("rebalance calculation failed: %v", err)
	}

	t.Logf("Scaling from 3 to 4 nodes:")
	t.Logf("  Total keys: %d", report.TotalKeys)
	t.Logf("  Moved keys: %d (%.2f%%)", report.MovedKeys, report.PercentMoved)
	t.Logf("  Keys received by new shard-D: %d", report.TargetNodeGain["shard-D"])

	// Theoretical optimal movement is 1/(3+1) = 25%
	// Consistent hashing with 128 vnodes should keep it between 18% and 32%
	if math.Abs(report.PercentMoved-25.0) > 8.0 {
		t.Fatalf("unexpected percentage moved: got %.2f%%, expected close to 25.0%%", report.PercentMoved)
	}

	// Verify that ALL moved keys went exclusively to the new shard-D!
	// (Existing shards should not be needlessly shuffling keys between each other!)
	if report.TargetNodeGain["shard-D"] != report.MovedKeys {
		t.Fatalf("all moved keys must go to shard-D: shard-D got %d, total moved %d",
			report.TargetNodeGain["shard-D"], report.MovedKeys)
	}
}

func TestHashRing_Replication(t *testing.T) {
	ring := NewHashRing(64)
	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	for _, n := range nodes {
		_ = ring.AddNode(n)
	}

	key := []byte("critical_record_key")
	replicas, err := ring.GetReplicas(key, 3)
	if err != nil {
		t.Fatal(err)
	}

	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}

	// Replicas must all be distinct nodes
	seen := make(map[string]bool)
	for _, r := range replicas {
		if seen[r] {
			t.Fatalf("duplicate node %s in replica set", r)
		}
		seen[r] = true
	}
}

