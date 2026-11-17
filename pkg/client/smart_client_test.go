package client

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/distributed-kv/engine/pkg/cdc"
	"github.com/distributed-kv/engine/pkg/consensus/raft"
	"github.com/distributed-kv/engine/pkg/rpc"
	"github.com/distributed-kv/engine/pkg/storage"
)

type testShardCluster struct {
	shardID   string
	nodes     []*rpc.ShardServer
	transport *raft.LocalTransport
}

func createTestShard(t *testing.T, baseDir, shardID string, nodeCount int) *testShardCluster {
	trans := raft.NewLocalTransport()
	var servers []*rpc.ShardServer

	var nodeIDs []string
	for i := 1; i <= nodeCount; i++ {
		nodeIDs = append(nodeIDs, fmt.Sprintf("%s-node%d", shardID, i))
	}

	for _, nodeID := range nodeIDs {
		nodeDir := filepath.Join(baseDir, nodeID)
		engineOpts := storage.DefaultEngineOptions(nodeDir)
		engine, err := storage.Open(engineOpts)
		if err != nil {
			t.Fatalf("failed to open storage engine for %s: %v", nodeID, err)
		}

		var peers []string
		for _, other := range nodeIDs {
			if other != nodeID {
				peers = append(peers, other)
			}
		}

		applyCh := make(chan raft.ApplyMsg, 100)
		raftNode, err := raft.NewNode(raft.Config{
			ID:        nodeID,
			Peers:     peers,
			Transport: trans,
			Storage:   raft.NewMemoryStorage(),
			ApplyCh:   applyCh,
		})
		if err != nil {
			t.Fatalf("failed to create raft node for %s: %v", nodeID, err)
		}
		trans.Register(nodeID, raftNode)

		server := rpc.NewShardServer(rpc.ServerConfig{
			NodeID:   nodeID,
			ShardID:  shardID,
			Engine:   engine,
			RaftNode: raftNode,
			ApplyCh:  applyCh,
			CDC:      cdc.NewBroadcaster(1000),
		})

		servers = append(servers, server)
	}

	return &testShardCluster{
		shardID:   shardID,
		nodes:     servers,
		transport: trans,
	}
}

func (sc *testShardCluster) Close() {
	for _, s := range sc.nodes {
		s.Stop()
	}
}

func TestEndToEnd_MultiShardCluster(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cluster_e2e_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create 2 independent Shard replica groups (each with 3 Raft nodes)
	shardA := createTestShard(t, tmpDir, "shard-A", 3)
	defer shardA.Close()

	shardB := createTestShard(t, tmpDir, "shard-B", 3)
	defer shardB.Close()

	// Wait 600ms for initial leader elections to settle in both shards
	time.Sleep(600 * time.Millisecond)

	// Initialize Smart Client
	smartClient := NewSmartClient(Config{VNodes: 128})

	for _, s := range shardA.nodes {
		smartClient.RegisterNode("shard-A", s)
	}
	for _, s := range shardB.nodes {
		smartClient.RegisterNode("shard-B", s)
	}

	// 1. Subscribe to CDC on shard-A
	cdcSub, err := smartClient.SubscribeCDC("shard-A", 0)
	if err != nil {
		t.Fatalf("failed to subscribe to CDC: %v", err)
	}
	defer cdcSub.Close()

	// 2. Write 30 keys through smart client
	// Client hashes each key and dispatches point-to-point to the appropriate shard leader!
	for i := 0; i < 30; i++ {
		k := fmt.Sprintf("customer:profile:%04d", i)
		v := fmt.Sprintf("data_payload_%04d", i)

		if err := smartClient.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put failed for %s: %v", k, err)
		}
	}

	// 3. Read back all 30 keys through smart client and verify data integrity
	for i := 0; i < 30; i++ {
		k := fmt.Sprintf("customer:profile:%04d", i)
		expectedVal := fmt.Sprintf("data_payload_%04d", i)

		val, found, err := smartClient.Get([]byte(k))
		if err != nil {
			t.Fatalf("get failed for %s: %v", k, err)
		}
		if !found {
			t.Fatalf("key %s was not found", k)
		}
		if string(val) != expectedVal {
			t.Fatalf("key %s: got %s, expected %s", k, string(val), expectedVal)
		}
	}

	// 4. Verify CDC stream emitted mutations for keys that routed to shard-A
	var cdcCount int
	timeout := time.After(2 * time.Second)
	for {
		select {
		case event := <-cdcSub.Events():
			cdcCount++
			if event.Type != cdc.EventPut {
				t.Errorf("expected EventPut, got %v", event.Type)
			}
		case <-timeout:
			goto cdcDone
		}
	}
cdcDone:
	t.Logf("CDC on shard-A received %d real-time mutation events", cdcCount)
	if cdcCount == 0 {
		t.Fatal("expected CDC to capture mutations routed to shard-A")
	}

	// 5. Test Key Deletion
	delKey := []byte("customer:profile:0005")
	if err := smartClient.Delete(delKey); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	_, found, err := smartClient.Get(delKey)
	if err != nil {
		t.Fatalf("get after delete returned error: %v", err)
	}
	if found {
		t.Fatal("expected deleted key to not be found")
	}
}

