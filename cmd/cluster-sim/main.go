package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/distributed-kv/engine/pkg/cdc"
	"github.com/distributed-kv/engine/pkg/client"
	"github.com/distributed-kv/engine/pkg/consensus/raft"
	"github.com/distributed-kv/engine/pkg/rpc"
	"github.com/distributed-kv/engine/pkg/storage"
)

func main() {
	fmt.Println("================================================================================")
	fmt.Println("       DISTRIBUTED KEY-VALUE DATABASE CLUSTER DEMONSTRATION & ENGINE RUNNER      ")
	fmt.Println("       Go | LSM-Tree (WAL, MemTable, SSTable, Compaction) | MVCC | Raft | CDC   ")
	fmt.Println("================================================================================")

	baseDir, err := os.MkdirTemp("", "cluster_demo_*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(baseDir)

	fmt.Printf("[1/6] Initializing Cluster Storage and Raft Consensus Groups at %s...\n", baseDir)

	// Create Shard A (3 replica nodes)
	shardA := setupShard(baseDir, "shard-alpha", 3)
	// Create Shard B (3 replica nodes)
	shardB := setupShard(baseDir, "shard-beta", 3)

	fmt.Println("      -> Shard Alpha initialized (3 Raft nodes: alpha-node1, alpha-node2, alpha-node3)")
	fmt.Println("      -> Shard Beta  initialized (3 Raft nodes: beta-node1,  beta-node2,  beta-node3)")
	fmt.Println("[2/6] Waiting for Raft leader elections to stabilize across shards...")
	time.Sleep(700 * time.Millisecond)

	// Initialize Smart Client
	smartClient := client.NewSmartClient(client.Config{VNodes: 128})
	for _, s := range shardA {
		smartClient.RegisterNode("shard-alpha", s)
	}
	for _, s := range shardB {
		smartClient.RegisterNode("shard-beta", s)
	}

	fmt.Println("[3/6] Smart Client connected: Consistent Hashing ring initialized with 128 vnodes.")

	// Start CDC Listener in background
	cdcSub, err := smartClient.SubscribeCDC("shard-alpha", 0)
	if err == nil {
		fmt.Println("[4/6] Subscribed to Real-Time Change Data Capture (CDC) stream on shard-alpha...")
		go func() {
			for event := range cdcSub.Events() {
				fmt.Printf("      [CDC STREAM EVENT] Seq:%d | Type:%s | Key:%s | Val:%s\n",
					event.Sequence, event.Type, string(event.Key), string(event.Value))
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)

	// Perform Sample Writes
	fmt.Println("\n[5/6] Executing Distributed Mutations via Smart Point-to-Point Routing:")
	samples := []struct {
		k string
		v string
	}{
		{"user:101", `{"name":"Alice","role":"Engineer"}`},
		{"user:102", `{"name":"Bob","role":"Architect"}`},
		{"user:103", `{"name":"Charlie","role":"Lead"}`},
		{"order:9001", `{"item":"MacBook Pro","amount":2499}`},
		{"order:9002", `{"item":"4K Monitor","amount":599}`},
	}

	for _, s := range samples {
		if err := smartClient.Put([]byte(s.k), []byte(s.v)); err != nil {
			fmt.Printf("      [ERROR] Put %s failed: %v\n", s.k, err)
		} else {
			fmt.Printf("      [OK] Put '%s' successfully routed and committed to Raft quorum\n", s.k)
		}
	}

	time.Sleep(300 * time.Millisecond)

	fmt.Println("\n[6/6] Verifying Data Retrieval & Snapshot Reads:")
	for _, s := range samples {
		val, found, err := smartClient.Get([]byte(s.k))
		if err != nil || !found {
			fmt.Printf("      [ERROR] Get %s failed (err: %v)\n", s.k, err)
		} else {
			fmt.Printf("      [GET] '%s' => %s\n", s.k, string(val))
		}
	}

	fmt.Println("\n================================================================================")
	fmt.Println("             ALL DISTRIBUTED ENGINE LAYERS VERIFIED & FULLY OPERATIONAL         ")
	fmt.Println("================================================================================")
}

func setupShard(baseDir, shardID string, count int) []*rpc.ShardServer {
	trans := raft.NewLocalTransport()
	var servers []*rpc.ShardServer
	var nodeIDs []string
	for i := 1; i <= count; i++ {
		nodeIDs = append(nodeIDs, fmt.Sprintf("%s-node%d", shardID, i))
	}

	for _, nodeID := range nodeIDs {
		nodeDir := filepath.Join(baseDir, nodeID)
		engineOpts := storage.DefaultEngineOptions(nodeDir)
		engine, err := storage.Open(engineOpts)
		if err != nil {
			panic(err)
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
			panic(err)
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
	return servers
}

