package client

import (
	"errors"
	"fmt"
	"sync"

	"github.com/distributed-kv/engine/pkg/cdc"
	"github.com/distributed-kv/engine/pkg/rpc"
	"github.com/distributed-kv/engine/pkg/sharding"
)

var (
	// ErrNoLeaderFound indicates no active leader could be located for a shard.
	ErrNoLeaderFound = errors.New("client: failed to find active leader for shard")
)

// SmartClient implements intelligent client-side routing, maintaining an in-memory
// cache of the consistent hash ring and per-shard Raft leaders to eliminate routing hops.
type SmartClient struct {
	mu           sync.RWMutex
	ring         *sharding.HashRing
	shardLeaders map[string]string          // shardID -> leader nodeID
	shardNodes   map[string][]string        // shardID -> list of replica nodeIDs
	nodeStubs    map[string]*rpc.ShardServer // nodeID -> direct server reference
}

// Config parameters for SmartClient.
type Config struct {
	VNodes int
}

// NewSmartClient initializes a new SmartClient.
func NewSmartClient(cfg Config) *SmartClient {
	vnodes := cfg.VNodes
	if vnodes <= 0 {
		vnodes = 128
	}
	return &SmartClient{
		ring:         sharding.NewHashRing(vnodes),
		shardLeaders: make(map[string]string),
		shardNodes:   make(map[string][]string),
		nodeStubs:    make(map[string]*rpc.ShardServer),
	}
}

// RegisterNode connects a server node to the client's topology.
func (c *SmartClient) RegisterNode(shardID string, server *rpc.ShardServer) {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeID := server.NodeID()
	c.nodeStubs[nodeID] = server

	// Add shard to hash ring if not already present
	_ = c.ring.AddNode(shardID)

	c.shardNodes[shardID] = append(c.shardNodes[shardID], nodeID)
}

// SetShardLeader manually sets or caches the active leader for a shard.
func (c *SmartClient) SetShardLeader(shardID, leaderNodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shardLeaders[shardID] = leaderNodeID
}

// Put routes a key-value write directly to the Raft leader of the owning shard.
func (c *SmartClient) Put(key, value []byte) error {
	shardID, err := c.ring.GetNode(key)
	if err != nil {
		return fmt.Errorf("routing failed: %w", err)
	}

	leaderStub, err := c.findOrDiscoverLeader(shardID)
	if err != nil {
		return err
	}

	// Attempt point-to-point dispatch directly to leader
	resp, err := leaderStub.Put(&rpc.PutRequest{Key: key, Value: value})
	if err == nil && resp.Success {
		return nil // Happy path: 0 routing hops!
	}

	// Leader might have stepped down: invalidate cache, rediscover, and retry
	c.invalidateLeader(shardID)
	newLeaderStub, err := c.discoverLeader(shardID)
	if err != nil {
		return fmt.Errorf("leader rediscovery failed: %w", err)
	}

	resp, err = newLeaderStub.Put(&rpc.PutRequest{Key: key, Value: value})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("put rejected by leader %s: %s", newLeaderStub.NodeID(), resp.Error)
	}

	return nil
}

// Get routes a read query directly to the owning shard.
func (c *SmartClient) Get(key []byte) ([]byte, bool, error) {
	shardID, err := c.ring.GetNode(key)
	if err != nil {
		return nil, false, fmt.Errorf("routing failed: %w", err)
	}

	leaderStub, err := c.findOrDiscoverLeader(shardID)
	if err != nil {
		return nil, false, err
	}

	resp, err := leaderStub.Get(&rpc.GetRequest{Key: key})
	if err != nil {
		return nil, false, err
	}

	return resp.Value, resp.Found, nil
}

// Delete routes a deletion directly to the Raft leader of the owning shard.
func (c *SmartClient) Delete(key []byte) error {
	shardID, err := c.ring.GetNode(key)
	if err != nil {
		return fmt.Errorf("routing failed: %w", err)
	}

	leaderStub, err := c.findOrDiscoverLeader(shardID)
	if err != nil {
		return err
	}

	resp, err := leaderStub.Delete(&rpc.DeleteRequest{Key: key})
	if err == nil && resp.Success {
		return nil
	}

	c.invalidateLeader(shardID)
	newLeaderStub, err := c.discoverLeader(shardID)
	if err != nil {
		return err
	}

	resp, err = newLeaderStub.Delete(&rpc.DeleteRequest{Key: key})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("delete rejected: %s", resp.Error)
	}

	return nil
}

// SubscribeCDC opens a change event stream from the specified shard.
func (c *SmartClient) SubscribeCDC(shardID string, fromSeq uint64) (*cdc.Subscription, error) {
	leaderStub, err := c.findOrDiscoverLeader(shardID)
	if err != nil {
		return nil, err
	}

	return leaderStub.SubscribeCDC(&rpc.CDCSubscriptionRequest{FromSequence: fromSeq})
}

func (c *SmartClient) findOrDiscoverLeader(shardID string) (*rpc.ShardServer, error) {
	c.mu.RLock()
	leaderID, exists := c.shardLeaders[shardID]
	var stub *rpc.ShardServer
	if exists {
		stub = c.nodeStubs[leaderID]
	}
	c.mu.RUnlock()

	if stub != nil {
		return stub, nil
	}

	return c.discoverLeader(shardID)
}

func (c *SmartClient) discoverLeader(shardID string) (*rpc.ShardServer, error) {
	c.mu.RLock()
	nodes := c.shardNodes[shardID]
	c.mu.RUnlock()

	for _, nodeID := range nodes {
		c.mu.RLock()
		server := c.nodeStubs[nodeID]
		c.mu.RUnlock()

		if server != nil {
			// Probe if this node is leader
			resp, _ := server.Put(&rpc.PutRequest{Key: []byte("__ping__"), Value: nil})
			if resp != nil && (resp.Success || resp.Error != "node is not leader") {
				c.mu.Lock()
				c.shardLeaders[shardID] = nodeID
				c.mu.Unlock()
				return server, nil
			}
		}
	}

	// Fallback to first available node in shard
	if len(nodes) > 0 {
		c.mu.RLock()
		s := c.nodeStubs[nodes[0]]
		c.mu.RUnlock()
		return s, nil
	}

	return nil, ErrNoLeaderFound
}

func (c *SmartClient) invalidateLeader(shardID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.shardLeaders, shardID)
}

