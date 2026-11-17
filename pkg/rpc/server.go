package rpc

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/distributed-kv/engine/pkg/cdc"
	"github.com/distributed-kv/engine/pkg/consensus/raft"
	"github.com/distributed-kv/engine/pkg/mvcc"
	"github.com/distributed-kv/engine/pkg/storage"
)

// CommandAction identifies state machine mutations replicated through Raft.
type CommandAction string

const (
	ActionPut    CommandAction = "PUT"
	ActionDelete CommandAction = "DELETE"
)

// ReplicatedCommand is serialized and appended to the Raft consensus log.
type ReplicatedCommand struct {
	Action    CommandAction `json:"action"`
	Key       []byte        `json:"key"`
	Value     []byte        `json:"value,omitempty"`
	RequestID string        `json:"request_id"`
}

// ShardServer coordinates the LSM Storage Engine, MVCC layer, Raft consensus, and CDC streamer.
type ShardServer struct {
	mu          sync.RWMutex
	nodeID      string
	shardID     string
	engine      *storage.Engine
	mvcc        *mvcc.MVCC
	raftNode    *raft.Node
	cdc         *cdc.Broadcaster
	pendingReqs map[uint64]chan error // index -> completion notify channel
	applyCh     chan raft.ApplyMsg
	stopCh      chan struct{}
}

// ServerConfig configures a ShardServer.
type ServerConfig struct {
	NodeID   string
	ShardID  string
	Engine   *storage.Engine
	RaftNode *raft.Node
	ApplyCh  chan raft.ApplyMsg
	CDC      *cdc.Broadcaster
}

// NewShardServer initializes and starts a shard server node.
func NewShardServer(cfg ServerConfig) *ShardServer {
	if cfg.CDC == nil {
		cfg.CDC = cdc.NewBroadcaster(10000)
	}

	s := &ShardServer{
		nodeID:      cfg.NodeID,
		shardID:     cfg.ShardID,
		engine:      cfg.Engine,
		mvcc:        mvcc.NewMVCC(cfg.Engine),
		raftNode:    cfg.RaftNode,
		cdc:         cfg.CDC,
		pendingReqs: make(map[uint64]chan error),
		applyCh:     cfg.ApplyCh,
		stopCh:      make(chan struct{}),
	}

	go s.applyLoop()
	return s
}

// applyLoop consumes committed Raft log entries and executes them on the LSM/MVCC engine.
func (s *ShardServer) applyLoop() {
	for {
		select {
		case <-s.stopCh:
			return
		case msg, ok := <-s.applyCh:
			if !ok {
				return
			}
			if !msg.CommandValid || len(msg.Command) == 0 {
				continue
			}

			var cmd ReplicatedCommand
			if err := json.Unmarshal(msg.Command, &cmd); err != nil {
				continue
			}

			// Apply to MVCC / LSM engine with monotonic commit timestamp
			commitTS := s.mvcc.Oracle().NextTimestamp()
			var applyErr error

			if cmd.Action == ActionPut {
				applyErr = s.mvcc.Put(cmd.Key, cmd.Value, commitTS)
				if applyErr == nil {
					s.cdc.Publish(cdc.EventPut, cmd.Key, cmd.Value)
				}
			} else if cmd.Action == ActionDelete {
				applyErr = s.mvcc.Delete(cmd.Key, commitTS)
				if applyErr == nil {
					s.cdc.Publish(cdc.EventDelete, cmd.Key, nil)
				}
			}

			// Notify waiting propose caller if this node was the leader proposing
			s.mu.Lock()
			if notifyCh, exists := s.pendingReqs[msg.CommandIndex]; exists {
				notifyCh <- applyErr
				delete(s.pendingReqs, msg.CommandIndex)
			}
			s.mu.Unlock()
		}
	}
}

// Put submits a key write via Raft consensus.
func (s *ShardServer) Put(req *PutRequest) (*PutResponse, error) {
	_, isLeader := s.raftNode.State()
	if !isLeader {
		return &PutResponse{
			Success:    false,
			Error:      "node is not leader",
			LeaderHint: "", // Could be populated with known leader ID
		}, nil
	}

	cmd := ReplicatedCommand{
		Action: ActionPut,
		Key:    req.Key,
		Value:  req.Value,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	idx, _, isL := s.raftNode.Propose(cmdBytes)
	if !isL {
		return &PutResponse{
			Success: false,
			Error:   "lost leadership before proposal",
		}, nil
	}

	// Wait for replication across quorum
	notifyCh := make(chan error, 1)
	s.mu.Lock()
	s.pendingReqs[idx] = notifyCh
	s.mu.Unlock()

	select {
	case err := <-notifyCh:
		if err != nil {
			return &PutResponse{Success: false, Error: err.Error()}, nil
		}
		return &PutResponse{Success: true}, nil
	case <-time.After(3 * time.Second):
		s.mu.Lock()
		delete(s.pendingReqs, idx)
		s.mu.Unlock()
		return &PutResponse{Success: false, Error: "replication timeout"}, nil
	}
}

// Get executes a read query against the local MVCC snapshot.
func (s *ShardServer) Get(req *GetRequest) (*GetResponse, error) {
	readTS := req.ReadTimestamp
	if readTS == 0 {
		readTS = s.mvcc.Oracle().CurrentTimestamp()
	}

	val, err := s.mvcc.Get(req.Key, readTS)
	if err != nil {
		if err == mvcc.ErrKeyNotFound || err == storage.ErrKeyNotFound {
			return &GetResponse{Found: false}, nil
		}
		return &GetResponse{Found: false, Error: err.Error()}, nil
	}

	return &GetResponse{Found: true, Value: val}, nil
}

// Delete submits a key deletion via Raft consensus.
func (s *ShardServer) Delete(req *DeleteRequest) (*DeleteResponse, error) {
	_, isLeader := s.raftNode.State()
	if !isLeader {
		return &DeleteResponse{
			Success: false,
			Error:   "node is not leader",
		}, nil
	}

	cmd := ReplicatedCommand{
		Action: ActionDelete,
		Key:    req.Key,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	idx, _, isL := s.raftNode.Propose(cmdBytes)
	if !isL {
		return &DeleteResponse{Success: false, Error: "lost leadership"}, nil
	}

	notifyCh := make(chan error, 1)
	s.mu.Lock()
	s.pendingReqs[idx] = notifyCh
	s.mu.Unlock()

	select {
	case err := <-notifyCh:
		if err != nil {
			return &DeleteResponse{Success: false, Error: err.Error()}, nil
		}
		return &DeleteResponse{Success: true}, nil
	case <-time.After(3 * time.Second):
		s.mu.Lock()
		delete(s.pendingReqs, idx)
		s.mu.Unlock()
		return &DeleteResponse{Success: false, Error: "replication timeout"}, nil
	}
}

// SubscribeCDC opens a real-time event stream.
func (s *ShardServer) SubscribeCDC(req *CDCSubscriptionRequest) (*cdc.Subscription, error) {
	return s.cdc.Subscribe(req.FromSequence), nil
}

// NodeID returns the identifier of the node.
func (s *ShardServer) NodeID() string {
	return s.nodeID
}

// ShardID returns the shard identifier.
func (s *ShardServer) ShardID() string {
	return s.shardID
}

// Stop halts the server and background goroutines.
func (s *ShardServer) Stop() {
	close(s.stopCh)
}

// Format placeholder
var _ = fmt.Sprintf

