package raft

import (
	"errors"
	"sync"
)

var (
	// ErrNodeUnreachable is returned when an RPC target cannot be contacted.
	ErrNodeUnreachable = errors.New("raft: node unreachable or partitioned")
)

// Transport abstracts message passing between cluster nodes.
type Transport interface {
	SendRequestVote(target string, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(target string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// LocalTransport provides an in-memory channel-based transport for cluster testing.
// It allows simulating network partitions and node drops deterministically.
type LocalTransport struct {
	mu          sync.RWMutex
	handlers    map[string]RPCHandler
	partitioned map[string]bool // target -> whether it is cut off by partition
}

// RPCHandler is implemented by Raft nodes to accept incoming RPC calls.
type RPCHandler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
}

// NewLocalTransport creates a local network simulation.
func NewLocalTransport() *LocalTransport {
	return &LocalTransport{
		handlers:    make(map[string]RPCHandler),
		partitioned: make(map[string]bool),
	}
}

// Register attaches a node handler to the local network.
func (t *LocalTransport) Register(id string, handler RPCHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[id] = handler
}

// SetPartition isolates or reconnects a node in the simulated network.
func (t *LocalTransport) SetPartition(id string, isolated bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.partitioned[id] = isolated
}

func (t *LocalTransport) SendRequestVote(target string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.partitioned[target] || t.partitioned[args.CandidateID] {
		return nil, ErrNodeUnreachable
	}

	handler, exists := t.handlers[target]
	if !exists {
		return nil, ErrNodeUnreachable
	}

	return handler.HandleRequestVote(args), nil
}

func (t *LocalTransport) SendAppendEntries(target string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.partitioned[target] || t.partitioned[args.LeaderID] {
		return nil, ErrNodeUnreachable
	}

	handler, exists := t.handlers[target]
	if !exists {
		return nil, ErrNodeUnreachable
	}

	return handler.HandleAppendEntries(args), nil
}

