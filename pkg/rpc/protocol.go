package rpc

import (
	"errors"

	"github.com/distributed-kv/engine/pkg/cdc"
)

var (
	// ErrNotLeader is returned when a write request is routed to a follower node.
	ErrNotLeader = errors.New("rpc: node is not leader for this shard")
	// ErrKeyNotFound indicates key does not exist.
	ErrKeyNotFound = errors.New("rpc: key not found")
)

// PutRequest encapsulates a write mutation.
type PutRequest struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// PutResponse indicates write outcome.
type PutResponse struct {
	Success    bool   `json:"success"`
	Error      string `json:"error,omitempty"`
	LeaderHint string `json:"leader_hint,omitempty"` // Redirect address if receiver is not leader
}

// GetRequest encapsulates a point-in-time read.
type GetRequest struct {
	Key           []byte `json:"key"`
	ReadTimestamp uint64 `json:"read_timestamp,omitempty"` // 0 for latest
}

// GetResponse returns read outcome.
type GetResponse struct {
	Found bool   `json:"found"`
	Value []byte `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}

// DeleteRequest encapsulates a key deletion.
type DeleteRequest struct {
	Key []byte `json:"key"`
}

// DeleteResponse indicates deletion outcome.
type DeleteResponse struct {
	Success    bool   `json:"success"`
	Error      string `json:"error,omitempty"`
	LeaderHint string `json:"leader_hint,omitempty"`
}

// ShardInfo details ownership and current leader for a shard.
type ShardInfo struct {
	ShardID  string   `json:"shard_id"`
	LeaderID string   `json:"leader_id"`
	Replicas []string `json:"replicas"`
}

// TopologyResponse describes the entire cluster layout for smart client routing.
type TopologyResponse struct {
	Shards []ShardInfo `json:"shards"`
}

// CDCSubscriptionRequest opens a change event stream.
type CDCSubscriptionRequest struct {
	FromSequence uint64 `json:"from_sequence"`
}

// DBService defines the complete database RPC contract.
type DBService interface {
	Put(req *PutRequest) (*PutResponse, error)
	Get(req *GetRequest) (*GetResponse, error)
	Delete(req *DeleteRequest) (*DeleteResponse, error)
	GetTopology() (*TopologyResponse, error)
	SubscribeCDC(req *CDCSubscriptionRequest) (*cdc.Subscription, error)
}

