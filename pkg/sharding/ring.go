package sharding

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
	"strconv"
	"sync"
)

var (
	// ErrEmptyRing indicates an operation was performed on an unpopulated hash ring.
	ErrEmptyRing = errors.New("sharding: hash ring is empty")
	// ErrNodeExists indicates the node has already been added to the ring.
	ErrNodeExists = errors.New("sharding: node already present on ring")
	// ErrNodeNotFound indicates the node is not found on the ring.
	ErrNodeNotFound = errors.New("sharding: node not found on ring")
)

// HashFunc defines the signature for mapping byte slices to 64-bit ring tokens.
type HashFunc func(data []byte) uint64

// DefaultHashFunc uses SHA-256 for cryptographic avalanche properties and uniform token distribution.
func DefaultHashFunc(data []byte) uint64 {
	sum := sha256.Sum256(data)
	return binary.BigEndian.Uint64(sum[0:8])
}

// HashRing manages virtual nodes across a circular token continuum.
type HashRing struct {
	mu       sync.RWMutex
	vnodes   int               // Virtual nodes per physical node (e.g. 128)
	hashFunc HashFunc          // 64-bit hash function
	ring     []uint64          // Sorted list of token hashes
	nodeMap  map[uint64]string // Token hash -> physical node ID
	nodes    map[string]bool   // Set of registered physical nodes
}

// NewHashRing creates a new consistent hash ring.
func NewHashRing(vnodes int) *HashRing {
	if vnodes <= 0 {
		vnodes = 128
	}
	return &HashRing{
		vnodes:   vnodes,
		hashFunc: DefaultHashFunc,
		nodeMap:  make(map[uint64]string),
		nodes:    make(map[string]bool),
	}
}

// AddNode registers a physical node, distributing its virtual nodes across the ring.
func (h *HashRing) AddNode(nodeID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.nodes[nodeID] {
		return ErrNodeExists
	}

	h.nodes[nodeID] = true
	for i := 0; i < h.vnodes; i++ {
		vtoken := nodeID + "#" + strconv.Itoa(i)
		tokenHash := h.hashFunc([]byte(vtoken))

		h.ring = append(h.ring, tokenHash)
		h.nodeMap[tokenHash] = nodeID
	}

	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})

	return nil
}

// RemoveNode unregisters a physical node and removes all its virtual nodes.
func (h *HashRing) RemoveNode(nodeID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.nodes[nodeID] {
		return ErrNodeNotFound
	}

	delete(h.nodes, nodeID)

	var newRing []uint64
	for _, tokenHash := range h.ring {
		if h.nodeMap[tokenHash] == nodeID {
			delete(h.nodeMap, tokenHash)
		} else {
			newRing = append(newRing, tokenHash)
		}
	}
	h.ring = newRing

	return nil
}

// GetNode determines which physical node owns key.
func (h *HashRing) GetNode(key []byte) (string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return "", ErrEmptyRing
	}

	keyHash := h.hashFunc(key)

	// Binary search for first token >= keyHash
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= keyHash
	})

	// Wrap around to start of ring if keyHash exceeds all tokens
	if idx == len(h.ring) {
		idx = 0
	}

	return h.nodeMap[h.ring[idx]], nil
}

// GetReplicas returns the primary node plus distinct replica nodes for a key.
func (h *HashRing) GetReplicas(key []byte, count int) ([]string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return nil, ErrEmptyRing
	}

	if count > len(h.nodes) {
		count = len(h.nodes)
	}

	keyHash := h.hashFunc(key)
	startIdx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= keyHash
	})

	seen := make(map[string]bool)
	var replicas []string

	for i := 0; i < len(h.ring) && len(replicas) < count; i++ {
		curIdx := (startIdx + i) % len(h.ring)
		nodeID := h.nodeMap[h.ring[curIdx]]
		if !seen[nodeID] {
			seen[nodeID] = true
			replicas = append(replicas, nodeID)
		}
	}

	return replicas, nil
}

// NodeCount returns the number of active physical nodes on the ring.
func (h *HashRing) NodeCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.nodes)
}

// Nodes returns a list of all active physical node IDs.
func (h *HashRing) Nodes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var res []string
	for n := range h.nodes {
		res = append(res, n)
	}
	sort.Strings(res)
	return res
}
