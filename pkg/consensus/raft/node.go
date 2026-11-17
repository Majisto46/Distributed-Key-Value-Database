package raft

import (
	"math/rand"
	"sync"
	"time"
)

const (
	minElectionTimeout = 150 * time.Millisecond
	maxElectionTimeout = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

// Node represents a single participant in the Raft consensus cluster.
type Node struct {
	mu        sync.Mutex
	id        string
	peers     []string
	transport Transport
	storage   Storage
	applyCh   chan ApplyMsg

	role        Role
	currentTerm uint64
	votedFor    string
	log         []LogEntry // 1-indexed; log[0] is dummy entry

	commitIndex uint64
	lastApplied uint64

	// Leader state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Timers
	lastContact     time.Time
	electionTimeout time.Duration
	stopCh          chan struct{}
	random          *rand.Rand
}

// Config parameters for creating a Raft node.
type Config struct {
	ID        string
	Peers     []string
	Transport Transport
	Storage   Storage
	ApplyCh   chan ApplyMsg
}

// NewNode initializes and starts a Raft node.
func NewNode(cfg Config) (*Node, error) {
	if cfg.Storage == nil {
		cfg.Storage = NewMemoryStorage()
	}

	n := &Node{
		id:         cfg.ID,
		peers:      cfg.Peers,
		transport:  cfg.Transport,
		storage:    cfg.Storage,
		applyCh:    cfg.ApplyCh,
		role:       RoleFollower,
		log:        []LogEntry{{Index: 0, Term: 0}}, // Dummy entry at index 0
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
		stopCh:     make(chan struct{}),
		random:     rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(cfg.ID)))),
	}

	// Restore persistent state if available
	savedTerm, savedVotedFor, savedEntries, err := cfg.Storage.LoadState()
	if err == nil && len(savedEntries) > 0 {
		n.currentTerm = savedTerm
		n.votedFor = savedVotedFor
		n.log = savedEntries
	}

	n.resetElectionTimeout()
	go n.runLoop()

	return n, nil
}

// ID returns the unique identifier of the node.
func (n *Node) ID() string {
	return n.id
}

// State returns the current term and whether this node is the cluster leader.
func (n *Node) State() (uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role == RoleLeader
}

// Role returns the current consensus role.
func (n *Node) CurrentRole() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Propose submits a new command to the Raft cluster.
// Returns the index assigned to the entry, the current term, and true if this node is leader.
func (n *Node) Propose(command []byte) (uint64, uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != RoleLeader {
		return 0, n.currentTerm, false
	}

	index := uint64(len(n.log))
	term := n.currentTerm
	entry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}

	n.log = append(n.log, entry)
	n.persist()

	n.matchIndex[n.id] = index
	n.nextIndex[n.id] = index + 1

	// Broadcast immediately to minimize latency
	go n.broadcastAppendEntries()

	return index, term, true
}

// Stop halts the background goroutines of the node.
func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) resetElectionTimeout() {
	jitter := time.Duration(n.random.Int63n(int64(maxElectionTimeout - minElectionTimeout)))
	n.electionTimeout = minElectionTimeout + jitter
	n.lastContact = time.Now()
}

func (n *Node) persist() {
	_ = n.storage.SaveState(n.currentTerm, n.votedFor, n.log)
}

func (n *Node) runLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := time.Now()
	switch n.role {
	case RoleFollower, RoleCandidate:
		if now.Sub(n.lastContact) >= n.electionTimeout {
			n.startElection()
		}
	case RoleLeader:
		if now.Sub(n.lastContact) >= heartbeatInterval {
			n.lastContact = now
			go n.broadcastAppendEntries()
		}
	}
}

func (n *Node) startElection() {
	n.role = RoleCandidate
	n.currentTerm++
	n.votedFor = n.id
	n.persist()
	n.resetElectionTimeout()

	term := n.currentTerm
	lastLog := n.lastLogEntry()

	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	votesReceived := 1 // Vote for self
	totalNodes := len(n.peers) + 1
	var voteMu sync.Mutex

	for _, peer := range n.peers {
		go func(target string) {
			reply, err := n.transport.SendRequestVote(target, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.stepDown(reply.Term)
				return
			}

			if n.role != RoleCandidate || n.currentTerm != term {
				return
			}

			if reply.VoteGranted {
				voteMu.Lock()
				votesReceived++
				hasMajority := votesReceived > totalNodes/2
				voteMu.Unlock()

				if hasMajority && n.role == RoleCandidate {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

func (n *Node) becomeLeader() {
	n.role = RoleLeader
	lastIndex := n.lastLogEntry().Index

	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}
	n.matchIndex[n.id] = lastIndex
	n.lastContact = time.Now()

	go n.broadcastAppendEntries()
}

func (n *Node) stepDown(term uint64) {
	n.role = RoleFollower
	n.currentTerm = term
	n.votedFor = ""
	n.persist()
	n.resetElectionTimeout()
}

func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}

	term := n.currentTerm
	leaderID := n.id
	leaderCommit := n.commitIndex

	for _, peer := range n.peers {
		nextIdx := n.nextIndex[peer]
		if nextIdx == 0 {
			nextIdx = 1
		}

		prevLogIndex := nextIdx - 1
		var prevLogTerm uint64
		if prevLogIndex < uint64(len(n.log)) {
			prevLogTerm = n.log[prevLogIndex].Term
		}

		var entries []LogEntry
		if nextIdx < uint64(len(n.log)) {
			entries = make([]LogEntry, len(n.log[nextIdx:]))
			copy(entries, n.log[nextIdx:])
		}

		args := &AppendEntriesArgs{
			Term:         term,
			LeaderID:     leaderID,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}

		go n.sendAppendEntriesToPeer(peer, args, term)
	}

	n.mu.Unlock()
}

func (n *Node) sendAppendEntriesToPeer(peer string, args *AppendEntriesArgs, term uint64) {
	reply, err := n.transport.SendAppendEntries(peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
		return
	}

	if n.role != RoleLeader || n.currentTerm != term {
		return
	}

	if reply.Success {
		// Log replicated successfully on peer
		match := args.PrevLogIndex + uint64(len(args.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
			n.nextIndex[peer] = match + 1
		}
		n.checkAndUpdateCommitIndex()
	} else {
		// Follower rejected due to log inconsistency -> decrement nextIndex
		if reply.ConflictIndex > 0 {
			n.nextIndex[peer] = reply.ConflictIndex
		} else if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

// checkAndUpdateCommitIndex advances commitIndex when a majority of matchIndex[peer] >= N
// and log[N].Term == currentTerm (Figure 8 safety rule).
func (n *Node) checkAndUpdateCommitIndex() {
	totalNodes := len(n.peers) + 1
	lastIndex := n.lastLogEntry().Index

	for N := lastIndex; N > n.commitIndex; N-- {
		if n.log[N].Term != n.currentTerm {
			continue // Leaders cannot directly commit log entries from previous terms
		}

		count := 1 // Count leader
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= N {
				count++
			}
		}

		if count > totalNodes/2 {
			n.commitIndex = N
			n.applyCommittedEntries()
			break
		}
	}
}

func (n *Node) applyCommittedEntries() {
	for n.commitIndex > n.lastApplied {
		n.lastApplied++
		entry := n.log[n.lastApplied]

		if n.applyCh != nil && len(entry.Command) > 0 {
			n.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
	}
}

func (n *Node) lastLogEntry() LogEntry {
	return n.log[len(n.log)-1]
}
