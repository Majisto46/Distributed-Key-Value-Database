package raft

import (
	"fmt"
)

// Role defines the current consensus state of a Raft node.
type Role string

const (
	RoleFollower  Role = "Follower"
	RoleCandidate Role = "Candidate"
	RoleLeader    Role = "Leader"
)

// LogEntry represents an individual replicated command in the Raft log.
type LogEntry struct {
	Index   uint64 // 1-based index in the log
	Term    uint64 // Election term when entry was received by leader
	Command []byte // Command payload to be executed on the state machine
}

// RequestVoteArgs is sent by candidates to gather votes during elections.
type RequestVoteArgs struct {
	Term         uint64 // Candidate's term
	CandidateID  string // Candidate requesting vote
	LastLogIndex uint64 // Index of candidate's last log entry
	LastLogTerm  uint64 // Term of candidate's last log entry
}

// RequestVoteReply is returned by voters.
type RequestVoteReply struct {
	Term        uint64 // CurrentTerm of voter, for candidate to update itself
	VoteGranted bool   // True means candidate received vote
}

// AppendEntriesArgs is used for log replication and heartbeats.
type AppendEntriesArgs struct {
	Term         uint64     // Leader's term
	LeaderID     string     // Leader's identifier
	PrevLogIndex uint64     // Index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // Term of PrevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit uint64     // Leader's commitIndex
}

// AppendEntriesReply is returned by followers.
type AppendEntriesReply struct {
	Term          uint64 // CurrentTerm of receiver, for leader to update itself
	Success       bool   // True if follower contained entry matching PrevLogIndex and PrevLogTerm
	ConflictIndex uint64 // Fast recovery optimization: first index of conflicting term
	ConflictTerm  uint64 // Fast recovery optimization: term of conflicting entry
}

// ApplyMsg is sent on the apply channel when a log entry is committed.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
}

// StateMachine is implemented by the underlying storage engine to execute committed commands.
type StateMachine interface {
	Apply(command []byte) ([]byte, error)
}

func (r Role) String() string {
	return string(r)
}

func (e LogEntry) String() string {
	return fmt.Sprintf("[Idx:%d Term:%d CmdLen:%d]", e.Index, e.Term, len(e.Command))
}

