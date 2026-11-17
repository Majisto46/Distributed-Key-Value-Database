package raft

// HandleRequestVote implements the RequestVote RPC receiver logic per Raft §5.2 & §5.4.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &RequestVoteReply{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// Rule 1: Reply false if candidate's term is older than our term
	if args.Term < n.currentTerm {
		return reply
	}

	// If candidate has higher term, step down and update term immediately
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}

	// Rule 2: Receiver votes if votedFor is null or candidateId, AND
	// candidate's log is at least as up-to-date as receiver's log (§5.4.1)
	canVote := (n.votedFor == "" || n.votedFor == args.CandidateID)

	lastEntry := n.lastLogEntry()
	candidateUpToDate := false
	if args.LastLogTerm > lastEntry.Term {
		candidateUpToDate = true
	} else if args.LastLogTerm == lastEntry.Term && args.LastLogIndex >= lastEntry.Index {
		candidateUpToDate = true
	}

	if canVote && candidateUpToDate {
		reply.VoteGranted = true
		n.votedFor = args.CandidateID
		n.persist()
		n.resetElectionTimeout() // Reset election timer when granting vote
	}

	reply.Term = n.currentTerm
	return reply
}

// HandleAppendEntries implements the AppendEntries RPC receiver logic per Raft §5.3.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{
		Term:    n.currentTerm,
		Success: false,
	}

	// Rule 1: Reply false if leader's term is older than our term
	if args.Term < n.currentTerm {
		return reply
	}

	// If leader has higher term, or if we were candidate in same term, recognize leader
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	} else if n.role == RoleCandidate {
		n.stepDown(args.Term)
	}

	// Valid heartbeat/append received from current leader: reset election timeout
	n.resetElectionTimeout()
	reply.Term = n.currentTerm

	// Rule 2: Reply false if log doesn't contain an entry at PrevLogIndex matching PrevLogTerm
	logLen := uint64(len(n.log))
	if args.PrevLogIndex >= logLen {
		// Follower's log is shorter than leader's PrevLogIndex
		reply.ConflictIndex = logLen
		reply.ConflictTerm = 0
		return reply
	}

	if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Term mismatch at PrevLogIndex: find first index with this conflicting term
		conflictTerm := n.log[args.PrevLogIndex].Term
		firstIdx := args.PrevLogIndex
		for firstIdx > 1 && n.log[firstIdx-1].Term == conflictTerm {
			firstIdx--
		}
		reply.ConflictIndex = firstIdx
		reply.ConflictTerm = conflictTerm
		return reply
	}

	// Rule 3 & 4: Append any new entries not already in the log.
	// If an existing entry conflicts with a new one, delete existing entry and all that follow it.
	insertIdx := args.PrevLogIndex + 1
	entryIdx := 0

	for ; entryIdx < len(args.Entries); entryIdx++ {
		currentLogIdx := insertIdx + uint64(entryIdx)
		if currentLogIdx < uint64(len(n.log)) {
			if n.log[currentLogIdx].Term != args.Entries[entryIdx].Term {
				// Conflict: truncate divergent suffix
				n.log = n.log[:currentLogIdx]
				break
			}
		} else {
			break
		}
	}

	// Append remaining new entries
	for ; entryIdx < len(args.Entries); entryIdx++ {
		n.log = append(n.log, args.Entries[entryIdx])
	}
	n.persist()

	// Rule 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > n.commitIndex {
		lastNewIndex := args.PrevLogIndex + uint64(len(args.Entries))
		if args.LeaderCommit < lastNewIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
		n.applyCommittedEntries()
	}

	reply.Success = true
	return reply
}

