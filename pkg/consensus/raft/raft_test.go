package raft

import (
	"testing"
	"time"
)

type cluster struct {
	transport *LocalTransport
	nodes     map[string]*Node
	applyChs  map[string]chan ApplyMsg
}

func newCluster(nodeIDs []string) *cluster {
	trans := NewLocalTransport()
	c := &cluster{
		transport: trans,
		nodes:     make(map[string]*Node),
		applyChs:  make(map[string]chan ApplyMsg),
	}

	for _, id := range nodeIDs {
		var peers []string
		for _, other := range nodeIDs {
			if other != id {
				peers = append(peers, other)
			}
		}

		applyCh := make(chan ApplyMsg, 100)
		c.applyChs[id] = applyCh

		node, err := NewNode(Config{
			ID:        id,
			Peers:     peers,
			Transport: trans,
			Storage:   NewMemoryStorage(),
			ApplyCh:   applyCh,
		})
		if err != nil {
			panic(err)
		}

		c.nodes[id] = node
		trans.Register(id, node)
	}

	return c
}

func (c *cluster) Stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
}

func (c *cluster) findLeader() *Node {
	for _, n := range c.nodes {
		_, isLeader := n.State()
		if isLeader {
			return n
		}
	}
	return nil
}

func (c *cluster) waitLeader(timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := c.findLeader()
		if leader != nil {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

func TestRaft_LeaderElection(t *testing.T) {
	nodeIDs := []string{"n1", "n2", "n3"}
	c := newCluster(nodeIDs)
	defer c.Stop()

	leader := c.waitLeader(2 * time.Second)
	if leader == nil {
		t.Fatal("expected a leader to be elected within 2s")
	}

	term, isLeader := leader.State()
	if !isLeader || term < 1 {
		t.Fatalf("expected elected leader with term >= 1, got term %d, isLeader %v", term, isLeader)
	}

	// Verify only 1 node is leader
	leadersCount := 0
	for _, n := range c.nodes {
		_, isL := n.State()
		if isL {
			leadersCount++
		}
	}

	if leadersCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leadersCount)
	}
}

func TestRaft_LogReplication(t *testing.T) {
	nodeIDs := []string{"n1", "n2", "n3"}
	c := newCluster(nodeIDs)
	defer c.Stop()

	leader := c.waitLeader(2 * time.Second)
	if leader == nil {
		t.Fatal("failed to elect leader")
	}

	commands := []string{"SET foo=bar", "SET count=1", "DEL foo"}
	for _, cmd := range commands {
		idx, _, isL := leader.Propose([]byte(cmd))
		if !isL {
			t.Fatalf("leader lost authority before proposal")
		}
		if idx == 0 {
			t.Fatal("invalid index returned")
		}
	}

	// Verify all nodes apply the exact 3 commands
	for _, id := range nodeIDs {
		applyCh := c.applyChs[id]
		for i, expectedCmd := range commands {
			select {
			case msg := <-applyCh:
				if string(msg.Command) != expectedCmd {
					t.Fatalf("node %s entry %d mismatch: got %s, expected %s",
						id, i, string(msg.Command), expectedCmd)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("node %s timed out waiting for command %d", id, i)
			}
		}
	}
}

func TestRaft_LeaderFailureAndReelection(t *testing.T) {
	nodeIDs := []string{"n1", "n2", "n3"}
	c := newCluster(nodeIDs)
	defer c.Stop()

	initialLeader := c.waitLeader(2 * time.Second)
	if initialLeader == nil {
		t.Fatal("initial leader election failed")
	}

	initialLeaderID := initialLeader.ID()
	initialTerm, _ := initialLeader.State()

	// Partition the initial leader
	c.transport.SetPartition(initialLeaderID, true)

	// Wait for a new leader to be elected among remaining 2 nodes
	var newLeader *Node
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for id, n := range c.nodes {
			if id != initialLeaderID {
				_, isL := n.State()
				if isL {
					newLeader = n
					break
				}
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if newLeader == nil {
		t.Fatal("failed to elect new leader after old leader was partitioned")
	}

	newTerm, _ := newLeader.State()
	if newTerm <= initialTerm {
		t.Fatalf("new leader term (%d) should be > initial term (%d)", newTerm, initialTerm)
	}

	// Heal partition: old leader reconnects
	c.transport.SetPartition(initialLeaderID, false)
	time.Sleep(200 * time.Millisecond)

	// Old leader must have stepped down to follower
	if initialLeader.CurrentRole() == RoleLeader {
		t.Fatalf("reconnected old leader should have stepped down from Leader role")
	}
}

func TestRaft_NetworkPartitionQuorum(t *testing.T) {
	// 5-node cluster: N1, N2, N3 (majority = 3), N4, N5 (minority = 2)
	nodeIDs := []string{"n1", "n2", "n3", "n4", "n5"}
	c := newCluster(nodeIDs)
	defer c.Stop()

	leader := c.waitLeader(2 * time.Second)
	if leader == nil {
		t.Fatal("failed to elect leader")
	}

	// Isolate N4 and N5 into a minority partition
	c.transport.SetPartition("n4", true)
	c.transport.SetPartition("n5", true)

	// Wait a moment for cluster to stabilize
	time.Sleep(100 * time.Millisecond)

	// The majority (N1, N2, N3) must still be able to commit proposals!
	activeLeader := c.waitLeader(2 * time.Second)
	if activeLeader == nil {
		t.Fatal("majority partition should maintain or elect a leader")
	}

	_, _, isL := activeLeader.Propose([]byte("quorum_command"))
	if !isL {
		t.Fatal("active leader should accept proposal")
	}

	// Verify majority node applies the command
	select {
	case msg := <-c.applyChs[activeLeader.ID()]:
		if string(msg.Command) != "quorum_command" {
			t.Fatalf("unexpected command: %s", string(msg.Command))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("majority failed to commit proposal")
	}
}
