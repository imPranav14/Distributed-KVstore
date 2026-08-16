package raft

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// fakePeer implements Peer by calling directly into another in-process
// Node's handler methods — no network, no gRPC, exactly the decoupling
// the Peer interface exists to provide.
type fakePeer struct {
	node *Node
}

func (p fakePeer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return p.node.HandleRequestVote(req), nil
}

func (p fakePeer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return p.node.HandleAppendEntries(req), nil
}

func newTestNode(t *testing.T, id uint64) *Node {
	t.Helper()
	s, err := LoadState(filepath.Join(t.TempDir(), "raft-state.dat"))
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	return NewNode(id, s, map[uint64]Peer{})
}

// newTestCluster wires n nodes together with fakePeers pointing at each
// other, but does NOT call Start() — tests that want the background timer
// loop running call Start()/Stop() themselves.
func newTestCluster(t *testing.T, n int) []*Node {
	t.Helper()
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = newTestNode(t, uint64(i+1))
	}
	for i, node := range nodes {
		peers := make(map[uint64]Peer)
		for j, other := range nodes {
			if i != j {
				peers[other.id] = fakePeer{node: other}
			}
		}
		node.peers = peers
	}
	return nodes
}

// waitFor polls check every 10ms until it returns true or timeout elapses,
// failing the test if it never does. Used instead of a single fixed sleep
// so tests are neither flaky (too short) nor needlessly slow (too long).
func waitFor(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

// --- HandleRequestVote ---

// This test checks the invariant that a follower with no vote yet in a
// term grants the vote to the first candidate that asks.
func TestHandleRequestVote_GrantsVoteToFirstCandidate(t *testing.T) {
	n := newTestNode(t, 1)

	resp := n.HandleRequestVote(&raftpb.RequestVoteRequest{Term: 1, CandidateId: 2})

	if !resp.VoteGranted {
		t.Fatalf("VoteGranted = false, want true")
	}
	if n.state.VotedFor() != 2 {
		t.Fatalf("VotedFor() = %d, want 2", n.state.VotedFor())
	}
}

// This test checks the invariant that a different candidate is refused
// once we've already voted for someone in the same term — the core safety
// property (at most one vote per term) at the unit level.
func TestHandleRequestVote_RejectsSecondCandidateSameTerm(t *testing.T) {
	n := newTestNode(t, 1)

	n.HandleRequestVote(&raftpb.RequestVoteRequest{Term: 1, CandidateId: 2})
	resp := n.HandleRequestVote(&raftpb.RequestVoteRequest{Term: 1, CandidateId: 3})

	if resp.VoteGranted {
		t.Fatalf("second candidate in the same term: VoteGranted = true, want false")
	}
}

// This test checks the invariant that re-granting to the SAME candidate we
// already voted for in this term is fine — an idempotent retry of an RPC
// we already answered, not a second distinct vote.
func TestHandleRequestVote_GrantsRepeatToSameCandidate(t *testing.T) {
	n := newTestNode(t, 1)

	n.HandleRequestVote(&raftpb.RequestVoteRequest{Term: 1, CandidateId: 2})
	resp := n.HandleRequestVote(&raftpb.RequestVoteRequest{Term: 1, CandidateId: 2})

	if !resp.VoteGranted {
		t.Fatalf("retried vote for the same candidate: VoteGranted = false, want true")
	}
}

// This test checks the invariant that a candidate campaigning on a term
// lower than ours is rejected, and told our (higher) term so it can catch
// up.
func TestHandleRequestVote_RejectsStaleTerm(t *testing.T) {
	n := newTestNode(t, 1)
	n.state.Save(5, NoVote)

	resp := n.HandleRequestVote(&raftpb.RequestVoteRequest{Term: 3, CandidateId: 2})

	if resp.VoteGranted {
		t.Fatalf("stale-term candidate: VoteGranted = true, want false")
	}
	if resp.Term != 5 {
		t.Fatalf("response Term = %d, want 5 (our current term)", resp.Term)
	}
}

// This test checks the invariant that seeing a higher term makes us adopt
// it and become a follower BEFORE evaluating the vote — a leader or
// candidate discovering it's stale purely from an incoming message.
func TestHandleRequestVote_HigherTermCausesStepDown(t *testing.T) {
	n := newTestNode(t, 1)
	n.role = Leader
	n.state.Save(2, NoVote)

	n.HandleRequestVote(&raftpb.RequestVoteRequest{Term: 9, CandidateId: 2})

	if n.role != Follower {
		t.Fatalf("role after seeing a higher term = %v, want Follower", n.role)
	}
	if n.state.CurrentTerm() != 9 {
		t.Fatalf("CurrentTerm() = %d, want 9", n.state.CurrentTerm())
	}
}

// --- HandleAppendEntries ---

// This test checks the invariant that a leader/candidate with a term
// behind ours is rejected.
func TestHandleAppendEntries_StaleTermRejected(t *testing.T) {
	n := newTestNode(t, 1)
	n.state.Save(5, NoVote)

	resp := n.HandleAppendEntries(&raftpb.AppendEntriesRequest{Term: 3, LeaderId: 2})

	if resp.Success {
		t.Fatalf("stale-term AppendEntries: Success = true, want false")
	}
}

// This test checks the invariant that a higher term in an AppendEntries
// causes step-down, same rule as RequestVote.
func TestHandleAppendEntries_HigherTermCausesStepDown(t *testing.T) {
	n := newTestNode(t, 1)
	n.role = Candidate
	n.state.Save(2, 1)

	n.HandleAppendEntries(&raftpb.AppendEntriesRequest{Term: 9, LeaderId: 2})

	if n.role != Follower {
		t.Fatalf("role after seeing a higher term = %v, want Follower", n.role)
	}
	if n.state.CurrentTerm() != 9 {
		t.Fatalf("CurrentTerm() = %d, want 9", n.state.CurrentTerm())
	}
}

// This test checks the invariant that a candidate receiving a heartbeat
// for the SAME term steps down without bumping the term — it lost the
// election, it isn't behind.
func TestHandleAppendEntries_CandidateStepsDownOnSameTermHeartbeat(t *testing.T) {
	n := newTestNode(t, 1)
	n.role = Candidate
	n.state.Save(5, 1)

	n.HandleAppendEntries(&raftpb.AppendEntriesRequest{Term: 5, LeaderId: 2})

	if n.role != Follower {
		t.Fatalf("role after same-term heartbeat = %v, want Follower", n.role)
	}
	if n.state.CurrentTerm() != 5 {
		t.Fatalf("CurrentTerm() = %d, want unchanged at 5", n.state.CurrentTerm())
	}
}

// --- Multi-node integration (in-process, via fakePeer) ---

// This test checks the invariant at the heart of this whole milestone:
// a 3-node cluster, started with no leader, elects EXACTLY one leader —
// not zero (livelock), not more than one (the safety violation CLAUDE.md's
// interview list asks you to prove impossible).
func TestThreeNodeCluster_ElectsExactlyOneLeader(t *testing.T) {
	nodes := newTestCluster(t, 3)
	for _, n := range nodes {
		n.Start()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	waitFor(t, 2*time.Second, func() bool {
		leaders := 0
		for _, n := range nodes {
			if n.Role() == Leader {
				leaders++
			}
		}
		return leaders == 1
	})

	leaders, followers := 0, 0
	for _, n := range nodes {
		switch n.Role() {
		case Leader:
			leaders++
		case Follower:
			followers++
		}
	}
	if leaders != 1 {
		t.Fatalf("leaders = %d, want exactly 1", leaders)
	}
	if followers != 2 {
		t.Fatalf("followers = %d, want exactly 2", followers)
	}
}

// This test checks the invariant that a cluster re-elects a new leader
// after the current one stops responding — the actual fault-tolerance
// property this whole milestone exists to provide.
func TestThreeNodeCluster_ReElectsAfterLeaderStops(t *testing.T) {
	nodes := newTestCluster(t, 3)
	for _, n := range nodes {
		n.Start()
	}

	waitFor(t, 2*time.Second, func() bool {
		for _, n := range nodes {
			if n.Role() == Leader {
				return true
			}
		}
		return false
	})

	var oldLeader *Node
	for _, n := range nodes {
		if n.Role() == Leader {
			oldLeader = n
		}
	}
	oldLeader.Stop() // simulates the leader going silent — no more heartbeats

	// oldLeader is already stopped above; Stop() is not idempotent (by
	// design — see node.go), so the cleanup below must not call it twice.
	defer func() {
		for _, n := range nodes {
			if n != oldLeader {
				n.Stop()
			}
		}
	}()

	waitFor(t, 2*time.Second, func() bool {
		for _, n := range nodes {
			if n != oldLeader && n.Role() == Leader {
				return true
			}
		}
		return false
	})

	newLeaderCount := 0
	for _, n := range nodes {
		if n != oldLeader && n.Role() == Leader {
			newLeaderCount++
		}
	}
	if newLeaderCount != 1 {
		t.Fatalf("new leaders among remaining nodes = %d, want exactly 1", newLeaderCount)
	}
}
