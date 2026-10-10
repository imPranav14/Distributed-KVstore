package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// recordingPeer forwards to an inner Peer and records the Success flag of
// every AppendEntries response, so a test can prove a REJECTED request was
// still counted as a leadership acknowledgement.
type recordingPeer struct {
	inner Peer

	mu        sync.Mutex
	successes []bool
}

func (r *recordingPeer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return r.inner.RequestVote(ctx, req)
}

func (r *recordingPeer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	resp, err := r.inner.AppendEntries(ctx, req)
	if err == nil {
		r.mu.Lock()
		r.successes = append(r.successes, resp.Success)
		r.mu.Unlock()
	}
	return resp, err
}

// leaderWithCommittedNoOp makes n leader of term with a committed entry from
// that same term — the state a real leader reaches once its no-op commits.
func leaderWithCommittedNoOp(t *testing.T, n *Node, term uint64) {
	t.Helper()
	seedLog(t, n, &raftpb.LogEntry{Term: term, Index: 1, Command: nil})
	makeLeader(t, n, term)
	n.mu.Lock()
	n.commitIndex = 1
	n.mu.Unlock()
}

// This test checks that a non-leader refuses to serve a read index at all.
func TestReadIndex_FollowerReturnsErrNotLeader(t *testing.T) {
	n := newTestNode(t, 1)

	if _, err := n.ReadIndex(context.Background()); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("ReadIndex on a follower = %v, want ErrNotLeader", err)
	}
}

// This test checks the ReadIndex precondition: a leader whose commitIndex
// still points at an entry from an EARLIER term must refuse. Its commit
// index may be below entries the previous leader already committed — a
// read there could miss a write that already returned OK.
func TestReadIndex_LeaderWithoutCurrentTermCommitIsNotReady(t *testing.T) {
	n := newTestNode(t, 1)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("old")},
		&raftpb.LogEntry{Term: 2, Index: 2, Command: nil}, // our no-op, NOT yet committed
	)
	makeLeader(t, n, 2)
	n.mu.Lock()
	n.commitIndex = 1 // only the term-1 entry is known committed
	n.mu.Unlock()

	if _, err := n.ReadIndex(context.Background()); !errors.Is(err, ErrNotReadyForReads) {
		t.Fatalf("ReadIndex before a current-term commit = %v, want ErrNotReadyForReads", err)
	}
}

// This test checks that a single-node leader confirms itself — it alone is
// the majority — and returns its commit index.
func TestReadIndex_SingleNodeReturnsCommitIndex(t *testing.T) {
	n := newTestNode(t, 1)
	leaderWithCommittedNoOp(t, n, 3)

	index, err := n.ReadIndex(context.Background())
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if index != 1 {
		t.Fatalf("ReadIndex = %d, want 1 (the commit index)", index)
	}
}

// This test checks the core safety property: a leader that cannot reach a
// majority must NOT return a read index. This is the cut-off leader that,
// without the confirmation round, would answer from data a newer leader on
// the other side of the partition has already changed.
func TestReadIndex_CutOffLeaderCannotConfirm(t *testing.T) {
	n := newTestNode(t, 1)
	n.peers = map[uint64]Peer{2: deadPeer{}, 3: deadPeer{}}
	leaderWithCommittedNoOp(t, n, 3)

	_, err := n.ReadIndex(context.Background())
	if !errors.Is(err, ErrLeadershipNotConfirmed) {
		t.Fatalf("ReadIndex with no reachable peers = %v, want ErrLeadershipNotConfirmed", err)
	}
}

// This test checks that a majority of acknowledgements is enough — one
// unreachable peer out of two does not block reads in a 3-node cluster.
func TestReadIndex_ConfirmsWithMajorityDespiteOneDeadPeer(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := nodes[0]
	leader.peers[nodes[2].id] = deadPeer{}
	leaderWithCommittedNoOp(t, leader, 1)

	if _, err := leader.ReadIndex(context.Background()); err != nil {
		t.Fatalf("ReadIndex with 2 of 3 reachable: %v, want success", err)
	}
}

// This test checks the subtle line in replicateToPeer: a follower whose log
// does NOT match (AppendEntries Success=false) still acknowledges the
// leader's term, and must be counted. Counting only Success would make
// reads wait for lagging followers to catch up, for no safety gain — the
// read needs proof of leadership, not of log agreement.
func TestReadIndex_RejectedAppendEntriesStillCountsAsAcknowledgement(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := nodes[0]
	// Both followers have EMPTY logs; the leader's first AppendEntries
	// claims prevLogIndex=1, so both reject it.
	recorders := map[uint64]*recordingPeer{}
	for _, f := range nodes[1:] {
		r := &recordingPeer{inner: fakePeer{node: f}}
		recorders[f.id] = r
		leader.peers[f.id] = r
	}
	leaderWithCommittedNoOp(t, leader, 1)

	if _, err := leader.ReadIndex(context.Background()); err != nil {
		t.Fatalf("ReadIndex: %v — rejected-but-same-term responses were not counted", err)
	}

	// ReadIndex returns as soon as it has a majority, so the slower
	// follower's reply may still be in flight. Wait for both, then require
	// that EVERY reply was a rejection: ReadIndex succeeded with no
	// Success=true from anyone, so rejections are what confirmed it.
	waitFor(t, time.Second, func() bool {
		for _, r := range recorders {
			r.mu.Lock()
			n := len(r.successes)
			r.mu.Unlock()
			if n == 0 {
				return false
			}
		}
		return true
	})
	for id, r := range recorders {
		r.mu.Lock()
		replies := append([]bool(nil), r.successes...)
		r.mu.Unlock()
		for _, success := range replies {
			if success {
				t.Fatalf("setup: follower %d accepted an AppendEntries — this test needs rejections only", id)
			}
		}
	}
}

// This test checks that a leader which has been superseded learns it during
// the confirmation round and fails the read, rather than serving stale data.
func TestReadIndex_SupersededLeaderFailsAndStepsDown(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := nodes[0]
	leaderWithCommittedNoOp(t, leader, 1)
	// The followers have since moved on to term 5 under a different leader.
	for _, f := range nodes[1:] {
		if err := f.state.Save(5, 9); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := leader.ReadIndex(ctx); !errors.Is(err, ErrLeadershipNotConfirmed) {
		t.Fatalf("ReadIndex on a superseded leader = %v, want ErrLeadershipNotConfirmed", err)
	}
	if leader.Role() != Follower {
		t.Fatalf("superseded leader's role = %v, want Follower — it saw term 5 and must step down", leader.Role())
	}
}
