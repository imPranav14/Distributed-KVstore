package raft

import (
	"testing"
	"time"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// receiveApply reads one message from applyCh or fails the test.
func receiveApply(t *testing.T, applyCh <-chan ApplyMsg) ApplyMsg {
	t.Helper()
	select {
	case msg := <-applyCh:
		return msg
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for an ApplyMsg")
		return ApplyMsg{}
	}
}

// assertNothingApplied fails if applyCh delivers anything within a short
// window — used to prove that UNCOMMITTED entries are never handed to the
// state machine.
func assertNothingApplied(t *testing.T, applyCh <-chan ApplyMsg) {
	t.Helper()
	select {
	case msg := <-applyCh:
		t.Fatalf("applyCh delivered index %d, want nothing (entry is not committed)", msg.Index)
	case <-time.After(100 * time.Millisecond):
	}
}

// startApplierOnly launches the applier WITHOUT the election timer. A
// single-node test node now elects itself (see
// TestSingleNode_ElectsItselfAndCommitsOwnProposals); with the full Start()
// that self-election would append and commit a no-op mid-test. Stop() still
// ends it, via the same stopCh.
func startApplierOnly(n *Node) {
	go n.runApplier()
}

// This test checks that the applier delivers committed entries in index
// order, each exactly once, and delivers NOTHING beyond commitIndex. An
// uncommitted entry reaching the state machine could later be truncated by
// a new leader — after a replica had already acted on it.
func TestApplier_DeliversCommittedEntriesInOrderExactlyOnce(t *testing.T) {
	n, applyCh := newTestNodeWithApplyCh(t, 1)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 1, Index: 2, Command: []byte("b")},
		&raftpb.LogEntry{Term: 1, Index: 3, Command: []byte("c")},
	)
	startApplierOnly(n)
	defer n.Stop()

	assertNothingApplied(t, applyCh) // nothing committed yet

	n.mu.Lock()
	n.commitIndex = 2 // commit only the first two
	n.signalApplierLocked()
	n.mu.Unlock()

	for _, want := range []string{"a", "b"} {
		msg := receiveApply(t, applyCh)
		if string(msg.Command) != want {
			t.Fatalf("delivered %q, want %q — out of order", msg.Command, want)
		}
	}
	assertNothingApplied(t, applyCh) // entry 3 is not committed

	n.mu.Lock()
	n.commitIndex = 3
	n.signalApplierLocked()
	n.mu.Unlock()

	if msg := receiveApply(t, applyCh); string(msg.Command) != "c" || msg.Index != 3 {
		t.Fatalf("delivered index %d %q, want index 3 \"c\"", msg.Index, msg.Command)
	}
	assertNothingApplied(t, applyCh) // and nothing delivered twice

	if n.LastApplied() != 3 {
		t.Fatalf("LastApplied() = %d, want 3", n.LastApplied())
	}
}

// This test checks the applier's exit condition (Rule 5): Stop must end it
// even while it is BLOCKED sending to a consumer that never reads — and it
// must close applyCh on the way out, which is how the consumer's loop ends.
func TestApplier_ExitsOnStopWhileBlockedAndClosesChannel(t *testing.T) {
	dir := t.TempDir()
	s, _ := LoadState(dir + "/raft-state.dat")
	l, _ := OpenLog(dir+"/raft-log.dat", 0, 0)
	defer l.Close()

	applyCh := make(chan ApplyMsg) // UNBUFFERED, and nobody reads it yet
	n := NewNode(1, s, l, map[uint64]Peer{}, applyCh)
	seedLog(t, n, &raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")})
	startApplierOnly(n)

	n.mu.Lock()
	n.commitIndex = 1
	n.signalApplierLocked()
	n.mu.Unlock()

	time.Sleep(50 * time.Millisecond) // let the applier block on the send
	n.Stop()

	// Draining must reach a CLOSED channel. A pending message may or may
	// not be delivered first, depending on which select arm won — either
	// is fine. What must not happen is blocking forever.
	deadline := time.After(time.Second)
	for {
		select {
		case _, open := <-applyCh:
			if !open {
				return // closed: the applier exited cleanly
			}
		case <-deadline:
			t.Fatalf("applyCh was never closed after Stop — the applier goroutine leaked")
		}
	}
}

// This test checks the fix for the gap found in Milestone 6's interview
// (Q3): a leader elected after a restart, holding ONLY previous-term
// entries, must commit them without any client write. Before the no-op,
// commitIndex stayed at 0 forever — data durable on every disk, invisible
// to every reader.
func TestNoOp_NewLeaderCommitsPreviousTermEntriesWithoutClientWrites(t *testing.T) {
	nodes := newTestCluster(t, 3)
	for _, n := range nodes {
		seedLog(t, n, &raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("written-before-restart")})
		n.Start()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	// No Propose anywhere in this test. The only way commitIndex can reach
	// 1 is the new leader's own no-op committing it transitively.
	waitFor(t, 3*time.Second, func() bool {
		for _, n := range nodes {
			if n.CommitIndex() < 1 {
				return false
			}
		}
		return true
	})
}

// This test checks that the no-op is delivered to the state machine as an
// EMPTY command rather than filtered out. ReadIndex waits until the state
// machine's applied index reaches the read index — and right after an
// election that index IS the no-op's. Filtering no-ops out would make the
// first read after every leader change wait forever.
func TestNoOp_IsDeliveredAsEmptyCommand(t *testing.T) {
	n, applyCh := newTestNodeWithApplyCh(t, 1) // no peers: a single-node cluster
	startApplierOnly(n)
	defer n.Stop()

	n.startElection()

	msg := receiveApply(t, applyCh)
	if len(msg.Command) != 0 {
		t.Fatalf("first applied entry has command %q, want empty (the no-op)", msg.Command)
	}
	if msg.Index != 1 || msg.Term != 1 {
		t.Fatalf("no-op at index %d term %d, want index 1 term 1", msg.Index, msg.Term)
	}
}

// This test checks that a single-node cluster — permitted by config.go for
// local testing — can elect itself and commit. With zero peers the
// vote-counting loop never runs, so before 7a the node campaigned forever.
func TestSingleNode_ElectsItselfAndCommitsOwnProposals(t *testing.T) {
	n := newTestNode(t, 1)

	n.startElection()
	if n.Role() != Leader {
		t.Fatalf("single node after startElection: role = %v, want Leader", n.Role())
	}

	index, _, ok := n.Propose([]byte("x"))
	if !ok {
		t.Fatalf("Propose rejected on a single-node leader")
	}
	if n.CommitIndex() != index {
		t.Fatalf("CommitIndex() = %d, want %d — a lone leader IS the majority", n.CommitIndex(), index)
	}
}

// This test checks the subtlest invariant in 7a: a leader that cannot write
// its no-op steps down WITHOUT erasing its vote. becomeFollowerLocked would
// persist (term, NoVote) — leaving the node free to vote for a different
// candidate in a term where it already voted for itself.
func TestNoOp_FailureStepsDownWithoutErasingVote(t *testing.T) {
	n := newTestNode(t, 7)
	n.log.Close() // every Append now fails

	n.startElection() // single node: wins term 1, then the no-op write fails

	if n.Role() != Follower {
		t.Fatalf("role = %v, want Follower after a failed no-op", n.Role())
	}
	if n.state.CurrentTerm() != 1 || n.state.VotedFor() != 7 {
		t.Fatalf("persisted (term, votedFor) = (%d, %d), want (1, 7) — the self-vote was erased",
			n.state.CurrentTerm(), n.state.VotedFor())
	}

	resp := n.HandleRequestVote(&raftpb.RequestVoteRequest{Term: 1, CandidateId: 9})
	if resp.VoteGranted {
		t.Fatalf("granted a second vote in term 1 to candidate 9 after already voting for itself")
	}
}

// This test checks the commitIndex bug found while wiring the applier: a
// delayed AppendEntries delivering FEWER entries than the follower has
// already committed made min(leaderCommit, lastNewEntry) land below the
// current commitIndex — and the old code moved commitIndex backward. The
// existing NeverGoesBackward test only covered a lower LeaderCommit, so it
// never exercised this path.
func TestAppendEntries_ShortDelayedRequestDoesNotMoveCommitIndexBackward(t *testing.T) {
	n := newTestNode(t, 1)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 1, Index: 2, Command: []byte("b")},
		&raftpb.LogEntry{Term: 1, Index: 3, Command: []byte("c")},
		&raftpb.LogEntry{Term: 1, Index: 4, Command: []byte("d")},
		&raftpb.LogEntry{Term: 1, Index: 5, Command: []byte("e")},
	)
	n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 5, PrevLogTerm: 1, LeaderCommit: 5,
	})
	if n.CommitIndex() != 5 {
		t.Fatalf("setup: CommitIndex() = %d, want 5", n.CommitIndex())
	}

	// A delayed request from earlier: carries only entry 3, but by now the
	// leader's commit index (6) is above ours, so the outer check passes.
	n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 1,
		Entries:      []*raftpb.LogEntry{{Term: 1, Index: 3, Command: []byte("c")}},
		LeaderCommit: 6,
	})

	if n.CommitIndex() != 5 {
		t.Fatalf("CommitIndex() = %d, want 5 — it moved backward on a short delayed request", n.CommitIndex())
	}
}
