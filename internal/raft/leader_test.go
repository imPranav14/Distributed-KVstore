package raft

import (
	"testing"
	"time"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// makeLeader forces a node into the Leader role at the given term, with
// leader state initialized — the same sequence startElection performs on
// winning, without needing a real election.
func makeLeader(t *testing.T, n *Node, term uint64) {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.state.Save(term, n.id); err != nil {
		t.Fatalf("Save: %v", err)
	}
	n.role = Leader
	n.initializeLeaderStateLocked()
}

// This test checks the invariant that only a leader may create log
// entries — a follower accepting a Propose would create entries that no
// consistency check could ever reconcile, since the log's whole ordering
// authority comes from having exactly one writer.
func TestPropose_RejectedWhenNotLeader(t *testing.T) {
	n := newTestNode(t, 1)

	_, _, isLeader := n.Propose([]byte("x"))

	if isLeader {
		t.Fatalf("Propose on a Follower returned isLeader = true, want false")
	}
	if n.log.LastIndex() != 0 {
		t.Fatalf("log grew to index %d on a rejected Propose, want 0", n.log.LastIndex())
	}
}

// This test checks that a leader's Propose appends at the next index with
// the current term, and that indices advance monotonically.
func TestPropose_AppendsAtNextIndexWithCurrentTerm(t *testing.T) {
	n := newTestNode(t, 1)
	makeLeader(t, n, 7)

	index1, term1, ok1 := n.Propose([]byte("a"))
	index2, term2, ok2 := n.Propose([]byte("b"))

	if !ok1 || !ok2 {
		t.Fatalf("Propose returned isLeader=false on a Leader")
	}
	if index1 != 1 || index2 != 2 {
		t.Fatalf("indices = %d, %d; want 1, 2", index1, index2)
	}
	if term1 != 7 || term2 != 7 {
		t.Fatalf("terms = %d, %d; want 7, 7", term1, term2)
	}
}

// This test checks the deliberate asymmetry in leader state initialization:
// nextIndex is optimistic (one past our last entry) while matchIndex is
// pessimistic (zero). Optimistic matchIndex would let a fresh leader count
// entries as replicated that no follower actually holds.
func TestInitializeLeaderState_OptimisticNextPessimisticMatch(t *testing.T) {
	n := newTestNode(t, 1)
	n.peers = map[uint64]Peer{2: nil, 3: nil}
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 1, Index: 2, Command: []byte("b")},
	)

	n.mu.Lock()
	n.initializeLeaderStateLocked()
	n.mu.Unlock()

	for _, peerID := range []uint64{2, 3} {
		if n.nextIndex[peerID] != 3 {
			t.Fatalf("nextIndex[%d] = %d, want 3 (one past our last entry)", peerID, n.nextIndex[peerID])
		}
		if n.matchIndex[peerID] != 0 {
			t.Fatalf("matchIndex[%d] = %d, want 0 (we have confirmed nothing yet)", peerID, n.matchIndex[peerID])
		}
	}
}

// --- commitIndex advancement ---

// This test checks that an entry replicated on a majority IS committed when
// it belongs to the leader's current term.
func TestAdvanceCommitIndex_CommitsCurrentTermEntryOnMajority(t *testing.T) {
	n := newTestNode(t, 1)
	n.peers = map[uint64]Peer{2: nil, 3: nil}
	makeLeader(t, n, 5)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 5, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 5, Index: 2, Command: []byte("b")},
	)

	n.mu.Lock()
	n.matchIndex[2] = 2 // one follower has both entries; with us that is 2 of 3
	n.matchIndex[3] = 0
	n.advanceCommitIndexLocked()
	n.mu.Unlock()

	if n.CommitIndex() != 2 {
		t.Fatalf("CommitIndex() = %d, want 2", n.CommitIndex())
	}
}

// This test checks that a majority is genuinely required — one replica out
// of three is not enough, no matter how durable it is locally.
func TestAdvanceCommitIndex_NoMajorityDoesNotCommit(t *testing.T) {
	n := newTestNode(t, 1)
	n.peers = map[uint64]Peer{2: nil, 3: nil}
	makeLeader(t, n, 5)
	seedLog(t, n, &raftpb.LogEntry{Term: 5, Index: 1, Command: []byte("a")})

	n.mu.Lock()
	n.matchIndex[2] = 0
	n.matchIndex[3] = 0
	n.advanceCommitIndexLocked()
	n.mu.Unlock()

	if n.CommitIndex() != 0 {
		t.Fatalf("CommitIndex() = %d, want 0 — only the leader holds this entry", n.CommitIndex())
	}
}

// This test checks the Raft paper's Figure 8 rule, the subtlest safety rule
// in the algorithm: an entry from a PREVIOUS term must NOT be committed by
// counting replicas, even when it sits on a clear majority. Committing it
// would allow a later leader — legitimately elected, because its log is
// more up-to-date by term — to overwrite an entry already reported as
// committed.
func TestAdvanceCommitIndex_RefusesToCommitPreviousTermEntryByCounting(t *testing.T) {
	n := newTestNode(t, 1)
	n.peers = map[uint64]Peer{2: nil, 3: nil}
	makeLeader(t, n, 4) // we lead term 4
	// Our log holds only an OLD term-2 entry, now replicated everywhere.
	seedLog(t, n, &raftpb.LogEntry{Term: 2, Index: 1, Command: []byte("old")})

	n.mu.Lock()
	n.matchIndex[2] = 1
	n.matchIndex[3] = 1 // replicated on ALL three nodes
	n.advanceCommitIndexLocked()
	n.mu.Unlock()

	if n.CommitIndex() != 0 {
		t.Fatalf("CommitIndex() = %d, want 0 — a previous-term entry must not be committed by counting replicas, even on a full majority (Figure 8)", n.CommitIndex())
	}
}

// This test checks the other half of the Figure 8 rule: once a CURRENT-term
// entry reaches a majority, it commits — and every earlier entry commits
// transitively along with it, because commitIndex is a single high-water
// mark covering everything below.
func TestAdvanceCommitIndex_CurrentTermEntryCommitsPreviousOnesTransitively(t *testing.T) {
	n := newTestNode(t, 1)
	n.peers = map[uint64]Peer{2: nil, 3: nil}
	makeLeader(t, n, 4)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 2, Index: 1, Command: []byte("old")},  // previous term
		&raftpb.LogEntry{Term: 4, Index: 2, Command: []byte("mine")}, // current term
	)

	n.mu.Lock()
	n.matchIndex[2] = 2
	n.matchIndex[3] = 0
	n.advanceCommitIndexLocked()
	n.mu.Unlock()

	if n.CommitIndex() != 2 {
		t.Fatalf("CommitIndex() = %d, want 2 — committing the current-term entry must carry the older one with it", n.CommitIndex())
	}
}

// --- end-to-end replication through the Peer interface ---

// This test checks the whole replication path in-process: a leader proposes
// commands, and every follower ends up with a byte-identical log, with the
// leader's commitIndex advancing to cover them.
func TestReplication_LeaderPropagatesEntriesToFollowers(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader, followers := nodes[0], nodes[1:]
	makeLeader(t, leader, 1)

	for _, cmd := range []string{"a", "b", "c"} {
		if _, _, ok := leader.Propose([]byte(cmd)); !ok {
			t.Fatalf("Propose(%q) rejected", cmd)
		}
	}

	// Drive replication directly rather than waiting on the timer, so the
	// test is deterministic instead of timing-dependent.
	for i := 0; i < 5; i++ {
		leader.replicateToAll()
		time.Sleep(20 * time.Millisecond)
	}

	for _, f := range followers {
		if got := logCommands(f); !equalStrings(got, []string{"a", "b", "c"}) {
			t.Fatalf("follower %d log = %v, want [a b c]", f.id, got)
		}
	}
	if leader.CommitIndex() != 3 {
		t.Fatalf("leader CommitIndex() = %d, want 3", leader.CommitIndex())
	}
}

// This test checks nextIndex backtracking: a follower whose log has
// diverged must be repaired by the leader walking backward until the logs
// agree, then overwriting the bad suffix. This is the recovery path for a
// follower that followed a since-deposed leader.
func TestReplication_BacktracksToRepairDivergedFollower(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader, diverged, healthy := nodes[0], nodes[1], nodes[2]

	// The leader's authoritative log, all from term 5.
	makeLeader(t, leader, 5)
	seedLog(t, leader,
		&raftpb.LogEntry{Term: 5, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 5, Index: 2, Command: []byte("b")},
		&raftpb.LogEntry{Term: 5, Index: 3, Command: []byte("c")},
	)
	leader.mu.Lock()
	leader.initializeLeaderStateLocked() // re-init now that the log is seeded
	leader.mu.Unlock()

	// The diverged follower shares entry 1 but has two bogus entries from a
	// dead leader's term 3 at positions 2 and 3.
	seedLog(t, diverged,
		&raftpb.LogEntry{Term: 5, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 3, Index: 2, Command: []byte("bogus-b")},
		&raftpb.LogEntry{Term: 3, Index: 3, Command: []byte("bogus-c")},
	)

	for i := 0; i < 8; i++ {
		leader.replicateToAll()
		time.Sleep(20 * time.Millisecond)
	}

	if got := logCommands(diverged); !equalStrings(got, []string{"a", "b", "c"}) {
		t.Fatalf("diverged follower log = %v, want [a b c] — backtracking failed to repair it", got)
	}
	if got := logCommands(healthy); !equalStrings(got, []string{"a", "b", "c"}) {
		t.Fatalf("healthy follower log = %v, want [a b c]", got)
	}
}

// This test checks the invariant the AppendEntries consistency check
// exists to protect, expressed as its consequence rather than its
// mechanism: a leader must never conclude that a follower holds entries it
// does not actually have.
//
// The setup is the ordinary one after any election — the leader's
// nextIndex starts OPTIMISTICALLY at lastIndex+1, so its very first
// AppendEntries to a lagging follower claims PrevLogIndex=3 when that
// follower only holds entry 1. The consistency check is what turns that
// optimistic guess into a rejection instead of a false confirmation.
// Without it the follower answers Success, the leader records
// matchIndex=3, counts that toward a majority, and commits entries that
// exist on exactly one machine.
func TestReplication_LaggingFollowerIsNotCountedAsCaughtUp(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader, lagging := nodes[0], nodes[1]

	makeLeader(t, leader, 5)
	seedLog(t, leader,
		&raftpb.LogEntry{Term: 5, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 5, Index: 2, Command: []byte("b")},
		&raftpb.LogEntry{Term: 5, Index: 3, Command: []byte("c")},
	)
	leader.mu.Lock()
	leader.initializeLeaderStateLocked() // nextIndex = 4 for both peers
	leader.mu.Unlock()

	// One follower is far behind; the other is unreachable, so the lagging
	// follower's answer is the only one that can form a majority.
	seedLog(t, lagging, &raftpb.LogEntry{Term: 5, Index: 1, Command: []byte("a")})
	leader.mu.Lock()
	leader.peers[nodes[2].id] = deadPeer{}
	leader.mu.Unlock()

	// A single replication round: the leader's optimistic first guess.
	leader.replicateToAll()
	time.Sleep(50 * time.Millisecond)

	leader.mu.Lock()
	matchIndex := leader.matchIndex[lagging.id]
	leader.mu.Unlock()

	if matchIndex > lagging.log.LastIndex() {
		t.Fatalf("leader recorded matchIndex=%d for a follower whose log ends at %d — it will count entries toward a majority that only the leader holds",
			matchIndex, lagging.log.LastIndex())
	}
	if leader.CommitIndex() > lagging.log.LastIndex() {
		t.Fatalf("leader committed through index %d while the only reachable follower holds %d entries — committed data exists on a single machine and dies with it",
			leader.CommitIndex(), lagging.log.LastIndex())
	}
}

// This test checks the invariant that matchIndex records only what a
// follower has CONFIRMED — the entries inside the request it acknowledged —
// not the leader's log length at whatever moment the response happens to
// arrive.
//
// A Propose that lands while an AppendEntries is in flight grows the
// leader's log. If matchIndex were computed from n.log.LastIndex() instead
// of prevLogIndex+len(entries), the leader would credit the follower with
// an entry it was never sent. In a 3-node cluster that single false credit
// is a majority, and since Propose stamps the CURRENT term the Figure 8 rule
// offers no protection: the entry is committed while it exists on exactly
// one machine. The gated peer makes this interleaving deterministic instead
// of a timing accident.
func TestReplication_MatchIndexCountsOnlyEntriesActuallySent(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader, follower := nodes[0], nodes[1]

	gate := &gatedPeer{
		inner:   fakePeer{node: follower},
		arrived: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	// If the test fails before releasing, don't leave the replication
	// goroutine blocked forever.
	t.Cleanup(func() {
		select {
		case <-gate.release:
		default:
			close(gate.release)
		}
	})

	leader.mu.Lock()
	leader.peers[follower.id] = gate
	leader.peers[nodes[2].id] = deadPeer{} // so this follower's ack alone decides the majority
	leader.mu.Unlock()

	makeLeader(t, leader, 1)
	if _, _, ok := leader.Propose([]byte("a")); !ok {
		t.Fatalf("Propose(a) rejected")
	}

	leader.replicateToAll()
	<-gate.arrived // the request carrying ONLY entry 1 is now in flight

	// While that request is in flight, the leader's log grows to 2 entries.
	if _, _, ok := leader.Propose([]byte("b")); !ok {
		t.Fatalf("Propose(b) rejected")
	}

	close(gate.release)

	waitFor(t, time.Second, func() bool {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		return leader.matchIndex[follower.id] > 0
	})

	leader.mu.Lock()
	matchIndex := leader.matchIndex[follower.id]
	leader.mu.Unlock()

	if matchIndex != 1 {
		t.Fatalf("matchIndex = %d, want 1 — the follower acknowledged a request containing only entry 1", matchIndex)
	}
	if leader.CommitIndex() != 1 {
		t.Fatalf("CommitIndex() = %d, want 1 — entry 2 exists only on the leader and must not be committed", leader.CommitIndex())
	}
}

// This test checks that replication still commits with one node completely
// unreachable: 2 of 3 is a majority, so a single failure must not stall
// progress. This is the actual fault-tolerance promise of the system.
func TestReplication_CommitsWithOneFollowerDown(t *testing.T) {
	nodes := newTestCluster(t, 3)
	leader := nodes[0]
	makeLeader(t, leader, 1)

	// Replace one peer with a fake that always fails, simulating a node
	// that is down or partitioned away.
	leader.mu.Lock()
	leader.peers[nodes[2].id] = deadPeer{}
	leader.initializeLeaderStateLocked()
	leader.mu.Unlock()

	if _, _, ok := leader.Propose([]byte("a")); !ok {
		t.Fatalf("Propose rejected")
	}

	for i := 0; i < 5; i++ {
		leader.replicateToAll()
		time.Sleep(20 * time.Millisecond)
	}

	if leader.CommitIndex() != 1 {
		t.Fatalf("CommitIndex() = %d, want 1 — 2 of 3 nodes is a majority, one failure must not stall commits", leader.CommitIndex())
	}
	if got := logCommands(nodes[1]); !equalStrings(got, []string{"a"}) {
		t.Fatalf("reachable follower log = %v, want [a]", got)
	}
}
