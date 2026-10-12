package raft

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// snapshotRequest is an InstallSnapshot from leader 2 in term.
func snapshotRequest(term, lastIncludedIndex, lastIncludedTerm uint64, data string) *raftpb.InstallSnapshotRequest {
	return &raftpb.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          2,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              []byte(data),
	}
}

// fiveCommandEntries seeds entries 1..5, all term 1, commands "a".."e".
func fiveCommandEntries(t *testing.T, n *Node) {
	t.Helper()
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 1, Index: 2, Command: []byte("b")},
		&raftpb.LogEntry{Term: 1, Index: 3, Command: []byte("c")},
		&raftpb.LogEntry{Term: 1, Index: 4, Command: []byte("d")},
		&raftpb.LogEntry{Term: 1, Index: 5, Command: []byte("e")},
	)
}

// --- follower side: HandleInstallSnapshot ---

// This test checks the whole follower-side contract on an empty follower:
// the snapshot is stored durably (it survives a restart), commitIndex jumps
// to it, and the applier is WOKEN to deliver it. The wakeup is checked
// directly — before any applier runs — because a running applier's own
// startup signal would deliver the snapshot anyway and hide a missing one.
func TestInstallSnapshot_FollowerStoresCommitsAndWakesApplier(t *testing.T) {
	dir := t.TempDir()
	follower, applyCh := openNodeAt(t, dir)

	resp, err := follower.HandleInstallSnapshot(snapshotRequest(1, 5, 1, "state-through-5"))
	if err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}
	if resp.Term != 1 {
		t.Fatalf("response term = %d, want 1", resp.Term)
	}
	if follower.CommitIndex() != 5 {
		t.Fatalf("commitIndex = %d, want 5", follower.CommitIndex())
	}
	if len(follower.applyNotify) != 1 {
		t.Fatalf("no applier wakeup pending after install — the snapshot would sit undelivered until the next commit")
	}

	go follower.runApplier() // no extra signal: only the install's wakeup may deliver it
	t.Cleanup(follower.Stop)
	msg := receiveApply(t, applyCh)
	if msg.Snapshot == nil || msg.Index != 5 || string(msg.Snapshot.Data) != "state-through-5" {
		t.Fatalf("applier delivered %+v, want the snapshot at index 5", msg)
	}

	restarted, _ := openNodeAt(t, dir)
	if restarted.log.SnapshotIndex() != 5 || string(restarted.log.Snapshot().Data) != "state-through-5" {
		t.Fatalf("snapshot not durable: after restart SnapshotIndex = %d", restarted.log.SnapshotIndex())
	}
}

// This test checks that a snapshot at or below commitIndex is ignored: the
// follower caught up through ordinary replication while the RPC was in
// flight. Installing it anyway would move commitIndex BACKWARD (5 to 3) and
// rewrite the log and snapshot files for nothing.
func TestInstallSnapshot_StaleSnapshotIsIgnored(t *testing.T) {
	dir := t.TempDir()
	follower, _ := openNodeAt(t, dir)
	fiveCommandEntries(t, follower)
	commitThrough(follower, 5)

	if _, err := follower.HandleInstallSnapshot(snapshotRequest(1, 3, 1, "old")); err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}

	if follower.CommitIndex() != 5 {
		t.Fatalf("commitIndex = %d after a stale snapshot, want 5 — it moved backward", follower.CommitIndex())
	}
	if follower.log.SnapshotIndex() != 0 {
		t.Fatalf("stale snapshot was installed: SnapshotIndex = %d, want 0", follower.log.SnapshotIndex())
	}
	if _, err := os.Stat(filepath.Join(dir, "snapshot.dat")); !os.IsNotExist(err) {
		t.Fatalf("stale snapshot was written to disk")
	}
}

// This test checks the term rules, which are AppendEntries' exactly: a
// snapshot from a stale leader is refused and answered with our term, so
// that leader steps down; a snapshot from a newer term is accepted and makes
// a candidate give up its election.
func TestInstallSnapshot_TermRules(t *testing.T) {
	t.Run("lower term is refused", func(t *testing.T) {
		follower, _ := openNodeAt(t, t.TempDir())
		if err := follower.state.Save(3, NoVote); err != nil {
			t.Fatalf("Save: %v", err)
		}

		resp, err := follower.HandleInstallSnapshot(snapshotRequest(2, 5, 2, "x"))
		if err != nil {
			t.Fatalf("HandleInstallSnapshot: %v", err)
		}
		if resp.Term != 3 {
			t.Fatalf("response term = %d, want 3 so the stale leader steps down", resp.Term)
		}
		if follower.log.SnapshotIndex() != 0 || follower.CommitIndex() != 0 {
			t.Fatalf("snapshot from a stale leader was installed")
		}
	})

	t.Run("higher term makes a candidate step down", func(t *testing.T) {
		candidate, _ := openNodeAt(t, t.TempDir())
		if err := candidate.state.Save(2, candidate.id); err != nil {
			t.Fatalf("Save: %v", err)
		}
		candidate.role = Candidate

		resp, err := candidate.HandleInstallSnapshot(snapshotRequest(4, 5, 4, "x"))
		if err != nil {
			t.Fatalf("HandleInstallSnapshot: %v", err)
		}
		if candidate.Role() != Follower || candidate.state.CurrentTerm() != 4 || resp.Term != 4 {
			t.Fatalf("role %v term %d, want Follower in term 4", candidate.Role(), candidate.state.CurrentTerm())
		}
		if candidate.log.SnapshotIndex() != 5 {
			t.Fatalf("snapshot not installed: SnapshotIndex = %d", candidate.log.SnapshotIndex())
		}
	})
}

// This test checks that an InstallSnapshot from the current leader counts
// as hearing from it. A follower being caught up by snapshot may receive no
// AppendEntries at all for a while; if this RPC did not reset its election
// timer, it would time out and start an election against a healthy leader.
func TestInstallSnapshot_ResetsElectionTimer(t *testing.T) {
	follower, _ := openNodeAt(t, t.TempDir())
	// Already in the leader's term. A HIGHER-term request would reset the
	// timer inside becomeFollowerLocked and hide a missing reset here.
	if err := follower.state.Save(1, NoVote); err != nil {
		t.Fatalf("Save: %v", err)
	}
	follower.mu.Lock()
	follower.lastHeartbeat = time.Now().Add(-time.Hour) // long overdue for an election
	follower.mu.Unlock()

	if _, err := follower.HandleInstallSnapshot(snapshotRequest(1, 5, 1, "x")); err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}

	follower.mu.Lock()
	defer follower.mu.Unlock()
	if time.Since(follower.lastHeartbeat) > follower.electionTimeout {
		t.Fatalf("election timer not reset: last heard from a leader %v ago", time.Since(follower.lastHeartbeat))
	}
}

// This test checks the Raft paper's rule for a follower that already has
// the snapshot's last entry: if its own entry there has the SAME term, the
// entries after it are the leader's too (Log Matching) and are kept; if the
// term differs, nothing after it can be trusted and the whole log goes.
func TestInstallSnapshot_KeepsSuffixOnlyIfItMatches(t *testing.T) {
	t.Run("matching term keeps entries after the snapshot", func(t *testing.T) {
		follower, _ := openNodeAt(t, t.TempDir())
		fiveCommandEntries(t, follower)
		commitThrough(follower, 2)

		if _, err := follower.HandleInstallSnapshot(snapshotRequest(1, 3, 1, "x")); err != nil {
			t.Fatalf("HandleInstallSnapshot: %v", err)
		}
		if got := logCommands(follower); !equalStrings(got, []string{"d", "e"}) {
			t.Fatalf("log after snapshot = %v, want [d e]", got)
		}
	})

	t.Run("different term discards the whole log", func(t *testing.T) {
		follower, _ := openNodeAt(t, t.TempDir())
		fiveCommandEntries(t, follower)
		commitThrough(follower, 2)

		if _, err := follower.HandleInstallSnapshot(snapshotRequest(2, 3, 2, "x")); err != nil {
			t.Fatalf("HandleInstallSnapshot: %v", err)
		}
		if follower.log.LastIndex() != 3 || len(logCommands(follower)) != 0 {
			t.Fatalf("log = %v ending at %d, want empty ending at the snapshot (3)", logCommands(follower), follower.log.LastIndex())
		}
	})
}

// This test checks delivery order when a snapshot is installed while the
// applier is mid-batch. The applier has copied entries 1..3 and is blocked
// handing entry 2 to a slow state machine; a snapshot through 5 arrives.
// The state machine must still see a strictly increasing sequence — 1, 2,
// 3, then the snapshot — never the snapshot followed by an older entry
// applied on top of state that already includes it. What guarantees this
// is the applier's design (one sender; a snapshot only at the start of a
// pass), not HandleInstallSnapshot's stale check.
func TestInstallSnapshot_DuringApplierBatchDeliversStrictlyForward(t *testing.T) {
	dir := t.TempDir()
	state, err := LoadState(filepath.Join(dir, "raft-state.dat"))
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	log, err := OpenLog(filepath.Join(dir, "raft-log.dat"), filepath.Join(dir, "snapshot.dat"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	applyCh := make(chan ApplyMsg) // unbuffered: the applier blocks until we read
	n := NewNode(1, state, log, map[uint64]Peer{}, applyCh)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 1, Index: 2, Command: []byte("b")},
		&raftpb.LogEntry{Term: 1, Index: 3, Command: []byte("c")},
	)
	startApplierOnly(n)
	t.Cleanup(n.Stop)
	commitThrough(n, 3)

	// Receiving entry 1 proves the applier copied its batch (1..3) and is
	// now blocked sending entry 2 — the interleaving is deterministic.
	delivered := []ApplyMsg{receiveApply(t, applyCh)}

	if _, err := n.HandleInstallSnapshot(snapshotRequest(1, 5, 1, "state-through-5")); err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}
	for i := 0; i < 3; i++ {
		delivered = append(delivered, receiveApply(t, applyCh))
	}

	for i := 1; i < len(delivered); i++ {
		if delivered[i].Index <= delivered[i-1].Index {
			t.Fatalf("delivery went backward: index %d after %d", delivered[i].Index, delivered[i-1].Index)
		}
	}
	last := delivered[len(delivered)-1]
	if last.Snapshot == nil || last.Index != 5 {
		t.Fatalf("last delivery = %+v, want the snapshot at index 5", last)
	}
}

// This test checks the INVARIANT that a follower which fails to store a
// snapshot answers with an error, and so gets no credit from the leader. A
// normal reply would make the leader set matchIndex to 5 for a follower that
// holds nothing — and count it toward a majority for entries 1..5.
func TestInstallSnapshot_FailedStoreGivesLeaderNoCredit(t *testing.T) {
	followerDir := t.TempDir()
	follower, _ := openNodeAtWithID(t, followerDir, 2)
	leader := leaderWithSnapshotAndLaggingPeer(t, follower)

	// A read-only directory makes the snapshot's temp file impossible to
	// create. (Root ignores permissions, so check the failure is real.)
	if err := os.Chmod(followerDir, 0o500); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(followerDir, 0o700) })
	if f, err := os.CreateTemp(followerDir, "probe"); err == nil {
		f.Close()
		t.Skip("directory is still writable (running as root?); cannot simulate a disk failure")
	}

	leader.replicateToPeer(2, leader.peers[2], 1)

	leader.mu.Lock()
	matchIndex, nextIndex := leader.matchIndex[2], leader.nextIndex[2]
	leader.mu.Unlock()
	if matchIndex != 0 || nextIndex != 1 {
		t.Fatalf("leader credited a failed install: matchIndex %d, nextIndex %d; want 0, 1", matchIndex, nextIndex)
	}
	if follower.CommitIndex() != 0 {
		t.Fatalf("follower commitIndex = %d after a failed install, want 0", follower.CommitIndex())
	}
}

// --- leader side: replicateToPeer and sendSnapshot ---

// leaderWithSnapshotAndLaggingPeer returns node 1 as leader of term 1, with
// entries 1..7 of which 1..5 are compacted into a snapshot, and follower as
// peer 2 needing everything from index 1 — entries it can no longer be sent.
func leaderWithSnapshotAndLaggingPeer(t *testing.T, follower *Node) *Node {
	t.Helper()
	leader, _ := openNodeAtWithID(t, t.TempDir(), 1)
	fiveCommandEntries(t, leader)
	seedLog(t, leader,
		&raftpb.LogEntry{Term: 1, Index: 6, Command: []byte("f")},
		&raftpb.LogEntry{Term: 1, Index: 7, Command: []byte("g")},
	)
	commitThrough(leader, 7)
	if err := leader.Snapshot(5, []byte("state-through-5")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	leader.peers = map[uint64]Peer{2: fakePeer{node: follower}}
	makeLeader(t, leader, 1)

	leader.mu.Lock()
	leader.nextIndex[2] = 1 // where backing off on rejections would leave it
	leader.mu.Unlock()
	return leader
}

// This test checks the leader's catch-up path end to end: a follower that
// needs compacted entries is sent the snapshot, the leader resumes ordinary
// replication right after it, and the next round ships the remaining
// entries as log entries.
func TestReplicateToPeer_LaggingFollowerCatchesUpViaSnapshot(t *testing.T) {
	follower, _ := openNodeAtWithID(t, t.TempDir(), 2)
	leader := leaderWithSnapshotAndLaggingPeer(t, follower)

	if acknowledged := leader.replicateToPeer(2, leader.peers[2], 1); !acknowledged {
		t.Fatalf("follower did not acknowledge the leader's term")
	}
	leader.mu.Lock()
	matchIndex, nextIndex := leader.matchIndex[2], leader.nextIndex[2]
	leader.mu.Unlock()
	if matchIndex != 5 || nextIndex != 6 {
		t.Fatalf("after snapshot: matchIndex %d, nextIndex %d; want 5, 6", matchIndex, nextIndex)
	}
	if follower.log.SnapshotIndex() != 5 {
		t.Fatalf("follower SnapshotIndex = %d, want 5", follower.log.SnapshotIndex())
	}

	leader.replicateToPeer(2, leader.peers[2], 1)
	if got := logCommands(follower); !equalStrings(got, []string{"f", "g"}) {
		t.Fatalf("follower log after second round = %v, want [f g]", got)
	}
	leader.mu.Lock()
	defer leader.mu.Unlock()
	if leader.matchIndex[2] != 7 {
		t.Fatalf("matchIndex = %d after second round, want 7", leader.matchIndex[2])
	}
}

// This test checks that a delayed snapshot reply never moves matchIndex
// backward. The follower is already confirmed through 7 when an older
// snapshot exchange (through 5) completes; setting matchIndex to 5 would
// un-replicate entries 6 and 7 in the leader's view and resend them.
func TestSendSnapshot_DelayedReplyDoesNotMoveMatchIndexBackward(t *testing.T) {
	follower, _ := openNodeAtWithID(t, t.TempDir(), 2)
	leader := leaderWithSnapshotAndLaggingPeer(t, follower)
	leader.replicateToPeer(2, leader.peers[2], 1) // snapshot through 5
	leader.replicateToPeer(2, leader.peers[2], 1) // entries 6, 7

	leader.sendSnapshot(2, leader.peers[2], 1, leader.log.Snapshot()) // the late duplicate

	leader.mu.Lock()
	defer leader.mu.Unlock()
	if leader.matchIndex[2] != 7 || leader.nextIndex[2] != 8 {
		t.Fatalf("after a delayed snapshot reply: matchIndex %d, nextIndex %d; want 7, 8", leader.matchIndex[2], leader.nextIndex[2])
	}
}
