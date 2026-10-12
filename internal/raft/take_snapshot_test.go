package raft

import (
	"path/filepath"
	"testing"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// openNodeAt builds a Node over the log and snapshot files in dir, exactly
// as main.go does on startup — so calling it twice on the same dir is a
// restart.
func openNodeAt(t *testing.T, dir string) (*Node, <-chan ApplyMsg) {
	t.Helper()
	return openNodeAtWithID(t, dir, 1)
}

func openNodeAtWithID(t *testing.T, dir string, id uint64) (*Node, <-chan ApplyMsg) {
	t.Helper()
	state, err := LoadState(filepath.Join(dir, "raft-state.dat"))
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	log, err := OpenLog(filepath.Join(dir, "raft-log.dat"), filepath.Join(dir, "snapshot.dat"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	applyCh := make(chan ApplyMsg, 1024)
	return NewNode(id, state, log, map[uint64]Peer{}, applyCh), applyCh
}

func commitThrough(n *Node, index uint64) {
	n.mu.Lock()
	n.commitIndex = index
	n.signalApplierLocked()
	n.mu.Unlock()
}

// This test checks that Node.Snapshot compacts the log and persists the
// snapshot: after a restart from the same files, the snapshot's data and
// boundary are still there and the covered entries are gone.
func TestNodeSnapshot_CompactsLogAndPersists(t *testing.T) {
	dir := t.TempDir()
	n, _ := openNodeAt(t, dir)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 1, Index: 2, Command: []byte("b")},
		&raftpb.LogEntry{Term: 2, Index: 3, Command: []byte("c")},
	)
	commitThrough(n, 3)

	if err := n.Snapshot(2, []byte("state-after-2")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if n.log.SnapshotIndex() != 2 {
		t.Fatalf("SnapshotIndex() = %d, want 2", n.log.SnapshotIndex())
	}

	restarted, _ := openNodeAt(t, dir)
	snapshot := restarted.log.Snapshot()
	if snapshot.LastIncludedIndex != 2 || snapshot.LastIncludedTerm != 1 || string(snapshot.Data) != "state-after-2" {
		t.Fatalf("snapshot after restart = (%d, %d, %q), want (2, 1, \"state-after-2\")",
			snapshot.LastIncludedIndex, snapshot.LastIncludedTerm, snapshot.Data)
	}
	if _, ok := restarted.log.EntriesFrom(2); ok {
		t.Fatalf("entry 2 still in the log after it was snapshotted")
	}
}

// This test checks that a node refuses to snapshot beyond its commit index:
// only applied — hence committed — state may be snapshotted. Snapshotting an
// uncommitted entry would make it permanent on this node even though a new
// leader could legitimately replace it.
func TestNodeSnapshot_RefusesBeyondCommitIndex(t *testing.T) {
	n, _ := openNodeAt(t, t.TempDir())
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 1, Index: 2, Command: []byte("b")},
	)
	commitThrough(n, 1)

	if err := n.Snapshot(2, []byte("x")); err == nil {
		t.Fatalf("Snapshot(2) with commitIndex 1 succeeded")
	}
	if n.log.SnapshotIndex() != 0 {
		t.Fatalf("log was compacted despite the refusal")
	}
}

// This test checks that a restarted node's commit index starts at its
// snapshot. A snapshot holds only committed entries; starting at 0 would
// leave the snapshot undelivered — and the store empty — until a new
// leader's commit happened to pass it.
func TestNewNode_CommitIndexStartsAtSnapshot(t *testing.T) {
	dir := t.TempDir()
	n, _ := openNodeAt(t, dir)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 1, Index: 2, Command: []byte("b")},
	)
	commitThrough(n, 2)
	if err := n.Snapshot(2, []byte("s")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restarted, _ := openNodeAt(t, dir)
	if restarted.CommitIndex() != 2 {
		t.Fatalf("restarted CommitIndex() = %d, want 2 (the snapshot index)", restarted.CommitIndex())
	}
}

// This test checks the restart path end to end: the applier must deliver
// the snapshot FIRST, then exactly the entries after it, each once and in
// order. This is what bounds restart cost — snapshot plus suffix instead of
// the whole history.
func TestApplier_RestartDeliversSnapshotThenEntries(t *testing.T) {
	dir := t.TempDir()
	n, _ := openNodeAt(t, dir)
	for i := uint64(1); i <= 5; i++ {
		seedLog(t, n, &raftpb.LogEntry{Term: 1, Index: i, Command: []byte{byte('a' + i - 1)}})
	}
	commitThrough(n, 5)
	if err := n.Snapshot(3, []byte("state-after-3")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restarted, applyCh := openNodeAt(t, dir)
	startApplierOnly(restarted)
	defer restarted.Stop()

	first := receiveApply(t, applyCh)
	if first.Snapshot == nil || first.Index != 3 || string(first.Snapshot.Data) != "state-after-3" {
		t.Fatalf("first delivery = index %d snapshot=%v, want the snapshot at 3", first.Index, first.Snapshot != nil)
	}
	assertNothingApplied(t, applyCh) // 4 and 5 are not known committed after restart

	commitThrough(restarted, 5)
	for _, want := range []struct {
		index   uint64
		command string
	}{{4, "d"}, {5, "e"}} {
		msg := receiveApply(t, applyCh)
		if msg.Snapshot != nil || msg.Index != want.index || string(msg.Command) != want.command {
			t.Fatalf("delivered index %d %q (snapshot=%v), want index %d %q",
				msg.Index, msg.Command, msg.Snapshot != nil, want.index, want.command)
		}
	}
	assertNothingApplied(t, applyCh) // and nothing twice
}

// This test checks the applier's re-arm after delivering a snapshot. On
// restart, a leader's AppendEntries can raise commitIndex past the snapshot
// BEFORE the applier's first pass. That commit leaves exactly one wakeup
// token, and the first pass spends it sending the snapshot alone. The
// entries after the snapshot are committed, but no new commit is coming to
// wake the applier again. Without the re-arm they would sit unapplied until
// some unrelated future write — in a quiet cluster, indefinitely, and any
// ReadIndex read waiting for them would time out.
func TestApplier_EntriesCommittedBeforeSnapshotDeliveryAreStillApplied(t *testing.T) {
	dir := t.TempDir()
	n, _ := openNodeAt(t, dir)
	for i := uint64(1); i <= 5; i++ {
		seedLog(t, n, &raftpb.LogEntry{Term: 1, Index: i, Command: []byte{byte('a' + i - 1)}})
	}
	commitThrough(n, 5)
	if err := n.Snapshot(3, []byte("state-after-3")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restarted, applyCh := openNodeAt(t, dir)
	commitThrough(restarted, 5) // the leader's commit arrives before the applier ever runs
	startApplierOnly(restarted)
	defer restarted.Stop()

	if first := receiveApply(t, applyCh); first.Snapshot == nil || first.Index != 3 {
		t.Fatalf("first delivery = index %d (snapshot=%v), want the snapshot at 3", first.Index, first.Snapshot != nil)
	}
	// No further commitThrough: entries 4 and 5 must arrive on their own.
	for _, want := range []uint64{4, 5} {
		if msg := receiveApply(t, applyCh); msg.Index != want {
			t.Fatalf("delivered index %d, want %d", msg.Index, want)
		}
	}
}
