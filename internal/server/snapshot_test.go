package server

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
)

// snapshotOf takes the state machine's snapshot the way Raft would deliver
// it back: as an ApplyMsg at its current lastApplied.
func snapshotOf(t *testing.T, sm *StateMachine) raft.ApplyMsg {
	t.Helper()
	data, err := sm.encodeSnapshot()
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	index := sm.LastApplied()
	return raft.ApplyMsg{
		Index:    index,
		Term:     1,
		Snapshot: &raft.Snapshot{LastIncludedIndex: index, LastIncludedTerm: 1, Data: data},
	}
}

// This test checks that a snapshot round trip reproduces the store exactly
// — including keys that were deleted, which must stay absent — and that
// lastApplied jumps to the snapshot's index.
func TestStateMachine_SnapshotRoundTripReproducesState(t *testing.T) {
	original := newTestStateMachine()
	applyAll(t, original,
		cmdMsg(t, 1, kvpb.Command_OP_PUT, "x", "1", "c1", 1),
		cmdMsg(t, 2, kvpb.Command_OP_PUT, "y", "2", "c1", 2),
		cmdMsg(t, 3, kvpb.Command_OP_DELETE, "x", "", "c1", 3),
	)

	restored := newTestStateMachine()
	if err := restored.apply(snapshotOf(t, original)); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}

	if _, found := restored.Get("x"); found {
		t.Fatalf("deleted key x reappeared after restore")
	}
	if v, _ := restored.Get("y"); v != "2" {
		t.Fatalf("y = %q after restore, want \"2\"", v)
	}
	if restored.LastApplied() != 3 {
		t.Fatalf("LastApplied() = %d after restore, want 3", restored.LastApplied())
	}
}

// This test is Milestone 7's interview Q4, built for real. A request's first
// copy is applied BEFORE the snapshot point; its retried copy sits in the
// log AFTER it, with another client's write in between. A replica restored
// from the snapshot must still skip the retry — which it can only do if the
// dedup table travelled inside the snapshot. Without it, the restored
// replica alone re-applies x=a, and the cluster silently diverges.
func TestStateMachine_RetryStraddlingSnapshotIsStillSkipped(t *testing.T) {
	replicaA := newTestStateMachine()
	applyAll(t, replicaA,
		cmdMsg(t, 1, kvpb.Command_OP_PUT, "x", "a", "client-1", 7),
		cmdMsg(t, 2, kvpb.Command_OP_PUT, "x", "b", "client-2", 1),
	)

	// Replica B is a fresh node restored from A's snapshot at index 2.
	replicaB := newTestStateMachine()
	if err := replicaB.apply(snapshotOf(t, replicaA)); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}

	// Both now apply the retry of client-1's request 7.
	retry := cmdMsg(t, 3, kvpb.Command_OP_PUT, "x", "a", "client-1", 7)
	applyAll(t, replicaA, retry)
	applyAll(t, replicaB, retry)

	a, _ := replicaA.Get("x")
	b, _ := replicaB.Get("x")
	if a != "b" || b != "b" {
		t.Fatalf("x = %q on the original replica, %q on the restored one; want \"b\" on both — the replicas diverged", a, b)
	}
}

// This test checks that a snapshot can never move state backward: one at or
// below lastApplied would undo writes clients were told succeeded.
func TestStateMachine_RefusesSnapshotThatMovesBackward(t *testing.T) {
	sm := newTestStateMachine()
	applyAll(t, sm, cmdMsg(t, 1, kvpb.Command_OP_PUT, "x", "old", "c1", 1))
	stale := snapshotOf(t, sm) // snapshot at index 1
	applyAll(t, sm, cmdMsg(t, 2, kvpb.Command_OP_PUT, "x", "new", "c1", 2))

	if err := sm.apply(stale); err == nil {
		t.Fatalf("applied a snapshot at index 1 after index 2 — state moved backward")
	}
	if v, _ := sm.Get("x"); v != "new" {
		t.Fatalf("x = %q, want \"new\" unchanged", v)
	}
}

// modelLogSnapshotter is a fake Raft node that models the real log's size:
// it grows by bytesPerEntry for every entry applied since the last
// snapshot, and drops back once a snapshot compacts them away. Driving the
// size from the state machine's own progress — rather than changing it from
// the test goroutine — makes the trigger point deterministic.
type modelLogSnapshotter struct {
	mu                sync.Mutex
	sm                *StateMachine
	bytesPerEntry     int64
	lastSnapshotIndex uint64
	calls             []snapshotCall
}

type snapshotCall struct {
	index uint64
	data  []byte
}

func (m *modelLogSnapshotter) LogSize() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(m.sm.LastApplied()-m.lastSnapshotIndex) * m.bytesPerEntry
}

func (m *modelLogSnapshotter) Snapshot(index uint64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, snapshotCall{index, data})
	m.lastSnapshotIndex = index // compaction: the log shrinks back
	return nil
}

// This test checks the trigger: a snapshot is taken after an apply exactly
// when the log passes the threshold, at exactly lastApplied, and its data
// decodes to the state at that point. With 400 bytes per entry and a
// 1000-byte threshold, the log is 400, 800, then 1200 bytes after entries
// 1, 2, 3 — so exactly one snapshot, at index 3, then the count starts over.
func TestStateMachine_SnapshotsWhenLogPassesThreshold(t *testing.T) {
	fake := &modelLogSnapshotter{bytesPerEntry: 400}
	sm := NewStateMachine(store.NewStore(), dedup.NewTable(), fake, 1000)
	fake.sm = sm

	applyCh := make(chan raft.ApplyMsg)
	done := make(chan error, 1)
	go func() { done <- sm.Run(applyCh) }()
	for i := uint64(1); i <= 5; i++ {
		applyCh <- cmdMsg(t, i, kvpb.Command_OP_PUT, fmt.Sprintf("k%d", i), fmt.Sprint(i), "c1", i)
	}
	close(applyCh)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.calls) != 1 || fake.calls[0].index != 3 {
		indices := make([]uint64, len(fake.calls))
		for i, c := range fake.calls {
			indices[i] = c.index
		}
		t.Fatalf("snapshots taken at %v, want exactly [3]", indices)
	}

	restored := newTestStateMachine()
	if err := restored.apply(raft.ApplyMsg{Index: 3, Snapshot: &raft.Snapshot{LastIncludedIndex: 3, Data: fake.calls[0].data}}); err != nil {
		t.Fatalf("restoring the taken snapshot: %v", err)
	}
	if v, _ := restored.Get("k3"); v != "3" {
		t.Fatalf("snapshot taken after index 3 has k3 = %q, want \"3\"", v)
	}
	if _, found := restored.Get("k4"); found {
		t.Fatalf("snapshot taken after index 3 contains k4, applied later — it is not a consistent cut")
	}
}

// --- end to end: bounded disk, and restart from a snapshot ---

// snapshottingNode is a real single-node Raft cluster over dir, whose state
// machine snapshots through the node itself — main.go's wiring, in-process.
type snapshottingNode struct {
	raft   *raft.Node
	sm     *StateMachine
	server *KVServer
	stop   func()
}

func startSnapshottingNode(t *testing.T, dir string, thresholdBytes int64) *snapshottingNode {
	t.Helper()
	state, err := raft.LoadState(filepath.Join(dir, "raft-state.dat"))
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	log, err := raft.OpenLog(filepath.Join(dir, "raft-log.dat"), filepath.Join(dir, "snapshot.dat"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}

	applyCh := make(chan raft.ApplyMsg)
	node := raft.NewNode(1, state, log, map[uint64]raft.Peer{}, applyCh)
	sm := NewStateMachine(store.NewStore(), dedup.NewTable(), node, thresholdBytes)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = sm.Run(applyCh)
	}()
	node.Start()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			node.Stop()
			<-runDone
			_ = log.Close()
		})
	}
	t.Cleanup(stop)
	waitForRole(t, node, raft.Leader)
	return &snapshottingNode{raft: node, sm: sm, server: NewKVServer(node, sm), stop: stop}
}

// This test checks the promise of the whole milestone: with snapshots on,
// the log's size is bounded by the threshold rather than by the number of
// writes — and a node restarted from its data directory comes back with
// every value, rebuilt from snapshot plus log suffix.
func TestSnapshots_BoundLogSizeAndSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	const threshold = 4 << 10 // 4 KiB
	node := startSnapshottingNode(t, dir, threshold)
	ctx := context.Background()

	const writes, keys = 2000, 20
	var largestLog int64
	for i := 0; i < writes; i++ {
		key := fmt.Sprintf("key-%02d", i%keys)
		if _, err := node.server.Put(ctx, &kvpb.PutRequest{Key: key, Value: fmt.Sprint(i), ClientId: "c1", RequestId: uint64(i + 1)}); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		if size := node.raft.LogSize(); size > largestLog {
			largestLog = size
		}
	}

	// Each entry is ~70 bytes, so 2000 writes would be ~140 KiB unbounded.
	// Snapshots fire after an apply once the log passes 4 KiB, so the log
	// should never get much past one threshold's worth.
	if largestLog > 2*threshold {
		t.Fatalf("log reached %d bytes over %d writes with a %d-byte threshold — compaction is not bounding it", largestLog, writes, threshold)
	}
	t.Logf("log peaked at %d bytes across %d writes (threshold %d)", largestLog, writes, threshold)

	// Restart from the same directory.
	node.stop()
	restarted := startSnapshottingNode(t, dir, threshold)

	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%02d", k)
		want := fmt.Sprint(writes - keys + k) // the last write to this key
		resp, err := restarted.server.Get(waitCtx, &kvpb.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("Get(%s) after restart: %v", key, err)
		}
		if resp.Value != want {
			t.Fatalf("%s = %q after restart, want %q", key, resp.Value, want)
		}
	}
}
