package server

import (
	"context"
	"fmt"
	"os"
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
// A threshold of 0 turns snapshots off.
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
	var snapshotter Snapshotter // nil interface, not a nil *raft.Node, when off
	if thresholdBytes > 0 {
		snapshotter = node
	}
	sm := NewStateMachine(store.NewStore(), dedup.NewTable(), snapshotter, thresholdBytes)
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

// This test checks that EVERY acknowledged write survives a restart, not
// just the latest value of a few hot keys: each of 300 writes goes to its
// own key, so a write lost anywhere — before the snapshot, or in the log
// suffix after it — shows up as a missing key. It is CLAUDE.md's Milestone 8
// break 2 ("restore from a snapshot without replaying the remaining log")
// turned into a permanent guard; on failure it names exactly what was lost.
func TestSnapshots_EveryAcknowledgedWriteSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	const threshold = 4 << 10
	const writes = 300
	node := startSnapshottingNode(t, dir, threshold)
	ctx := context.Background()
	for i := 1; i <= writes; i++ {
		put := &kvpb.PutRequest{Key: fmt.Sprintf("key-%03d", i), Value: fmt.Sprint(i), ClientId: "c1", RequestId: uint64(i)}
		if _, err := node.server.Put(ctx, put); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	node.stop()

	snapshot, err := raft.LoadSnapshot(filepath.Join(dir, "snapshot.dat"))
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snapshot.LastIncludedIndex == 0 {
		t.Fatalf("setup: no snapshot was taken, so this restart does not exercise snapshot + log suffix")
	}

	restarted := startSnapshottingNode(t, dir, threshold)
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// A linearizable read first, so the restarted node has applied
	// everything it knows about before the store is inspected directly.
	if _, err := restarted.server.Get(waitCtx, &kvpb.GetRequest{Key: "key-001"}); err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	var lost []string
	for i := 1; i <= writes; i++ {
		key := fmt.Sprintf("key-%03d", i)
		if v, _ := restarted.sm.Get(key); v != fmt.Sprint(i) {
			lost = append(lost, key)
		}
	}
	if len(lost) > 0 {
		t.Fatalf("%d of %d acknowledged writes lost after restart (snapshot at raft index %d): first %s, last %s",
			len(lost), writes, snapshot.LastIncludedIndex, lost[0], lost[len(lost)-1])
	}
}

// --- end to end: a follower that missed compacted entries ---

// putToCurrentLeader retries a Put against every node until one accepts it,
// as the real client does. Needed after healing a partition: the healed
// node campaigned while cut off, and its higher term deposes the leader
// (there is no PreVote), so leadership may move. Retrying with the SAME
// request ID is safe — that is what the dedup table is for.
func putToCurrentLeader(t *testing.T, nodes []*testNode, key, value, clientID string, requestID uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, tn := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := tn.server.Put(ctx, &kvpb.PutRequest{Key: key, Value: value, ClientId: clientID, RequestId: requestID})
			cancel()
			if err == nil {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("Put(%s=%s) was not accepted by any node within 5s", key, value)
}

// This test checks the reason InstallSnapshot exists, on a real 3-node
// cluster: a follower is cut off while the leader writes enough to compact
// away everything the follower is missing. Once healed, ordinary log
// entries can no longer catch it up — only the snapshot can. Afterwards it
// must hold the same state as the others, INCLUDING the dedup table: a
// retry of a request applied before the snapshot must be skipped on the
// caught-up follower too, or that one replica silently diverges.
func TestInstallSnapshot_IsolatedFollowerCatchesUpAfterLeaderCompacts(t *testing.T) {
	const threshold = 2 << 10 // 2 KiB — compaction after ~30 writes
	nodes, network := startSnapshottingCluster(t, 3, threshold)
	leader := waitForLeader(t, nodes)

	put(t, leader, "x", "a", "client-1", 7) // the request a retry will repeat later
	var follower *testNode
	for _, tn := range nodes {
		if tn != leader {
			follower = tn
			break
		}
	}
	waitApplied(t, []*testNode{follower}, "x", "a")
	network.isolate(follower.raft.ID())

	// The leader and the other follower are still a majority.
	for i := 1; i <= 200; i++ {
		put(t, leader, fmt.Sprintf("key-%03d", i), fmt.Sprint(i), "client-2", uint64(i))
	}
	put(t, leader, "x", "b", "client-2", 201)
	if _, ok := leader.raft.TermAt(2); ok {
		t.Fatalf("setup: leader never compacted entry 2 — the follower could catch up without a snapshot")
	}

	network.heal(follower.raft.ID())
	waitApplied(t, nodes, "x", "b")
	waitApplied(t, nodes, "key-200", "200")
	if _, ok := follower.raft.TermAt(2); ok {
		t.Fatalf("follower still holds entry 2 — it caught up through log entries, not the snapshot")
	}

	// client-1's request 7 is retried. Every replica must skip it.
	putToCurrentLeader(t, nodes, "x", "a", "client-1", 7)
	putToCurrentLeader(t, nodes, "fence", "done", "client-3", 1) // committed after the retry
	waitApplied(t, nodes, "fence", "done")
	for _, tn := range nodes {
		if v, _ := tn.sm.Get("x"); v != "b" {
			t.Fatalf("node %d has x = %q after the retry, want \"b\" — the retry was re-applied", tn.raft.ID(), v)
		}
	}
}

// --- CLAUDE.md Milestone 8 break 1: never compact ---

// fileSize returns path's size, or 0 if it does not exist.
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatalf("Stat(%s): %v", path, err)
	}
	return info.Size()
}

// This is CLAUDE.md's break, "never trigger compaction: run 100k writes and
// observe the log size," as a measurement rather than a pass/fail check. It
// runs the same 100k writes over 1,000 keys with compaction off and on, and
// reports disk usage and restart time for each. The live state is the same
// 1,000 keys either way; only the history kept on disk differs.
//
// Opt-in, because 100k fsynced writes take minutes:
//
//	KVSTORE_LONG=1 go test -run TestBreak_NeverCompact -v -timeout 60m ./internal/server/
func TestBreak_NeverCompact(t *testing.T) {
	if os.Getenv("KVSTORE_LONG") == "" {
		t.Skip("set KVSTORE_LONG=1 to run the 100k-write compaction measurement")
	}
	const writes, keys = 100_000, 1_000

	for _, mode := range []struct {
		name      string
		threshold int64
	}{
		{"compaction off", 0},
		{"compaction on (1 MiB default)", 1 << 20},
	} {
		dir := t.TempDir()
		node := startSnapshottingNode(t, dir, mode.threshold)
		ctx := context.Background()
		started := time.Now()
		for i := 0; i < writes; i++ {
			key := fmt.Sprintf("key-%04d", i%keys)
			if _, err := node.server.Put(ctx, &kvpb.PutRequest{Key: key, Value: fmt.Sprint(i), ClientId: "c1", RequestId: uint64(i + 1)}); err != nil {
				t.Fatalf("%s: Put %d: %v", mode.name, i, err)
			}
		}
		writeTime := time.Since(started)
		node.stop()

		logBytes := fileSize(t, filepath.Join(dir, "raft-log.dat"))
		snapshotBytes := fileSize(t, filepath.Join(dir, "snapshot.dat"))

		// Restart time: until a read of the LAST key returns its last value.
		restartStarted := time.Now()
		restarted := startSnapshottingNode(t, dir, mode.threshold)
		lastKey := fmt.Sprintf("key-%04d", (writes-1)%keys)
		waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
		resp, err := restarted.server.Get(waitCtx, &kvpb.GetRequest{Key: lastKey})
		cancel()
		if err != nil || resp.Value != fmt.Sprint(writes-1) {
			t.Fatalf("%s: after restart Get(%s) = %v, %v; want %d", mode.name, lastKey, resp, err, writes-1)
		}
		restartTime := time.Since(restartStarted)
		restarted.stop()

		t.Logf("%-30s log %9d B  snapshot %7d B  total %9d B  | %d writes in %v  | restart %v",
			mode.name, logBytes, snapshotBytes, logBytes+snapshotBytes, writes, writeTime.Round(time.Second), restartTime.Round(time.Millisecond))
	}
}
