package server

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// votingButUnreachablePeer grants every vote but fails every AppendEntries.
// A node whose peers all behave like this wins elections yet can never
// commit anything — the shape of a leader that has just been cut off from
// its followers, which is exactly what the wait-for-apply tests need.
type votingButUnreachablePeer struct{}

func (votingButUnreachablePeer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return &raftpb.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (votingButUnreachablePeer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return nil, errors.New("peer is unreachable")
}

func (votingButUnreachablePeer) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return nil, errors.New("peer is unreachable")
}

// unreachablePeer fails every RPC. A node whose only peer is unreachable can
// never collect a majority (it needs 2 of 2 votes), so it stays a follower.
type unreachablePeer struct{}

func (unreachablePeer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return nil, errors.New("peer is unreachable")
}

func (unreachablePeer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return nil, errors.New("peer is unreachable")
}

func (unreachablePeer) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return nil, errors.New("peer is unreachable")
}

// testNode is one running Raft node plus its state machine and KVServer.
type testNode struct {
	raft   *raft.Node
	sm     *StateMachine
	server *KVServer

	// markStopped records that a test stopped the node itself, so Cleanup
	// doesn't call Stop a second time (Stop is not idempotent).
	markStopped func()
}

// startTestNode builds and starts a node with the given peers. Callers that
// need a leader wait for one with waitForRole.
func startTestNode(t *testing.T, peers map[uint64]raft.Peer) *testNode {
	t.Helper()
	dir := t.TempDir()
	state, err := raft.LoadState(filepath.Join(dir, "raft-state.dat"))
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	log, err := raft.OpenLog(filepath.Join(dir, "raft-log.dat"), filepath.Join(dir, "snapshot.dat"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}

	applyCh := make(chan raft.ApplyMsg)
	sm := NewStateMachine(store.NewStore(), dedup.NewTable(), nil, 0)
	node := raft.NewNode(1, state, log, peers, applyCh)

	runDone := make(chan error, 1)
	go func() { runDone <- sm.Run(applyCh) }()
	node.Start()

	stopped := false
	t.Cleanup(func() {
		if !stopped {
			node.Stop()
		}
		<-runDone // Run exits once the applier closes applyCh
		_ = log.Close()
	})

	return &testNode{
		raft:        node,
		sm:          sm,
		server:      NewKVServer(node, sm),
		markStopped: func() { stopped = true },
	}
}

// startSingleNodeLeader starts a one-node cluster and waits until it has
// elected itself — every write then commits the moment it is proposed.
func startSingleNodeLeader(t *testing.T) *testNode {
	t.Helper()
	tn := startTestNode(t, map[uint64]raft.Peer{})
	waitForRole(t, tn.raft, raft.Leader)
	return tn
}

func waitForRole(t *testing.T, node *raft.Node, want raft.Role) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if node.Role() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node never reached role %v", want)
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("error = %v (code %v), want code %v", err, status.Code(err), want)
	}
}

// This test checks that Get on a never-written key reports found=false
// through the full Raft-backed server.
func TestGet_MissingKey_ReturnsNotFound(t *testing.T) {
	tn := startSingleNodeLeader(t)

	resp, err := tn.server.Get(context.Background(), &kvpb.GetRequest{Key: "missing"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.Found {
		t.Fatalf("Get on missing key: Found = true, want false")
	}
}

// This test checks the full write path end to end: Put proposes, the entry
// commits, the state machine applies it, and only then does Put return —
// so the value is visible to the very next Get.
func TestPutThenGet_ValueVisible(t *testing.T) {
	tn := startSingleNodeLeader(t)
	ctx := context.Background()

	if _, err := tn.server.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "5", ClientId: "c1", RequestId: 1}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp, err := tn.server.Get(ctx, &kvpb.GetRequest{Key: "x"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.Found || resp.Value != "5" {
		t.Fatalf("Get after Put: Value=%q Found=%v, want \"5\" true — Put returned before its write was applied", resp.Value, resp.Found)
	}
}

// This test checks that a retried request is applied once even though it
// now enters the Raft log TWICE — dedup moved into the replicated state
// machine, so the second copy is skipped at apply time. Both calls still
// return OK: the client's write did happen, just once.
func TestPut_DuplicateRequestID_AppliedOnce(t *testing.T) {
	tn := startSingleNodeLeader(t)
	ctx := context.Background()

	if _, err := tn.server.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "first", ClientId: "c1", RequestId: 1}); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if _, err := tn.server.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "second-should-be-ignored", ClientId: "c1", RequestId: 1}); err != nil {
		t.Fatalf("retried Put: %v, want OK — the request was applied once already", err)
	}

	resp, _ := tn.server.Get(ctx, &kvpb.GetRequest{Key: "x"})
	if resp.Value != "first" {
		t.Fatalf("value = %q, want \"first\" — the retry was applied a second time", resp.Value)
	}
}

// This test checks that Delete goes through the same propose-commit-apply
// path as Put.
func TestDelete_RemovesKey(t *testing.T) {
	tn := startSingleNodeLeader(t)
	ctx := context.Background()

	if _, err := tn.server.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "5", ClientId: "c1", RequestId: 1}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := tn.server.Delete(ctx, &kvpb.DeleteRequest{Key: "x", ClientId: "c1", RequestId: 2}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	resp, _ := tn.server.Get(ctx, &kvpb.GetRequest{Key: "x"})
	if resp.Found {
		t.Fatalf("Get after Delete: Found = true, want false")
	}
}

// This test checks that only the leader accepts writes. A follower that
// accepted one could not order it against the leader's writes — the single
// writer is where the log gets its one authoritative order.
func TestPutAndGet_OnFollower_ReturnNotLeader(t *testing.T) {
	// One peer that never answers: this node can never win (it needs 2 of
	// 2 votes), so it stays a follower.
	tn := startTestNode(t, map[uint64]raft.Peer{2: unreachablePeer{}})
	ctx := context.Background()

	_, err := tn.server.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "5", ClientId: "c1", RequestId: 1})
	requireCode(t, err, codes.Unavailable)

	_, err = tn.server.Get(ctx, &kvpb.GetRequest{Key: "x"})
	requireCode(t, err, codes.Unavailable)
}

// This test checks the term-match invariant in replicate: if this node
// loses leadership and a NEW leader's entry is committed at the index we
// proposed at, the handler must report failure — not OK. Our command never
// committed; OK would tell the client a write succeeded that was
// overwritten.
func TestPut_LeadershipLostBeforeCommit_ReturnsErrorNotOK(t *testing.T) {
	tn := startTestNode(t, map[uint64]raft.Peer{
		2: votingButUnreachablePeer{},
		3: votingButUnreachablePeer{},
	})
	waitForRole(t, tn.raft, raft.Leader) // wins, but can never commit anything
	ourTerm, _ := tn.raft.TermAt(1)      // the term of its own no-op

	putResult := make(chan error, 1)
	go func() {
		_, err := tn.server.Put(context.Background(), &kvpb.PutRequest{Key: "x", Value: "mine", ClientId: "c1", RequestId: 1})
		putResult <- err
	}()

	// Wait until our proposal sits at index 2 (index 1 is the no-op).
	waitFor(t, func() bool { _, ok := tn.raft.TermAt(2); return ok })

	// A new leader in a later term overwrites indices 1-2 with its own
	// entries and commits them.
	theirCommand, err := encodeCommand(&kvpb.Command{Op: kvpb.Command_OP_PUT, Key: "x", Value: "theirs", ClientId: "c2", RequestId: 1})
	if err != nil {
		t.Fatalf("encodeCommand: %v", err)
	}
	newTerm := ourTerm + 1
	resp := tn.raft.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: newTerm, LeaderId: 9, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []*raftpb.LogEntry{
			{Term: newTerm, Index: 1, Command: nil},
			{Term: newTerm, Index: 2, Command: theirCommand},
		},
		LeaderCommit: 2,
	})
	if !resp.Success {
		t.Fatalf("setup: new leader's AppendEntries was rejected")
	}

	select {
	case err := <-putResult:
		requireCode(t, err, codes.Unavailable)
	case <-time.After(3 * time.Second):
		t.Fatalf("Put never returned after its index was taken by another leader's entry")
	}

	value, _ := tn.sm.Get("x")
	if value != "theirs" {
		t.Fatalf("x = %q, want \"theirs\" — the committed entry at index 2 is the new leader's", value)
	}
}

// This test checks that a write which cannot commit returns a deadline
// error rather than hanging, and that this error means "outcome unknown":
// the handler does not claim success or definite failure.
func TestPut_CannotCommit_ReturnsDeadlineExceeded(t *testing.T) {
	tn := startTestNode(t, map[uint64]raft.Peer{
		2: votingButUnreachablePeer{},
		3: votingButUnreachablePeer{},
	})
	waitForRole(t, tn.raft, raft.Leader)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := tn.server.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "5", ClientId: "c1", RequestId: 1})
	requireCode(t, err, codes.DeadlineExceeded)
}

// This test checks the shutdown ordering: stopping Raft must release
// handlers stuck waiting for an apply that will now never happen —
// promptly, not after their 5-second applyWaitTimeout. Otherwise every
// in-flight write would stall a graceful shutdown.
func TestPut_NodeStopReleasesWaitingHandlers(t *testing.T) {
	tn := startTestNode(t, map[uint64]raft.Peer{
		2: votingButUnreachablePeer{},
		3: votingButUnreachablePeer{},
	})
	waitForRole(t, tn.raft, raft.Leader)

	putResult := make(chan error, 1)
	go func() {
		_, err := tn.server.Put(context.Background(), &kvpb.PutRequest{Key: "x", Value: "5", ClientId: "c1", RequestId: 1})
		putResult <- err
	}()
	waitFor(t, func() bool { _, ok := tn.raft.TermAt(2); return ok })

	start := time.Now()
	tn.raft.Stop()
	tn.markStopped()

	select {
	case err := <-putResult:
		requireCode(t, err, codes.Unavailable)
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("handler released %v after Stop, want promptly", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("handler still waiting 3s after the node stopped")
	}
}

func waitFor(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition never became true")
}
