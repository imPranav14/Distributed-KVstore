package server

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/ring"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// newIdleRaftNode builds a Raft node that is never started: no timers, no
// applier. Its handlers can be called directly and its state inspected.
func newIdleRaftNode(t *testing.T) *raft.Node {
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
	t.Cleanup(func() { _ = log.Close() })
	return raft.NewNode(1, state, log, map[uint64]raft.Peer{}, make(chan raft.ApplyMsg, 16))
}

// currentTerm reads a node's term through its public surface: a heartbeat
// from term 0 is always stale, so it changes nothing and the reply carries
// the node's current term.
func currentTerm(node *raft.Node) uint64 {
	return node.HandleAppendEntries(&raftpb.AppendEntriesRequest{Term: 0}).Term
}

// This test checks the INVARIANT that a Raft message is handled only by the
// group it names — for all three RPCs. Two groups in one process must never
// see each other's traffic: an entry for group 2 landing in group 1's log
// would mix two independent logs, and nothing downstream could detect it.
func TestRaftServer_RoutesEachRPCToItsOwnGroup(t *testing.T) {
	group1, group2 := newIdleRaftNode(t), newIdleRaftNode(t)
	raftServer, err := NewRaftServer(map[ring.GroupID]*raft.Node{1: group1, 2: group2})
	if err != nil {
		t.Fatalf("NewRaftServer: %v", err)
	}
	ctx := context.Background()

	resp, err := raftServer.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		GroupId: 2, Term: 1, LeaderId: 2,
		Entries: []*raftpb.LogEntry{{Term: 1, Index: 1, Command: []byte("for-group-2")}},
	})
	if err != nil || !resp.Success {
		t.Fatalf("AppendEntries for group 2: resp %v, err %v", resp, err)
	}
	if _, ok := group2.TermAt(1); !ok {
		t.Fatalf("group 2 did not receive its own entry")
	}
	if _, ok := group1.TermAt(1); ok {
		t.Fatalf("group 1's log received an entry addressed to group 2")
	}

	if _, err := raftServer.RequestVote(ctx, &raftpb.RequestVoteRequest{GroupId: 1, Term: 5, CandidateId: 2}); err != nil {
		t.Fatalf("RequestVote for group 1: %v", err)
	}
	if currentTerm(group1) != 5 || currentTerm(group2) != 1 {
		t.Fatalf("after a group 1 vote request at term 5: terms are %d and %d, want 5 and 1",
			currentTerm(group1), currentTerm(group2))
	}

	if _, err := raftServer.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		GroupId: 1, Term: 5, LeaderId: 2, LastIncludedIndex: 10, LastIncludedTerm: 5, Data: []byte("state"),
	}); err != nil {
		t.Fatalf("InstallSnapshot for group 1: %v", err)
	}
	if group1.CommitIndex() != 10 || group2.CommitIndex() != 0 {
		t.Fatalf("after a group 1 snapshot: commit indexes are %d and %d, want 10 and 0",
			group1.CommitIndex(), group2.CommitIndex())
	}
}

// This test checks that a request naming a group this process does not host
// is refused and touches nothing. Group 0 is the important case: it is what
// an unset group_id reads as, so a sender that forgot to set it — or a
// binary from before sharding — must be refused, not handled by some group.
func TestRaftServer_RejectsUnknownOrMissingGroup(t *testing.T) {
	group1 := newIdleRaftNode(t)
	raftServer, err := NewRaftServer(map[ring.GroupID]*raft.Node{1: group1})
	if err != nil {
		t.Fatalf("NewRaftServer: %v", err)
	}
	ctx := context.Background()

	for _, group := range []uint64{0, 7} {
		_, err := raftServer.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
			GroupId: group, Term: 3, LeaderId: 2,
			Entries: []*raftpb.LogEntry{{Term: 3, Index: 1, Command: []byte("x")}},
		})
		requireCode(t, err, codes.NotFound)
		_, err = raftServer.RequestVote(ctx, &raftpb.RequestVoteRequest{GroupId: group, Term: 3, CandidateId: 2})
		requireCode(t, err, codes.NotFound)
		_, err = raftServer.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{GroupId: group, Term: 3, LeaderId: 2, LastIncludedIndex: 4, LastIncludedTerm: 3})
		requireCode(t, err, codes.NotFound)
	}

	if currentTerm(group1) != 0 || group1.CommitIndex() != 0 {
		t.Fatalf("a refused request changed group 1: term %d, commit index %d", currentTerm(group1), group1.CommitIndex())
	}
	if _, ok := group1.TermAt(1); ok {
		t.Fatalf("a refused request wrote to group 1's log")
	}
}

// This test checks that a dispatcher can never be built with group 0 (unset
// fields would match it) or with no groups at all.
func TestNewRaftServer_RefusesGroupZeroAndNoGroups(t *testing.T) {
	if _, err := NewRaftServer(map[ring.GroupID]*raft.Node{0: newIdleRaftNode(t)}); err == nil {
		t.Fatalf("NewRaftServer accepted group 0")
	}
	if _, err := NewRaftServer(map[ring.GroupID]*raft.Node{}); err == nil {
		t.Fatalf("NewRaftServer accepted no groups")
	}
}

// This test checks the INVARIANT behind RaftServer having no mutex: its
// group map is read-only after construction. The caller's map is copied, so
// changing it later — which, while handlers run, would be a data race — has
// no effect on routing.
func TestNewRaftServer_IsUnaffectedByLaterChangesToTheCallersMap(t *testing.T) {
	group1 := newIdleRaftNode(t)
	nodes := map[ring.GroupID]*raft.Node{1: group1}
	raftServer, err := NewRaftServer(nodes)
	if err != nil {
		t.Fatalf("NewRaftServer: %v", err)
	}

	delete(nodes, 1)
	nodes[7] = newIdleRaftNode(t)

	if _, err := raftServer.RequestVote(context.Background(), &raftpb.RequestVoteRequest{GroupId: 1, Term: 2, CandidateId: 2}); err != nil {
		t.Fatalf("group 1 stopped being routed after the caller's map changed: %v", err)
	}
	_, err = raftServer.RequestVote(context.Background(), &raftpb.RequestVoteRequest{GroupId: 7, Term: 2, CandidateId: 2})
	requireCode(t, err, codes.NotFound)
}

// recordingRaftStub stands in for the generated gRPC client and records the
// group ID on every request it is handed.
type recordingRaftStub struct {
	mu     sync.Mutex
	groups []uint64
}

func (s *recordingRaftStub) record(group uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups = append(s.groups, group)
}

func (s *recordingRaftStub) RequestVote(ctx context.Context, in *raftpb.RequestVoteRequest, opts ...grpc.CallOption) (*raftpb.RequestVoteResponse, error) {
	s.record(in.GroupId)
	return &raftpb.RequestVoteResponse{}, nil
}

func (s *recordingRaftStub) AppendEntries(ctx context.Context, in *raftpb.AppendEntriesRequest, opts ...grpc.CallOption) (*raftpb.AppendEntriesResponse, error) {
	s.record(in.GroupId)
	return &raftpb.AppendEntriesResponse{}, nil
}

func (s *recordingRaftStub) InstallSnapshot(ctx context.Context, in *raftpb.InstallSnapshotRequest, opts ...grpc.CallOption) (*raftpb.InstallSnapshotResponse, error) {
	s.record(in.GroupId)
	return &raftpb.InstallSnapshotResponse{}, nil
}

// This test checks the sending half of the invariant: every request a group
// sends names that group. Raft itself never sets group_id, so if the peer
// did not stamp it, every message would arrive as group 0 and be refused.
func TestGroupPeer_StampsItsGroupOnEveryRPC(t *testing.T) {
	stub := &recordingRaftStub{}
	peer := &groupPeer{stub: stub, group: 3}
	ctx := context.Background()

	_, _ = peer.RequestVote(ctx, &raftpb.RequestVoteRequest{})
	_, _ = peer.AppendEntries(ctx, &raftpb.AppendEntriesRequest{})
	_, _ = peer.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{})

	if len(stub.groups) != 3 || stub.groups[0] != 3 || stub.groups[1] != 3 || stub.groups[2] != 3 {
		t.Fatalf("group IDs sent = %v, want [3 3 3]", stub.groups)
	}
}

// newGroupReplica builds one replica of one group — Raft node, state machine
// and KVServer — wired to the given peers. Nothing runs until start is
// called, so every process can be wired before any election begins.
func newGroupReplica(t *testing.T, nodeID uint64, peers map[uint64]raft.Peer) (replica *testNode, start func()) {
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
	node := raft.NewNode(nodeID, state, log, peers, applyCh)
	sm := NewStateMachine(store.NewStore(), dedup.NewTable(), nil, 0)

	runDone := make(chan struct{})
	started := false
	t.Cleanup(func() {
		if started {
			node.Stop()
			<-runDone
		}
		_ = log.Close()
	})
	start = func() {
		started = true
		// Exit (Rule 5): Run returns once node.Stop() makes the applier
		// close applyCh.
		go func() {
			defer close(runDone)
			_ = sm.Run(applyCh)
		}()
		node.Start()
	}
	return &testNode{raft: node, sm: sm, server: NewKVServer(node, sm)}, start
}

// This test checks both halves of the invariant together, over real gRPC:
// three processes on localhost, each hosting groups 1 and 2 behind ONE port,
// with each pair of processes sharing ONE connection for both groups. Each
// group must elect its own leader and replicate only its own writes — group
// 1's replicas end with x=one and group 2's with x=two, never a mix.
func TestTwoGroupsOverRealGRPC_ShareConnectionsWithoutMixing(t *testing.T) {
	const nodeCount = 3
	groups := []ring.GroupID{1, 2}

	listeners := make([]net.Listener, nodeCount)
	for i := range listeners {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Listen: %v", err)
		}
		listeners[i] = listener
	}

	replicas := map[ring.GroupID][]*testNode{}
	var starts []func()
	for i := 0; i < nodeCount; i++ {
		nodeID := uint64(i + 1)

		connections := map[uint64]*RaftPeerConn{}
		for j := 0; j < nodeCount; j++ {
			if j == i {
				continue
			}
			connection, err := DialRaftPeer(listeners[j].Addr().String())
			if err != nil {
				t.Fatalf("DialRaftPeer: %v", err)
			}
			t.Cleanup(func() { _ = connection.Close() })
			connections[uint64(j+1)] = connection
		}

		hosted := map[ring.GroupID]*raft.Node{}
		for _, group := range groups {
			peers := map[uint64]raft.Peer{}
			for peerID, connection := range connections {
				peers[peerID] = connection.ForGroup(group)
			}
			replica, start := newGroupReplica(t, nodeID, peers)
			hosted[group] = replica.raft
			replicas[group] = append(replicas[group], replica)
			starts = append(starts, start)
		}

		raftServer, err := NewRaftServer(hosted)
		if err != nil {
			t.Fatalf("NewRaftServer: %v", err)
		}
		grpcServer := grpc.NewServer()
		raftpb.RegisterRaftServer(grpcServer, raftServer)
		// Exit (Rule 5): Serve returns once Stop runs in cleanup — which,
		// registered after the replicas, runs before they are stopped.
		go func() { _ = grpcServer.Serve(listeners[i]) }()
		t.Cleanup(grpcServer.Stop)
	}

	for _, start := range starts {
		start()
	}

	leader1 := waitForLeader(t, replicas[1])
	leader2 := waitForLeader(t, replicas[2])
	put(t, leader1, "x", "one", "client-1", 1)
	put(t, leader2, "x", "two", "client-2", 1)

	waitApplied(t, replicas[1], "x", "one")
	waitApplied(t, replicas[2], "x", "two")
}
