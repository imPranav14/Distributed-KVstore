package server

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// testNetwork connects in-process nodes and can cut any node off from all
// the others — a partition the test controls exactly, which Docker cannot
// give us reliably.
type testNetwork struct {
	mu       sync.Mutex
	isolated map[uint64]bool
}

func (tn *testNetwork) isolate(id uint64) {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	tn.isolated[id] = true
}

func (tn *testNetwork) blocked(from, to uint64) bool {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	return tn.isolated[from] || tn.isolated[to]
}

var errPartitioned = errors.New("partitioned")

// networkPeer is node `from`'s view of node `to`, routed through testNetwork.
type networkPeer struct {
	network  *testNetwork
	from, to uint64
	target   *raft.Node
}

func (p *networkPeer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	if p.network.blocked(p.from, p.to) {
		return nil, errPartitioned
	}
	return p.target.HandleRequestVote(req), nil
}

func (p *networkPeer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	if p.network.blocked(p.from, p.to) {
		return nil, errPartitioned
	}
	return p.target.HandleAppendEntries(req), nil
}

// startTestCluster starts n fully wired nodes, each with its own state
// machine and KVServer.
func startTestCluster(t *testing.T, n int) ([]*testNode, *testNetwork) {
	t.Helper()
	network := &testNetwork{isolated: map[uint64]bool{}}
	nodes := make([]*testNode, n)
	peerMaps := make([]map[uint64]raft.Peer, n)

	for i := range nodes {
		dir := t.TempDir()
		state, err := raft.LoadState(filepath.Join(dir, "raft-state.dat"))
		if err != nil {
			t.Fatalf("LoadState: %v", err)
		}
		log, err := raft.OpenLog(filepath.Join(dir, "raft-log.dat"))
		if err != nil {
			t.Fatalf("OpenLog: %v", err)
		}
		applyCh := make(chan raft.ApplyMsg)
		sm := NewStateMachine(store.NewStore(), dedup.NewTable())
		peerMaps[i] = map[uint64]raft.Peer{}
		node := raft.NewNode(uint64(i+1), state, log, peerMaps[i], applyCh)

		runDone := make(chan struct{})
		go func() {
			defer close(runDone)
			_ = sm.Run(applyCh)
		}()
		t.Cleanup(func() {
			node.Stop()
			<-runDone
			_ = log.Close()
		})
		nodes[i] = &testNode{raft: node, sm: sm, server: NewKVServer(node, sm)}
	}

	// Fill the peer maps BEFORE Start: NewNode keeps a reference to each
	// map, and nothing reads them until the background loops run.
	for i := range nodes {
		for j := range nodes {
			if i != j {
				peerMaps[i][uint64(j+1)] = &networkPeer{network: network, from: uint64(i + 1), to: uint64(j + 1), target: nodes[j].raft}
			}
		}
	}
	for _, tn := range nodes {
		tn.raft.Start()
	}
	return nodes, network
}

// waitForLeader returns the single node that is leader, excluding any IDs
// given (used to wait for a NEW leader after cutting the old one off).
func waitForLeader(t *testing.T, nodes []*testNode, excluding ...*testNode) *testNode {
	t.Helper()
	var leader *testNode
	waitFor(t, func() bool {
		for _, tn := range nodes {
			skip := false
			for _, ex := range excluding {
				if tn == ex {
					skip = true
				}
			}
			if !skip && tn.raft.Role() == raft.Leader {
				leader = tn
				return true
			}
		}
		return false
	})
	return leader
}

func put(t *testing.T, tn *testNode, key, value, clientID string, requestID uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := tn.server.Put(ctx, &kvpb.PutRequest{Key: key, Value: value, ClientId: clientID, RequestId: requestID}); err != nil {
		t.Fatalf("Put(%s=%s): %v", key, value, err)
	}
}

// waitApplied waits until every given node's local store holds key=value.
func waitApplied(t *testing.T, nodes []*testNode, key, value string) {
	t.Helper()
	waitFor(t, func() bool {
		for _, tn := range nodes {
			if v, _ := tn.sm.Get(key); v != value {
				return false
			}
		}
		return true
	})
}

// This test checks CLAUDE.md's Milestone 7 break, case (a): a follower cut
// off from the cluster still holds an old value in its local store. It must
// refuse to serve the read — if followers served reads from local state,
// this read would return x=1 after x=2 had been committed and acknowledged.
func TestGet_CutOffFollowerRefusesInsteadOfReturningStaleData(t *testing.T) {
	nodes, network := startTestCluster(t, 3)
	leader := waitForLeader(t, nodes)

	put(t, leader, "x", "1", "c1", 1)
	waitApplied(t, nodes, "x", "1")

	var follower *testNode
	for _, tn := range nodes {
		if tn != leader {
			follower = tn
			break
		}
	}
	network.isolate(follower.raft.ID())

	put(t, leader, "x", "2", "c1", 2) // the leader and the other follower are a majority

	// The follower's local store is genuinely stale — the danger is real.
	if v, _ := follower.sm.Get("x"); v != "1" {
		t.Fatalf("setup: cut-off follower's local x = %q, want stale \"1\"", v)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := follower.server.Get(ctx, &kvpb.GetRequest{Key: "x"})
	if err == nil {
		t.Fatalf("cut-off follower served x=%q — a stale read after x=2 was acknowledged", resp.Value)
	}
	requireCode(t, err, codes.Unavailable)
}

// This test checks case (b), the one that motivates ReadIndex: a LEADER cut
// off by a partition still believes it leads, while the other side elects a
// new leader that accepts a write. Reading on "whoever thinks it's leader"
// would return the old value; ReadIndex's confirmation round must fail
// instead, because the old leader cannot reach a majority.
func TestGet_CutOffOldLeaderCannotServeStaleRead(t *testing.T) {
	nodes, network := startTestCluster(t, 3)
	oldLeader := waitForLeader(t, nodes)

	put(t, oldLeader, "x", "1", "c1", 1)
	waitApplied(t, nodes, "x", "1")

	network.isolate(oldLeader.raft.ID())
	newLeader := waitForLeader(t, nodes, oldLeader)
	put(t, newLeader, "x", "2", "c1", 2)

	// The danger is real: the old leader still believes it leads, and its
	// local store still says x=1.
	if oldLeader.raft.Role() != raft.Leader {
		t.Fatalf("setup: cut-off old leader's role = %v, want it to still believe it is Leader", oldLeader.raft.Role())
	}
	if v, _ := oldLeader.sm.Get("x"); v != "1" {
		t.Fatalf("setup: old leader's local x = %q, want stale \"1\"", v)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := oldLeader.server.Get(ctx, &kvpb.GetRequest{Key: "x"})
	if err == nil {
		t.Fatalf("cut-off old leader served x=%q — a stale read after the new leader committed x=2", resp.Value)
	}
	requireCode(t, err, codes.Unavailable)

	// And the read the client retries elsewhere is correct.
	resp, err = newLeader.server.Get(ctx, &kvpb.GetRequest{Key: "x"})
	if err != nil || resp.Value != "2" {
		t.Fatalf("new leader Get = (%v, %v), want x=\"2\"", resp, err)
	}
}
