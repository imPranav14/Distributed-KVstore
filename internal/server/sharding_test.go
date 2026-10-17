package server

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/ring"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
)

func mustRing(t *testing.T, groups ...ring.GroupID) *ring.Ring {
	t.Helper()
	keyRing, err := ring.New(groups)
	if err != nil {
		t.Fatalf("ring.New(%v): %v", groups, err)
	}
	return keyRing
}

// shardedServer starts one single-node Raft group per ID — each is its own
// majority, so writes commit at once — and serves them all from one
// KVServer, the way a process hosts its groups from 9d on.
func shardedServer(t *testing.T, groups ...ring.GroupID) (*KVServer, map[ring.GroupID]GroupReplica) {
	t.Helper()
	replicas := map[ring.GroupID]GroupReplica{}
	for _, group := range groups {
		node := startTestNode(t, map[uint64]raft.Peer{})
		waitForRole(t, node.raft, raft.Leader)
		replicas[group] = GroupReplica{Raft: node.raft, StateMachine: node.sm}
	}
	kvServer, err := NewKVServer(replicas)
	if err != nil {
		t.Fatalf("NewKVServer: %v", err)
	}
	return kvServer, replicas
}

// This test checks the INVARIANT that an operation runs only in the group
// that owns its key: after 30 writes, each group's state machine holds
// exactly the keys the ring assigns it, and none of anyone else's. The
// expected owner comes from a SEPARATE ring built the way a client builds
// one, so this also checks that client and server rings agree.
func TestShardedKV_KeysLandOnlyInTheirOwningGroup(t *testing.T) {
	kvServer, replicas := shardedServer(t, 1, 2, 3)
	clientRing := mustRing(t, 1, 2, 3)
	ctx := context.Background()

	keysPerGroup := map[ring.GroupID]int{}
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("key-%02d", i)
		owner := clientRing.Owner(key)
		keysPerGroup[owner]++
		put := &kvpb.PutRequest{Key: key, Value: key, ClientId: "c1", RequestId: uint64(i + 1), GroupId: uint64(owner)}
		if _, err := kvServer.Put(ctx, put); err != nil {
			t.Fatalf("Put(%s) to its owner, group %d: %v", key, owner, err)
		}
	}
	if len(keysPerGroup) != 3 {
		t.Fatalf("setup: 30 keys covered only groups %v, so the test cannot show keys staying apart", keysPerGroup)
	}

	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("key-%02d", i)
		owner := clientRing.Owner(key)
		for group, replica := range replicas {
			_, found := replica.StateMachine.Get(key)
			if found != (group == owner) {
				t.Fatalf("%s is owned by group %d, but group %d has found=%v", key, owner, group, found)
			}
		}
	}
}

// This test checks that a write naming the wrong group is refused BEFORE
// anything is proposed: no group's log grows. Refusing after proposing would
// be too late — the entry would already be durable in some group's log.
func TestShardedKV_WrongGroupWriteIsRefusedBeforeProposing(t *testing.T) {
	kvServer, replicas := shardedServer(t, 1, 2, 3)
	key := "user:1"
	owner := mustRing(t, 1, 2, 3).Owner(key)
	wrongGroup := owner%3 + 1

	logSizes := map[ring.GroupID]int64{}
	for group, replica := range replicas {
		logSizes[group] = replica.Raft.LogSize()
	}

	ctx := context.Background()
	for _, named := range []uint64{uint64(wrongGroup), 0} {
		_, err := kvServer.Put(ctx, &kvpb.PutRequest{Key: key, Value: "v", ClientId: "c1", RequestId: 1, GroupId: named})
		requireCode(t, err, codes.FailedPrecondition)
		_, err = kvServer.Delete(ctx, &kvpb.DeleteRequest{Key: key, ClientId: "c1", RequestId: 2, GroupId: named})
		requireCode(t, err, codes.FailedPrecondition)
	}

	for group, replica := range replicas {
		if size := replica.Raft.LogSize(); size != logSizes[group] {
			t.Fatalf("group %d's log grew from %d to %d bytes — a refused write was proposed", group, logSizes[group], size)
		}
	}
}

// This test checks that a READ naming the wrong group is refused too. It is
// the subtler case: the wrong group has no such key, so without the check
// the reply would be a clean "not found" — a stale read that looks correct.
func TestShardedKV_WrongGroupReadIsRefusedNotAnsweredNotFound(t *testing.T) {
	kvServer, _ := shardedServer(t, 1, 2, 3)
	key := "user:1"
	owner := mustRing(t, 1, 2, 3).Owner(key)
	ctx := context.Background()

	if _, err := kvServer.Put(ctx, &kvpb.PutRequest{Key: key, Value: "v", ClientId: "c1", RequestId: 1, GroupId: uint64(owner)}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	for _, named := range []uint64{uint64(owner%3 + 1), 0} {
		resp, err := kvServer.Get(ctx, &kvpb.GetRequest{Key: key, GroupId: named})
		if err == nil {
			t.Fatalf("Get naming group %d answered found=%v, want a FailedPrecondition error", named, resp.Found)
		}
		requireCode(t, err, codes.FailedPrecondition)
	}

	resp, err := kvServer.Get(ctx, &kvpb.GetRequest{Key: key, GroupId: uint64(owner)})
	if err != nil || resp.Value != "v" {
		t.Fatalf("Get naming the owner: resp %v, err %v; want v", resp, err)
	}
}

// This test checks that a KVServer is never built so that unset group IDs,
// or no groups at all, could be served.
func TestNewKVServer_RefusesNoReplicasAndGroupZero(t *testing.T) {
	if _, err := NewKVServer(map[ring.GroupID]GroupReplica{}); err == nil {
		t.Fatalf("NewKVServer accepted no replicas")
	}
	node := startTestNode(t, map[uint64]raft.Peer{})
	if _, err := NewKVServer(map[ring.GroupID]GroupReplica{0: {Raft: node.raft, StateMachine: node.sm}}); err == nil {
		t.Fatalf("NewKVServer accepted group 0")
	}
}

// This test checks that a client whose ring disagrees with the servers'
// fails SAFE, in both directions: every write either lands in the group
// both sides agree on, or is refused — none is ever written to a group the
// other side would not look in.
//
// It also shows why consistent hashing makes this workable: the two rings
// disagree ONLY about keys involving the group one side lacks. A client with
// an extra group 4 disagrees only on keys it gives to group 4; a client
// missing group 3 disagrees only on keys the servers give to group 3. Every
// other key works. (With modulo hashing, a count mismatch would disagree on
// most keys.)
func TestRingSkew_FailsSafeInBothDirections(t *testing.T) {
	kvServer, _ := shardedServer(t, 1, 2, 3)
	serverRing := mustRing(t, 1, 2, 3)
	ctx := context.Background()

	cases := []struct {
		name                   string
		clientRing             *ring.Ring
		disagreementIsExpected func(clientGroup, serverGroup ring.GroupID) bool
	}{
		{"client has an extra group 4", mustRing(t, 1, 2, 3, 4),
			func(clientGroup, _ ring.GroupID) bool { return clientGroup == 4 }},
		{"client is missing group 3", mustRing(t, 1, 2),
			func(_, serverGroup ring.GroupID) bool { return serverGroup == 3 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			agreed, refused := 0, 0
			for i := 0; i < 60; i++ {
				key := fmt.Sprintf("skew-%02d", i)
				clientGroup, serverGroup := tc.clientRing.Owner(key), serverRing.Owner(key)
				_, err := kvServer.Put(ctx, &kvpb.PutRequest{
					Key: key, Value: "v", ClientId: tc.name, RequestId: uint64(i + 1), GroupId: uint64(clientGroup),
				})

				if clientGroup == serverGroup {
					if err != nil {
						t.Fatalf("%s: client and server agree on group %d, but the write failed: %v", key, clientGroup, err)
					}
					agreed++
					continue
				}
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("%s: client named group %d, server owns %d — want FailedPrecondition, got %v", key, clientGroup, serverGroup, err)
				}
				if !tc.disagreementIsExpected(clientGroup, serverGroup) {
					t.Fatalf("%s: rings disagree (client %d, server %d) on groups BOTH sides have — consistent hashing should prevent this", key, clientGroup, serverGroup)
				}
				refused++
			}
			if agreed == 0 || refused == 0 {
				t.Fatalf("setup: %d agreed, %d refused — need both for the test to mean anything", agreed, refused)
			}
			t.Logf("%d keys agreed and were written, %d disagreed and were refused", agreed, refused)
		})
	}
}

// This test checks the INVARIANT that ownership is checked before
// leadership. A follower — which cannot serve anything — must still answer a
// wrong-group request with FailedPrecondition, not Unavailable. The client
// depends on that order: it treats NotLeader as "this node agrees with my
// ring, keep it eligible" and FailedPrecondition as "this node disagrees,
// never ask it again in this call".
func TestKVServer_WrongGroupOnAFollower_IsRefusedNotNotLeader(t *testing.T) {
	// One unreachable peer: this node needs 2 of 2 votes, so it stays a follower.
	follower := startTestNode(t, map[uint64]raft.Peer{2: unreachablePeer{}})
	if follower.raft.Role() == raft.Leader {
		t.Fatalf("setup: the node is leader, want a follower")
	}
	ctx := context.Background()

	_, err := follower.server.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "v", ClientId: "c1", RequestId: 1, GroupId: 2})
	requireCode(t, err, codes.FailedPrecondition)
	_, err = follower.server.Get(ctx, &kvpb.GetRequest{Key: "x", GroupId: 2})
	requireCode(t, err, codes.FailedPrecondition)

	// The right group on the same follower is Unavailable — proving the
	// refusal above came from the ownership check, not from leadership.
	_, err = follower.server.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "v", ClientId: "c1", RequestId: 1, GroupId: 1})
	requireCode(t, err, codes.Unavailable)
}
