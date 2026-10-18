package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/ring"
	"github.com/imPranav14/Distributed-KVstore/internal/server"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
)

// startTestServer boots a real KVServer on a loopback TCP socket (ephemeral
// port, chosen by the OS via ":0"). This is the first genuinely end-to-end
// test in the project: real gRPC serialization, a real network connection,
// not just direct Go function calls.
//
// The grpcServer.Serve goroutine's exit condition (Rule 5) is
// grpcServer.Stop(), registered via t.Cleanup so it always runs, even if
// the test fails partway through.
func startTestServer(t *testing.T) (addr string) {
	t.Helper()
	return startTestServerWithInterceptor(t, nil)
}

// startTestServerWithInterceptor is like startTestServer but lets a test
// inject a grpc.UnaryServerInterceptor to observe or manipulate requests
// before they reach the real handler — used below to inspect exactly what
// RequestId value arrives on each retry attempt.
func startTestServerWithInterceptor(t *testing.T, interceptor grpc.UnaryServerInterceptor) (addr string) {
	t.Helper()
	return startShardedTestServer(t, interceptor, 1)
}

// startShardedTestServer serves one single-node Raft group per ID from one
// KVServer, the way a node hosts its shard groups.
func startShardedTestServer(t *testing.T, interceptor grpc.UnaryServerInterceptor, groups ...ring.GroupID) (addr string) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	replicas := map[ring.GroupID]server.GroupReplica{}
	for _, group := range groups {
		replicas[group] = startSingleNodeGroup(t)
	}
	kvServer, err := server.NewKVServer(replicas)
	if err != nil {
		t.Fatalf("NewKVServer: %v", err)
	}

	var opts []grpc.ServerOption
	if interceptor != nil {
		opts = append(opts, grpc.UnaryInterceptor(interceptor))
	}
	grpcServer := grpc.NewServer(opts...)
	kvpb.RegisterKVStoreServer(grpcServer, kvServer)

	go func() {
		// Exits when grpcServer.Stop() closes the listener below.
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.Stop)

	return listener.Addr().String()
}

// startSingleNodeGroup runs a real one-node Raft cluster and waits for it to
// elect itself, so every write the client sends goes through propose →
// commit → apply exactly as it does in production. A single node is its own
// majority, so writes commit the moment they are proposed and the client
// tests stay fast.
//
// Exits (Rule 5): Cleanup stops the node; its applier then closes applyCh,
// which ends the state machine's Run goroutine.
func startSingleNodeGroup(t *testing.T) server.GroupReplica {
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
	sm := server.NewStateMachine(store.NewStore(), dedup.NewTable(), nil, 0)
	node := raft.NewNode(1, state, log, map[uint64]raft.Peer{}, applyCh)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = sm.Run(applyCh)
	}()
	node.Start()
	t.Cleanup(func() {
		node.Stop()
		<-runDone
		_ = log.Close()
	})

	deadline := time.Now().Add(3 * time.Second)
	for node.Role() != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatalf("single-node cluster never elected itself")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return server.GroupReplica{Raft: node, StateMachine: sm}
}

// oneGroupRing is the ring of a single-group cluster: every key belongs to
// group 1, matching the servers these tests start.
func oneGroupRing(t *testing.T) *ring.Ring {
	t.Helper()
	keyRing, err := ring.New([]ring.GroupID{1})
	if err != nil {
		t.Fatalf("ring.New: %v", err)
	}
	return keyRing
}

// This test checks the invariant that every retry attempt of a SINGLE
// logical Put call reuses the exact same RequestId — not just that two
// SEPARATE calls get different ids (TestPut_ConsecutiveCalls_BothApply
// already covers that). An interceptor fails the first two attempts
// outright (before the real handler runs) and records the RequestId seen
// on every attempt, so this test can assert directly on what was actually
// sent over the wire on each retry — the property the whole dedup design
// in Milestone 2 depends on the client upholding.
func TestPut_RetriesReuseSameRequestID(t *testing.T) {
	var mu sync.Mutex
	var seenRequestIDs []uint64
	attemptCount := 0

	interceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if putReq, ok := req.(*kvpb.PutRequest); ok {
			mu.Lock()
			seenRequestIDs = append(seenRequestIDs, putReq.RequestId)
			attemptCount++
			shouldFail := attemptCount <= 2 // fail the first two attempts
			mu.Unlock()

			if shouldFail {
				return nil, status.Error(codes.Unavailable, "simulated transient failure")
			}
		}
		return handler(ctx, req)
	}

	addr := startTestServerWithInterceptor(t, interceptor)
	c, err := NewClient(oneGroupRing(t), addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if err := c.Put(context.Background(), "x", "5"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seenRequestIDs) < 3 {
		t.Fatalf("server saw %d attempts, want at least 3 (2 failures + 1 success)", len(seenRequestIDs))
	}
	for i, id := range seenRequestIDs {
		if id != seenRequestIDs[0] {
			t.Fatalf("attempt %d used RequestId=%d, want %d (same as attempt 0) — retries must reuse the same RequestId", i, id, seenRequestIDs[0])
		}
	}
}

// This test checks the invariant that a full PUT -> GET -> DELETE -> GET
// round trip works correctly through real gRPC serialization end to end —
// not just that each layer works in isolation (already proven in earlier
// milestones), but that the wire format, client marshaling, and server
// unmarshaling all agree with each other.
func TestPutGetDelete_EndToEnd(t *testing.T) {
	addr := startTestServer(t)
	c, err := NewClient(oneGroupRing(t), addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx := context.Background()

	if err := c.Put(ctx, "x", "5"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	value, found, err := c.Get(ctx, "x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || value != "5" {
		t.Fatalf("Get after Put: value=%q found=%v, want value=%q found=true", value, found, "5")
	}

	if err := c.Delete(ctx, "x"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, found, err = c.Get(ctx, "x")
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if found {
		t.Fatalf("Get after Delete: found = true, want false")
	}
}

// This test checks the invariant that Get on a key that was never written
// correctly reports found=false over the real network path.
func TestGet_MissingKey_ReturnsNotFound(t *testing.T) {
	addr := startTestServer(t)
	c, err := NewClient(oneGroupRing(t), addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	_, found, err := c.Get(context.Background(), "never-existed")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found {
		t.Fatalf("Get on missing key: found = true, want false")
	}
}

// This test checks the invariant that nextRequestID actually advances
// between separate logical calls — two consecutive Puts from the same
// Client must both succeed and both be visible, not have the second
// mistaken for a duplicate of the first (which would happen if
// nextRequestID were never incremented).
func TestPut_ConsecutiveCalls_BothApply(t *testing.T) {
	addr := startTestServer(t)
	c, err := NewClient(oneGroupRing(t), addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx := context.Background()

	if err := c.Put(ctx, "a", "1"); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if err := c.Put(ctx, "b", "2"); err != nil {
		t.Fatalf("second Put: %v", err)
	}

	valueA, foundA, _ := c.Get(ctx, "a")
	valueB, foundB, _ := c.Get(ctx, "b")
	if !foundA || valueA != "1" {
		t.Fatalf("key a: value=%q found=%v, want value=%q found=true", valueA, foundA, "1")
	}
	if !foundB || valueB != "2" {
		t.Fatalf("key b: value=%q found=%v, want value=%q found=true", valueB, foundB, "2")
	}
}

// This test checks, over a real socket, that a client with no server to
// reach keeps trying for its whole budget — not one quick round — and then
// gives up with ErrNoLeaderReachable rather than retrying forever.
func TestPut_NoServer_FailsAfterItsBudget(t *testing.T) {
	// Nothing is listening on this port.
	c, err := NewClient(oneGroupRing(t), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.policy.totalBudget = 300 * time.Millisecond // the default 5s, shortened to keep the test quick

	start := time.Now()
	err = c.Put(context.Background(), "x", "5")
	elapsed := time.Since(start)

	if !errors.Is(err, ErrNoLeaderReachable) {
		t.Fatalf("Put against a non-listening address: err = %v, want ErrNoLeaderReachable", err)
	}
	if elapsed < 300*time.Millisecond || elapsed > time.Second {
		t.Fatalf("Put failed after %v, want it to keep trying for its 300ms budget and then stop", elapsed)
	}
}

// This test checks the invariant that a caller-cancelled context stops
// retries immediately, rather than waiting out the full backoff schedule.
func TestPut_ContextCancelled_StopsRetryingEarly(t *testing.T) {
	c, err := NewClient(oneGroupRing(t), "127.0.0.1:1") // nothing listening
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = c.Put(ctx, "x", "5")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("Put with a cancelled context: err = nil, want an error")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Put took %v after context cancellation, want it to stop quickly instead of running the full backoff schedule", elapsed)
	}
}

// This test checks round-robin failover: with the FIRST address dead, the
// client must move on to the next node instead of retrying the dead one
// until it gives up. Before Milestone 7c a client knew exactly one node, so
// losing it — or reaching a follower — meant failing outright.
func TestClient_FailsOverPastDeadNode(t *testing.T) {
	addr := startTestServer(t)
	c, err := NewClient(oneGroupRing(t), "127.0.0.1:1", addr) // nothing listens on port 1
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx := context.Background()

	if err := c.Put(ctx, "x", "5"); err != nil {
		t.Fatalf("Put with the first node dead: %v, want failover to the second", err)
	}
	value, found, err := c.Get(ctx, "x")
	if err != nil || !found || value != "5" {
		t.Fatalf("Get after failover = (%q, %v, %v), want (\"5\", true, nil)", value, found, err)
	}
}

// This test checks that NewClient refuses an empty address list rather than
// building a client that would panic on its first call.
func TestNewClient_RequiresAtLeastOneTarget(t *testing.T) {
	if _, err := NewClient(oneGroupRing(t)); err == nil {
		t.Fatalf("NewClient(oneGroupRing(t)) with no targets: err = nil, want an error")
	}
}

// This test checks that the client names the key's group — as ITS ring
// computes it — on every attempt of every call, retries included. The
// server refuses any request whose group disagrees with its own ring, so a
// client that sent the wrong group, or none, could not read or write at
// all; and one that changed the group between retries would split a single
// logical write across two groups' dedup tables.
func TestClient_NamesTheKeysGroupOnEveryAttempt(t *testing.T) {
	type attempt struct {
		key   string
		group uint64
	}
	var mu sync.Mutex
	var attempts []attempt
	failedOnce := false

	interceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		var key string
		var group uint64
		switch r := req.(type) {
		case *kvpb.PutRequest:
			key, group = r.Key, r.GroupId
		case *kvpb.GetRequest:
			key, group = r.Key, r.GroupId
		case *kvpb.DeleteRequest:
			key, group = r.Key, r.GroupId
		}
		mu.Lock()
		attempts = append(attempts, attempt{key, group})
		failFirst := !failedOnce
		failedOnce = true
		mu.Unlock()
		if failFirst { // force one retry, to check it names the same group
			return nil, status.Error(codes.Unavailable, "simulated transient failure")
		}
		return handler(ctx, req)
	}
	addr := startShardedTestServer(t, interceptor, 1, 2, 3)

	keyRing, err := ring.New([]ring.GroupID{1, 2, 3})
	if err != nil {
		t.Fatalf("ring.New: %v", err)
	}
	c, err := NewClient(keyRing, addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx := context.Background()
	for i := 0; i < 9; i++ {
		key := fmt.Sprintf("user:%d", i)
		if err := c.Put(ctx, key, "v"); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
		if value, found, err := c.Get(ctx, key); err != nil || !found || value != "v" {
			t.Fatalf("Get(%s) = %q, %v, %v; want v", key, value, found, err)
		}
		if err := c.Delete(ctx, key); err != nil {
			t.Fatalf("Delete(%s): %v", key, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	groupsSeen := map[uint64]bool{}
	for _, a := range attempts {
		if want := uint64(keyRing.Owner(a.key)); a.group != want {
			t.Fatalf("an attempt for %q named group %d, want %d", a.key, a.group, want)
		}
		groupsSeen[a.group] = true
	}
	if attempts[0].key != attempts[1].key {
		t.Fatalf("setup: the first attempt was not retried")
	}
	if len(groupsSeen) < 2 {
		t.Fatalf("setup: every key fell in one group %v, so the test cannot tell groups apart", groupsSeen)
	}
}
