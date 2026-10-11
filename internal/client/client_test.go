package client

import (
	"context"
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

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	kvServer := startSingleNodeRaft(t)

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

// startSingleNodeRaft runs a real one-node Raft cluster behind a KVServer
// and waits for it to elect itself, so every write the client sends goes
// through propose → commit → apply exactly as it does in production. A
// single node is its own majority, so writes commit the moment they are
// proposed and the client tests stay fast.
//
// Exits (Rule 5): Cleanup stops the node; its applier then closes applyCh,
// which ends the state machine's Run goroutine.
func startSingleNodeRaft(t *testing.T) *server.KVServer {
	t.Helper()
	dir := t.TempDir()
	state, err := raft.LoadState(filepath.Join(dir, "raft-state.dat"))
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	log, err := raft.OpenLog(filepath.Join(dir, "raft-log.dat"), 0, 0)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}

	applyCh := make(chan raft.ApplyMsg)
	sm := server.NewStateMachine(store.NewStore(), dedup.NewTable())
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
	return server.NewKVServer(node, sm)
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
	c, err := NewClient(addr)
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
	c, err := NewClient(addr)
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
	c, err := NewClient(addr)
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
	c, err := NewClient(addr)
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

// This test checks the invariant that retryWithBackoff gives up after
// maxAttempts and returns an error, rather than retrying forever, when
// there is no server to connect to at all.
func TestPut_NoServer_FailsAfterRetries(t *testing.T) {
	// Nothing is listening on this port.
	c, err := NewClient("127.0.0.1:1")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	start := time.Now()
	err = c.Put(context.Background(), "x", "5")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("Put against a non-listening address: err = nil, want an error")
	}
	// Backoff between 5 attempts is 100+200+400+800 = 1500ms minimum.
	if elapsed < 1500*time.Millisecond {
		t.Fatalf("Put failed after only %v, want at least 1.5s (backoff was skipped)", elapsed)
	}
}

// This test checks the invariant that a caller-cancelled context stops
// retries immediately, rather than waiting out the full backoff schedule.
func TestPut_ContextCancelled_StopsRetryingEarly(t *testing.T) {
	c, err := NewClient("127.0.0.1:1") // nothing listening
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
	c, err := NewClient("127.0.0.1:1", addr) // nothing listens on port 1
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
	if _, err := NewClient(); err == nil {
		t.Fatalf("NewClient() with no targets: err = nil, want an error")
	}
}
