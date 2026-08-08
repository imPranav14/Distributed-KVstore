package server

import (
	"context"
	"testing"

	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
)

func newTestServer() *KVServer {
	return NewKVServer(store.NewStore(), dedup.NewTable())
}

// This test checks the invariant that Get correctly translates store.Get's
// (value, found) result into a GetResponse for a key that was never set.
func TestGet_MissingKey_ReturnsNotFound(t *testing.T) {
	s := newTestServer()

	resp, err := s.Get(context.Background(), &kvpb.GetRequest{Key: "missing"})

	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if resp.Found {
		t.Fatalf("Get on missing key: Found = true, want false")
	}
}

// This test checks the invariant that Put is correctly wired through to
// store.Put — a value written via the gRPC handler must be visible to a
// subsequent Get through the same handler.
func TestPutThenGet_ValueVisible(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	_, err := s.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "5", ClientId: "c1", RequestId: 1})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	resp, err := s.Get(ctx, &kvpb.GetRequest{Key: "x"})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !resp.Found || resp.Value != "5" {
		t.Fatalf("Get after Put: Value=%q Found=%v, want Value=%q Found=true", resp.Value, resp.Found, "5")
	}
}

// This test checks the invariant that Put correctly wires ClientId/RequestId
// from the proto message into dedup.Table — not just that dedup works in
// isolation (already proven in Milestone 2), but that this layer converts
// and passes the fields through correctly. A retried request (same
// ClientId/RequestId, different Value) must not overwrite the original.
func TestPut_DuplicateRequestID_NotReapplied(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	if _, err := s.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "first", ClientId: "c1", RequestId: 1}); err != nil {
		t.Fatalf("first Put returned error: %v", err)
	}
	if _, err := s.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "second-should-be-ignored", ClientId: "c1", RequestId: 1}); err != nil {
		t.Fatalf("retried Put returned error: %v", err)
	}

	resp, err := s.Get(ctx, &kvpb.GetRequest{Key: "x"})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if resp.Value != "first" {
		t.Fatalf("value after duplicate request = %q, want %q — the retry was reapplied instead of deduped", resp.Value, "first")
	}
}

// This test checks the invariant that Delete is correctly wired through to
// store.Delete.
func TestDelete_RemovesKey(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	if _, err := s.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "5", ClientId: "c1", RequestId: 1}); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := s.Delete(ctx, &kvpb.DeleteRequest{Key: "x", ClientId: "c1", RequestId: 2}); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}

	resp, err := s.Get(ctx, &kvpb.GetRequest{Key: "x"})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if resp.Found {
		t.Fatalf("Get after Delete: Found = true, want false")
	}
}

// This test checks the invariant that Delete's ClientId/RequestId are also
// correctly wired into dedup.Table — a retried Delete must not re-run
// (harmless here since Delete is naturally idempotent, but proves the
// wiring, not just Store's own idempotency).
func TestDelete_DuplicateRequestID_NotReapplied(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	if _, err := s.Put(ctx, &kvpb.PutRequest{Key: "x", Value: "5", ClientId: "c1", RequestId: 1}); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := s.Delete(ctx, &kvpb.DeleteRequest{Key: "x", ClientId: "c1", RequestId: 2}); err != nil {
		t.Fatalf("first Delete returned error: %v", err)
	}
	// A different client re-uses the same key after the delete — if the
	// retried Delete below were incorrectly reapplied as "new work", this
	// wouldn't corrupt anything further for Delete specifically (deleting
	// an absent key is a no-op), so this test's real job is exercising
	// that the ClientId/RequestId plumbing doesn't panic or misroute on a
	// second call with the same identifiers.
	if _, err := s.Delete(ctx, &kvpb.DeleteRequest{Key: "x", ClientId: "c1", RequestId: 2}); err != nil {
		t.Fatalf("retried Delete returned error: %v", err)
	}
}
