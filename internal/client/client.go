// Package client implements a KVStore client with automatic retries. It
// does not depend on internal/dedup: the server's dedup.ClientID and
// dedup.RequestID types exist only to organize the server's dedup table.
// This package only needs to put the right raw values (string, uint64) on
// the wire, matching the proto message fields — the two sides are connected
// through the .proto contract, not a shared Go type.
package client

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/imPranav14/Distributed-KVstore/internal/ring"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
)

const (
	maxAttempts       = 5
	baseBackoff       = 100 * time.Millisecond
	maxBackoff        = 2 * time.Second
	perAttemptTimeout = 2 * time.Second
)

// Client is NOT safe for concurrent use by multiple goroutines. This is not
// an implementation gap to fix with a mutex — it follows directly from
// Milestone 2's dedup design, which only remembers the LATEST RequestID per
// client and assumes at most one request in flight at a time. Two
// goroutines racing to increment nextRequestID would let one operation's
// dedup entry silently supersede the other's; the design itself, not just
// this struct, is inherently single-flight.
type Client struct {
	// stubs holds one connection per cluster node. current is the node the
	// next attempt goes to — this client's best guess at the leader.
	stubs   []kvpb.KVStoreClient
	current int

	// keyRing tells the client which shard group owns each key. It must be
	// built from the same groups as the servers' rings; the server checks
	// the group the client names and refuses a mismatch (Milestone 9c).
	keyRing *ring.Ring

	clientID      string
	nextRequestID uint64
}

// NewClient dials every target (e.g. "localhost:50051", "localhost:50052")
// and returns a ready-to-use Client with a freshly generated ClientID.
// keyRing must describe the cluster's shard groups exactly as the servers'
// rings do. Dialing is lazy: no node needs to be up yet. No TLS: out of
// scope per CLAUDE.md's explicit project boundaries.
func NewClient(keyRing *ring.Ring, targets ...string) (*Client, error) {
	if keyRing == nil {
		return nil, fmt.Errorf("client: no key ring given")
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("client: no target addresses given")
	}

	stubs := make([]kvpb.KVStoreClient, 0, len(targets))
	for _, target := range targets {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("client: failed to dial %s: %w", target, err)
		}
		stubs = append(stubs, kvpb.NewKVStoreClient(conn))
	}

	return &Client{stubs: stubs, keyRing: keyRing, clientID: uuid.NewString()}, nil
}

// Put stores value at key, retrying on failure. requestID is allocated once
// for this logical call and reused across every retry attempt below — that
// reuse is what lets the server's dedup table recognize a retry as the same
// request rather than a new one.
func (c *Client) Put(ctx context.Context, key, value string) error {
	requestID := c.nextRequestID
	c.nextRequestID++
	// Computed once, like requestID: every attempt names the same group.
	group := uint64(c.keyRing.Owner(key))

	return c.retryWithBackoff(ctx, func(attemptCtx context.Context, stub kvpb.KVStoreClient) error {
		_, err := stub.Put(attemptCtx, &kvpb.PutRequest{
			Key: key, Value: value, ClientId: c.clientID, RequestId: requestID, GroupId: group,
		})
		return err
	})
}

// Delete removes key, retrying on failure. Same requestID reuse reasoning
// as Put.
func (c *Client) Delete(ctx context.Context, key string) error {
	requestID := c.nextRequestID
	c.nextRequestID++
	group := uint64(c.keyRing.Owner(key))

	return c.retryWithBackoff(ctx, func(attemptCtx context.Context, stub kvpb.KVStoreClient) error {
		_, err := stub.Delete(attemptCtx, &kvpb.DeleteRequest{
			Key: key, ClientId: c.clientID, RequestId: requestID, GroupId: group,
		})
		return err
	})
}

// Get reads key, retrying on failure. No requestID is needed: GetRequest
// carries no ClientId/RequestId at all, since reads are side-effect-free
// and always safe to retry.
func (c *Client) Get(ctx context.Context, key string) (value string, found bool, err error) {
	var resp *kvpb.GetResponse
	group := uint64(c.keyRing.Owner(key))
	err = c.retryWithBackoff(ctx, func(attemptCtx context.Context, stub kvpb.KVStoreClient) error {
		var rpcErr error
		resp, rpcErr = stub.Get(attemptCtx, &kvpb.GetRequest{Key: key, GroupId: group})
		return rpcErr
	})
	if err != nil {
		return "", false, err
	}
	return resp.Value, resp.Found, nil
}

// retryWithBackoff runs fn against one node at a time, retrying with
// exponential backoff (100ms, 200ms, 400ms... capped at maxBackoff) on
// failure, up to maxAttempts. Each attempt gets its own perAttemptTimeout
// derived from ctx, so a single hung attempt can't block forever. If ctx is
// cancelled while waiting between attempts, retryWithBackoff stops
// immediately instead of blindly finishing out maxAttempts.
//
// Failover is round-robin: after ANY failed attempt, the next attempt goes
// to the next node. A "not leader" reply, a dead node and a timeout all
// mean the same thing to the client — try someone else. With three nodes
// that is at most two wasted hops to reach the leader. A successful node is
// kept for the next call.
//
// INVARIANT: every attempt of one logical call — including attempts that
// land on DIFFERENT nodes — carries the same request ID, because fn closes
// over it. That is what makes failover safe for writes: if an attempt that
// "failed" (timed out) actually committed, the replicated dedup table on
// every node skips the retry.
func (c *Client) retryWithBackoff(ctx context.Context, fn func(context.Context, kvpb.KVStoreClient) error) error {
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, perAttemptTimeout)
		lastErr = fn(attemptCtx, c.stubs[c.current])
		cancel()

		if lastErr == nil {
			return nil
		}
		c.current = (c.current + 1) % len(c.stubs)

		if attempt == maxAttempts-1 {
			break
		}

		backoff := baseBackoff << attempt // baseBackoff * 2^attempt
		if backoff > maxBackoff {
			backoff = maxBackoff
		}

		select {
		case <-time.After(backoff):
			// continue to next attempt
		case <-ctx.Done():
			return fmt.Errorf("client: context cancelled while waiting to retry: %w", ctx.Err())
		}
	}

	return fmt.Errorf("client: exhausted %d attempts: %w", maxAttempts, lastErr)
}
