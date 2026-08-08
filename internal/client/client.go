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
	stub          kvpb.KVStoreClient
	clientID      string
	nextRequestID uint64
}

// NewClient dials target (e.g. "localhost:50051") and returns a ready-to-use
// Client with a freshly generated ClientID. No TLS: out of scope per
// CLAUDE.md's explicit project boundaries.
func NewClient(target string) (*Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("client: failed to dial %s: %w", target, err)
	}

	return &Client{
		stub:     kvpb.NewKVStoreClient(conn),
		clientID: uuid.NewString(),
	}, nil
}

// Put stores value at key, retrying on failure. requestID is allocated once
// for this logical call and reused across every retry attempt below — that
// reuse is what lets the server's dedup table recognize a retry as the same
// request rather than a new one.
func (c *Client) Put(ctx context.Context, key, value string) error {
	requestID := c.nextRequestID
	c.nextRequestID++

	return c.retryWithBackoff(ctx, func(attemptCtx context.Context) error {
		_, err := c.stub.Put(attemptCtx, &kvpb.PutRequest{
			Key: key, Value: value, ClientId: c.clientID, RequestId: requestID,
		})
		return err
	})
}

// Delete removes key, retrying on failure. Same requestID reuse reasoning
// as Put.
func (c *Client) Delete(ctx context.Context, key string) error {
	requestID := c.nextRequestID
	c.nextRequestID++

	return c.retryWithBackoff(ctx, func(attemptCtx context.Context) error {
		_, err := c.stub.Delete(attemptCtx, &kvpb.DeleteRequest{
			Key: key, ClientId: c.clientID, RequestId: requestID,
		})
		return err
	})
}

// Get reads key, retrying on failure. No requestID is needed: GetRequest
// carries no ClientId/RequestId at all, since reads are side-effect-free
// and always safe to retry.
func (c *Client) Get(ctx context.Context, key string) (value string, found bool, err error) {
	var resp *kvpb.GetResponse
	err = c.retryWithBackoff(ctx, func(attemptCtx context.Context) error {
		var rpcErr error
		resp, rpcErr = c.stub.Get(attemptCtx, &kvpb.GetRequest{Key: key})
		return rpcErr
	})
	if err != nil {
		return "", false, err
	}
	return resp.Value, resp.Found, nil
}

// retryWithBackoff runs fn, retrying with exponential backoff (100ms, 200ms,
// 400ms... capped at maxBackoff) on failure, up to maxAttempts. Each
// attempt gets its own perAttemptTimeout derived from ctx, so a single
// hung attempt can't block forever. If ctx is cancelled while waiting
// between attempts, retryWithBackoff stops immediately instead of blindly
// finishing out maxAttempts.
func (c *Client) retryWithBackoff(ctx context.Context, fn func(context.Context) error) error {
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, perAttemptTimeout)
		lastErr = fn(attemptCtx)
		cancel()

		if lastErr == nil {
			return nil
		}

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
