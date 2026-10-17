// Package client implements a KVStore client with automatic retries. It
// does not depend on internal/dedup: the server's dedup.ClientID and
// dedup.RequestID types exist only to organize the server's dedup table.
// This package only needs to put the right raw values (string, uint64) on
// the wire, matching the proto message fields — the two sides are connected
// through the .proto contract, not a shared Go type.
package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/imPranav14/Distributed-KVstore/internal/ring"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
)

// retryPolicy bounds how hard one logical call tries before giving up.
type retryPolicy struct {
	// maxAttempts is the ONE budget shared by every kind of failure —
	// refusals, NotLeader, unreachable nodes and timeouts alike — so a call
	// always ends, even against a group that can never elect a leader.
	maxAttempts int

	// After a leader-discovery failure the client waits baseBackoff × 2^n,
	// capped at maxBackoff, giving an election time to finish.
	baseBackoff time.Duration
	maxBackoff  time.Duration

	// perAttemptTimeout bounds one call to one node, so a hung node costs
	// one attempt rather than the whole call.
	perAttemptTimeout time.Duration
}

// defaultRetryPolicy is what NewClient uses. With no caller deadline, a call
// that never finds a leader ends after at most 5 × 2s of attempts plus
// 100+200+400+800ms of backoff — about 11.5s — with ErrNoLeaderReachable.
var defaultRetryPolicy = retryPolicy{
	maxAttempts:       5,
	baseBackoff:       100 * time.Millisecond,
	maxBackoff:        2 * time.Second,
	perAttemptTimeout: 2 * time.Second,
}

var (
	// ErrNoLeaderReachable means the attempt budget ran out without any node
	// serving the key's group — every node was unreachable, timed out, or
	// was not the leader (for example because the group has lost its
	// majority). The last gRPC error is wrapped alongside it.
	ErrNoLeaderReachable = errors.New("client: no leader reachable for the key's group")

	// ErrRingMismatch means EVERY node refused the group this client named
	// for the key: the client's ring disagrees with the whole cluster's.
	ErrRingMismatch = errors.New("client: every node refused the key's group; this client's ring disagrees with the cluster's")
)

// Client is NOT safe for concurrent use by multiple goroutines. This is not
// an implementation gap to fix with a mutex — it follows directly from
// Milestone 2's dedup design, which only remembers the LATEST RequestID per
// client and assumes at most one request in flight at a time. Two
// goroutines racing to increment nextRequestID would let one operation's
// dedup entry silently supersede the other's; the design itself, not just
// this struct, is inherently single-flight.
type Client struct {
	// stubs holds one connection per cluster node.
	stubs []kvpb.KVStoreClient

	// leaderGuess holds, per shard group, the index in stubs of the node
	// that last served that group — the client's best guess at its leader.
	// Groups have different leaders, so one shared guess would start every
	// group switch at the wrong node. No mutex: a Client is single-goroutine
	// (see above).
	leaderGuess map[ring.GroupID]int

	policy retryPolicy

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

	return newClient(keyRing, stubs, defaultRetryPolicy), nil
}

// newClient builds a Client over already-created stubs. NewClient uses it
// with real connections; tests use it with fake nodes and a fast policy.
func newClient(keyRing *ring.Ring, stubs []kvpb.KVStoreClient, policy retryPolicy) *Client {
	return &Client{
		stubs:       stubs,
		leaderGuess: make(map[ring.GroupID]int),
		policy:      policy,
		keyRing:     keyRing,
		clientID:    uuid.NewString(),
	}
}

// Put stores value at key, retrying on failure. requestID is allocated once
// for this logical call and reused across every retry attempt below — that
// reuse is what lets the server's dedup table recognize a retry as the same
// request rather than a new one.
func (c *Client) Put(ctx context.Context, key, value string) error {
	requestID := c.nextRequestID
	c.nextRequestID++
	// Computed once, like requestID: every attempt names the same group.
	group := c.keyRing.Owner(key)

	return c.retry(ctx, group, func(attemptCtx context.Context, stub kvpb.KVStoreClient) error {
		_, err := stub.Put(attemptCtx, &kvpb.PutRequest{
			Key: key, Value: value, ClientId: c.clientID, RequestId: requestID, GroupId: uint64(group),
		})
		return err
	})
}

// Delete removes key, retrying on failure. Same requestID reuse reasoning
// as Put.
func (c *Client) Delete(ctx context.Context, key string) error {
	requestID := c.nextRequestID
	c.nextRequestID++
	group := c.keyRing.Owner(key)

	return c.retry(ctx, group, func(attemptCtx context.Context, stub kvpb.KVStoreClient) error {
		_, err := stub.Delete(attemptCtx, &kvpb.DeleteRequest{
			Key: key, ClientId: c.clientID, RequestId: requestID, GroupId: uint64(group),
		})
		return err
	})
}

// Get reads key, retrying on failure. No requestID is needed: GetRequest
// carries no ClientId/RequestId at all, since reads are side-effect-free
// and always safe to retry.
func (c *Client) Get(ctx context.Context, key string) (value string, found bool, err error) {
	var resp *kvpb.GetResponse
	group := c.keyRing.Owner(key)
	err = c.retry(ctx, group, func(attemptCtx context.Context, stub kvpb.KVStoreClient) error {
		var rpcErr error
		resp, rpcErr = stub.Get(attemptCtx, &kvpb.GetRequest{Key: key, GroupId: uint64(group)})
		return rpcErr
	})
	if err != nil {
		return "", false, err
	}
	return resp.Value, resp.Found, nil
}

// retry runs fn against one node at a time until it succeeds, the attempt
// budget runs out, or the caller's context ends. Every failure falls into
// exactly one class:
//
//   - The caller's context ended: stop now and say so. That is the caller's
//     decision, not a fault of the node or the cluster.
//   - FailedPrecondition — a routing refusal: that node's ring disagrees
//     with ours about the key's group. Asking it again cannot change the
//     answer, so it is not asked again in this call, and there is no
//     backoff, because waiting cannot fix a configuration mismatch. Once
//     EVERY node has refused, the client's own ring is the one that is
//     wrong: ErrRingMismatch.
//   - Unavailable or DeadlineExceeded — leader discovery, exactly as before
//     sharding: NotLeader, an unreachable node, a node shutting down, or
//     this one attempt timing out. Move to the next node and back off, to
//     give an election time to finish. NotLeader is NOT a refusal: the
//     server checks ownership before leadership, so a NotLeader reply means
//     that node's ring agrees with ours — and it may become leader soon.
//   - Anything else is permanent (InvalidArgument, Internal, ...): no node
//     and no amount of waiting will change it, so it is returned at once.
//
// INVARIANT: ONE attempt budget (policy.maxAttempts) covers every class, so
// a call always ends — even against a group that has lost its majority and
// can never elect a leader. It ends with ErrNoLeaderReachable.
//
// INVARIANT: every attempt of one logical call — including attempts that
// land on DIFFERENT nodes — carries the same request ID and the same group,
// because fn closes over both. That is what makes failover safe for writes:
// if an attempt that "failed" (timed out) actually committed, the
// replicated dedup table skips the retry. It is also why every error this
// returns for a write means "outcome unknown", never "not applied".
func (c *Client) retry(ctx context.Context, group ring.GroupID, fn func(context.Context, kvpb.KVStoreClient) error) error {
	node := c.leaderGuess[group] // a group never seen starts at node 0
	refused := make(map[int]bool, len(c.stubs))
	var lastErr error

	for attempt := 0; attempt < c.policy.maxAttempts; attempt++ {
		// The caller's deadline or cancellation always wins over the budget —
		// including on the refusal path, which never sleeps and so would
		// otherwise never notice it.
		if ctx.Err() != nil {
			return callerStopped(group, ctx.Err(), lastErr)
		}

		attemptCtx, cancel := context.WithTimeout(ctx, c.policy.perAttemptTimeout)
		err := fn(attemptCtx, c.stubs[node])
		cancel()
		if err == nil {
			c.leaderGuess[group] = node // only a node that SERVED the group becomes the guess
			return nil
		}
		lastErr = err

		// The attempt may have failed only because the CALLER's context
		// ended mid-call. That is not this node's fault, and on the last
		// attempt it must not be reported as "no leader reachable".
		if ctx.Err() != nil {
			return callerStopped(group, ctx.Err(), lastErr)
		}

		switch status.Code(err) {
		case codes.FailedPrecondition:
			refused[node] = true
			if len(refused) == len(c.stubs) {
				// The sentinel already names the package; the group and the last
				// refusal are the context it lacks.
				return fmt.Errorf("%w (group %d): %w", ErrRingMismatch, group, err)
			}
			node = c.nextEligible(node, refused)
			continue // no backoff: waiting cannot fix a ring mismatch

		case codes.Unavailable, codes.DeadlineExceeded:
			node = c.nextEligible(node, refused)
			if attempt == c.policy.maxAttempts-1 {
				break // no point sleeping before giving up
			}
			if err := c.sleepBackoff(ctx, attempt); err != nil {
				return callerStopped(group, err, lastErr)
			}

		default:
			return err
		}
	}

	return fmt.Errorf("%w (group %d, %d attempts): %w", ErrNoLeaderReachable, group, c.policy.maxAttempts, lastErr)
}

// nextEligible returns the next node after from, round-robin, skipping
// nodes that refused the group in this call. retry only calls it while at
// least one node has not refused, so it always finds one (possibly from
// itself, when it is the only one left).
func (c *Client) nextEligible(from int, refused map[int]bool) int {
	for step := 1; step <= len(c.stubs); step++ {
		candidate := (from + step) % len(c.stubs)
		if !refused[candidate] {
			return candidate
		}
	}
	return from // unreachable: some node is always eligible here
}

// sleepBackoff waits baseBackoff × 2^attempt, capped at maxBackoff, and
// returns early with the context's error if the caller's context ends first.
func (c *Client) sleepBackoff(ctx context.Context, attempt int) error {
	backoff := c.policy.baseBackoff << attempt // baseBackoff * 2^attempt
	if backoff > c.policy.maxBackoff || backoff <= 0 {
		backoff = c.policy.maxBackoff
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// callerStopped reports a call ended by the caller's own context. It wraps
// the context error, so errors.Is(err, context.DeadlineExceeded) works, and
// the last attempt's error, if there was one, for diagnosis.
func callerStopped(group ring.GroupID, ctxErr, lastErr error) error {
	if lastErr == nil {
		return fmt.Errorf("client: group %d: stopped before any attempt: %w", group, ctxErr)
	}
	return fmt.Errorf("client: group %d: stopped by the caller's context: %w; last attempt: %w", group, ctxErr, lastErr)
}
