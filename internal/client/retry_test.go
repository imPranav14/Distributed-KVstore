package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/imPranav14/Distributed-KVstore/internal/ring"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
)

// callLog records which fake node was called, in order, across all nodes.
type callLog struct {
	mu    sync.Mutex
	order []string
}

func (l *callLog) add(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.order = append(l.order, name)
}

func (l *callLog) calls() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.order...)
}

// fakeNode stands in for one server. respond decides each call's outcome
// from the group the request named and how many times THIS node has been
// called; nil means success.
type fakeNode struct {
	name    string
	log     *callLog
	mu      sync.Mutex
	calls   int
	respond func(ctx context.Context, group uint64, call int) error
}

func (f *fakeNode) handle(ctx context.Context, group uint64) error {
	f.mu.Lock()
	call := f.calls
	f.calls++
	f.mu.Unlock()
	f.log.add(f.name)
	return f.respond(ctx, group, call)
}

func (f *fakeNode) Get(ctx context.Context, in *kvpb.GetRequest, opts ...grpc.CallOption) (*kvpb.GetResponse, error) {
	if err := f.handle(ctx, in.GroupId); err != nil {
		return nil, err
	}
	return &kvpb.GetResponse{Value: "v", Found: true}, nil
}

func (f *fakeNode) Put(ctx context.Context, in *kvpb.PutRequest, opts ...grpc.CallOption) (*kvpb.PutResponse, error) {
	if err := f.handle(ctx, in.GroupId); err != nil {
		return nil, err
	}
	return &kvpb.PutResponse{}, nil
}

func (f *fakeNode) Delete(ctx context.Context, in *kvpb.DeleteRequest, opts ...grpc.CallOption) (*kvpb.DeleteResponse, error) {
	if err := f.handle(ctx, in.GroupId); err != nil {
		return nil, err
	}
	return &kvpb.DeleteResponse{}, nil
}

// The outcomes a real server produces.
func refuse(context.Context, uint64, int) error {
	return status.Error(codes.FailedPrecondition, "wrong group")
}
func notLeader(context.Context, uint64, int) error {
	return status.Error(codes.Unavailable, "not leader")
}
func accept(context.Context, uint64, int) error { return nil }
func hang(ctx context.Context, _ uint64, _ int) error {
	<-ctx.Done()
	return status.FromContextError(ctx.Err()).Err()
}

// fastPolicy keeps unit tests short: a 300ms budget, 5ms backoff capped at 20ms.
var fastPolicy = retryPolicy{totalBudget: 300 * time.Millisecond, baseBackoff: 5 * time.Millisecond, maxBackoff: 20 * time.Millisecond, perAttemptTimeout: 20 * time.Millisecond}

// slowBackoffPolicy makes any backoff obvious in elapsed time, to prove a
// path took none.
var slowBackoffPolicy = retryPolicy{totalBudget: 2 * time.Second, baseBackoff: 200 * time.Millisecond, maxBackoff: time.Second, perAttemptTimeout: 100 * time.Millisecond}

// endsAtTheBudget checks a call that never succeeded ran for its whole
// budget — no shorter, and not much longer.
func endsAtTheBudget(t *testing.T, elapsed time.Duration, policy retryPolicy) {
	t.Helper()
	if elapsed < policy.totalBudget || elapsed > policy.totalBudget+200*time.Millisecond {
		t.Fatalf("call took %v, want it to keep trying for its %v budget and then stop", elapsed, policy.totalBudget)
	}
}

// askedMoreThanOnce checks every named node was asked repeatedly — the
// client kept cycling instead of giving up after one rotation.
func askedMoreThanOnce(t *testing.T, calls []string, nodes ...string) {
	t.Helper()
	joined := strings.Join(calls, "")
	for _, node := range nodes {
		if strings.Count(joined, node) < 2 {
			t.Fatalf("calls %v: node %s was asked fewer than twice — the client stopped cycling", calls, node)
		}
	}
}

// newFakeClient builds a client over fake nodes named A, B, C, ...
func newFakeClient(t *testing.T, policy retryPolicy, keyRing *ring.Ring, responders ...func(context.Context, uint64, int) error) (*Client, []*fakeNode, *callLog) {
	t.Helper()
	log := &callLog{}
	nodes := make([]*fakeNode, len(responders))
	stubs := make([]kvpb.KVStoreClient, len(responders))
	for i, respond := range responders {
		nodes[i] = &fakeNode{name: string(rune('A' + i)), log: log, respond: respond}
		stubs[i] = nodes[i]
	}
	if keyRing == nil {
		keyRing = oneGroupRing(t)
	}
	return newClient(keyRing, stubs, policy), nodes, log
}

// keyInGroup finds a key the ring assigns to group.
func keyInGroup(t *testing.T, keyRing *ring.Ring, group ring.GroupID) string {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if key := fmt.Sprintf("key-%d", i); keyRing.Owner(key) == group {
			return key
		}
	}
	t.Fatalf("no key in group %d among 1000 candidates", group)
	return ""
}

// This test checks that the client remembers a leader PER GROUP. Fake node A
// leads group 1, B group 2, C group 3; each refuses leadership for the
// others. After one search per group, calls that alternate between groups
// must reach the right node first time: exactly one call each. With a single
// shared guess, every group switch would start at the previous group's
// leader and waste calls (plus a backoff) finding the right one.
func TestClient_RemembersALeaderPerGroup(t *testing.T) {
	keyRing, err := ring.New([]ring.GroupID{1, 2, 3})
	if err != nil {
		t.Fatalf("ring.New: %v", err)
	}
	leaderOf := func(group uint64) func(context.Context, uint64, int) error {
		return func(_ context.Context, requested uint64, _ int) error {
			if requested == group {
				return nil
			}
			return status.Error(codes.Unavailable, "not leader")
		}
	}
	c, _, log := newFakeClient(t, fastPolicy, keyRing, leaderOf(1), leaderOf(2), leaderOf(3))
	keys := []string{keyInGroup(t, keyRing, 1), keyInGroup(t, keyRing, 2), keyInGroup(t, keyRing, 3)}
	ctx := context.Background()

	for _, key := range keys { // one search per group
		if err := c.Put(ctx, key, "v"); err != nil {
			t.Fatalf("warm-up Put(%s): %v", key, err)
		}
	}
	callsBefore := len(log.calls())
	for i := 0; i < 30; i++ {
		if err := c.Put(ctx, keys[i%3], "v"); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if wasted := len(log.calls()) - callsBefore - 30; wasted != 0 {
		t.Fatalf("30 calls alternating between groups made %d extra attempts, want 0", wasted)
	}
}

// This test checks the fail-fast path: when EVERY node refuses the group,
// the client's own ring is the one that disagrees. Waiting cannot fix a
// config mismatch, so the client asks each node exactly once, with no
// backoff, and returns ErrRingMismatch.
func TestClient_EveryNodeRefuses_FailsFastWithRingMismatch(t *testing.T) {
	c, _, log := newFakeClient(t, slowBackoffPolicy, nil, refuse, refuse, refuse)

	start := time.Now()
	err := c.Put(context.Background(), "x", "v")
	elapsed := time.Since(start)

	if !errors.Is(err, ErrRingMismatch) {
		t.Fatalf("err = %v, want ErrRingMismatch", err)
	}
	if got := log.calls(); strings.Join(got, "") != "ABC" {
		t.Fatalf("calls = %v, want each node exactly once: [A B C]", got)
	}
	if elapsed >= slowBackoffPolicy.baseBackoff/2 {
		t.Fatalf("took %v — a refusal must not back off", elapsed)
	}
}

// This test checks the case one misconfigured SERVER creates: it refuses,
// but the next node agrees with the client. The client must move on at once
// — no backoff — and succeed.
func TestClient_OneNodeRefuses_NextIsTriedWithoutBackoff(t *testing.T) {
	c, _, log := newFakeClient(t, slowBackoffPolicy, nil, refuse, accept, accept)

	start := time.Now()
	if err := c.Put(context.Background(), "x", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := log.calls(); strings.Join(got, "") != "AB" {
		t.Fatalf("calls = %v, want [A B]", got)
	}
	if elapsed := time.Since(start); elapsed >= slowBackoffPolicy.baseBackoff/2 {
		t.Fatalf("took %v — moving past a refusal must not back off", elapsed)
	}
}

// This test checks that NotLeader is NOT a routing refusal. The server
// checks ownership before leadership, so NotLeader means that node's ring
// AGREES with ours — it must stay eligible, because it may become leader.
// A and C refuse; B is not leader at first, then is. The client must go
// A → B → C → B and succeed: never revisiting A or C, and never concluding
// its own ring is wrong while B agrees. Counting refusals instead of
// distinct refusing nodes would give up at A's second refusal.
func TestClient_NotLeaderIsNotARefusal(t *testing.T) {
	notLeaderOnce := func(_ context.Context, _ uint64, call int) error {
		if call == 0 {
			return status.Error(codes.Unavailable, "not leader")
		}
		return nil
	}
	c, _, log := newFakeClient(t, fastPolicy, nil, refuse, notLeaderOnce, refuse)

	if err := c.Put(context.Background(), "x", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := log.calls(); strings.Join(got, "") != "ABCB" {
		t.Fatalf("calls = %v, want [A B C B]", got)
	}
}

// This test checks that ONE budget bounds every path, and that a refused
// node is never asked again within a call. A refuses; B and C never have a
// leader. The client keeps cycling B and C for the whole budget, never asks
// A again, and reports that no leader could be reached.
func TestClient_OneBudget_RefusedNodesAreNotRevisited(t *testing.T) {
	c, _, log := newFakeClient(t, fastPolicy, nil, refuse, notLeader, notLeader)

	start := time.Now()
	err := c.Put(context.Background(), "x", "v")
	elapsed := time.Since(start)

	if !errors.Is(err, ErrNoLeaderReachable) {
		t.Fatalf("err = %v, want ErrNoLeaderReachable", err)
	}
	endsAtTheBudget(t, elapsed, fastPolicy)
	calls := log.calls()
	if strings.Count(strings.Join(calls, ""), "A") != 1 {
		t.Fatalf("calls = %v: the refusing node A was asked more than once", calls)
	}
	askedMoreThanOnce(t, calls, "B", "C")
}

// This test checks the no-majority case: a group that cannot elect a
// leader answers NotLeader everywhere. The client must stop after its
// budget and say so clearly — ErrNoLeaderReachable — while keeping the last
// gRPC error inspectable.
func TestClient_NoLeaderAnywhere_StopsWithNoLeaderReachable(t *testing.T) {
	c, _, log := newFakeClient(t, fastPolicy, nil, notLeader, notLeader, notLeader)

	start := time.Now()
	err := c.Put(context.Background(), "x", "v")
	elapsed := time.Since(start)

	if !errors.Is(err, ErrNoLeaderReachable) {
		t.Fatalf("err = %v, want ErrNoLeaderReachable", err)
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("status.Code(err) = %v, want the last attempt's Unavailable to stay inspectable", status.Code(err))
	}
	endsAtTheBudget(t, elapsed, fastPolicy)
	askedMoreThanOnce(t, log.calls(), "A", "B", "C")
}

// This test checks the timeout path: every node hangs until the ATTEMPT's
// own timeout fires. That is the node's failure, not the caller's, so it is
// leader discovery — and still bounded by the same budget.
func TestClient_EveryAttemptTimesOut_StopsWithinTheBudget(t *testing.T) {
	c, _, log := newFakeClient(t, fastPolicy, nil, hang, hang, hang)

	start := time.Now()
	err := c.Put(context.Background(), "x", "v")
	elapsed := time.Since(start)

	if !errors.Is(err, ErrNoLeaderReachable) || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("err = %v, want ErrNoLeaderReachable wrapping DeadlineExceeded", err)
	}
	endsAtTheBudget(t, elapsed, fastPolicy)
	askedMoreThanOnce(t, log.calls(), "A", "B", "C")
}

// This test checks that the caller's deadline wins over the attempt budget:
// with a 50ms deadline and 200ms backoffs, the client returns at about the
// deadline, after fewer than maxAttempts calls, with an error the caller
// recognises as its own deadline.
func TestClient_CallerDeadlineShorterThanBudget_ReturnsAtTheDeadline(t *testing.T) {
	c, _, log := newFakeClient(t, slowBackoffPolicy, nil, notLeader, notLeader, notLeader)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := c.Put(ctx, "x", "v")
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrNoLeaderReachable) {
		t.Fatalf("err = %v, want the caller's context.DeadlineExceeded, not ErrNoLeaderReachable", err)
	}
	if len(log.calls()) > 1 {
		t.Fatalf("made %d calls — the 50ms deadline should end the call during its first 200ms backoff", len(log.calls()))
	}
	if elapsed > 150*time.Millisecond {
		t.Fatalf("took %v, want about the 50ms deadline", elapsed)
	}
}

// This test checks that when the caller's deadline expires DURING an
// attempt, the error says so — rather than blaming the cluster with
// ErrNoLeaderReachable. The node never failed; the caller stopped waiting.
func TestClient_CallerDeadlineMidAttempt_IsReportedAsTheCallers(t *testing.T) {
	policy := retryPolicy{totalBudget: time.Second, baseBackoff: time.Millisecond, maxBackoff: time.Millisecond, perAttemptTimeout: time.Second}
	c, _, _ := newFakeClient(t, policy, nil, hang)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := c.Put(ctx, "x", "v")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if errors.Is(err, ErrNoLeaderReachable) {
		t.Fatalf("err = %v: the caller's own deadline was blamed on the cluster", err)
	}
}

// This test checks the converse: when OUR budget expires during an attempt
// and the caller set no deadline, the error is ErrNoLeaderReachable — not
// context.DeadlineExceeded, which would tell the caller its own deadline
// expired when it never set one.
func TestClient_OurBudgetMidAttempt_IsReportedAsNoLeaderReachable(t *testing.T) {
	policy := retryPolicy{totalBudget: 30 * time.Millisecond, baseBackoff: time.Millisecond, maxBackoff: time.Millisecond, perAttemptTimeout: time.Second}
	c, _, _ := newFakeClient(t, policy, nil, hang)

	err := c.Put(context.Background(), "x", "v")
	if !errors.Is(err, ErrNoLeaderReachable) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want ErrNoLeaderReachable and NOT the caller's context.DeadlineExceeded", err)
	}
}

// This test checks that an already-cancelled context makes no call at all.
func TestClient_AlreadyCancelled_MakesNoCall(t *testing.T) {
	c, _, log := newFakeClient(t, fastPolicy, nil, accept)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := c.Put(ctx, "x", "v"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want it to wrap context.Canceled", err)
	}
	if calls := log.calls(); len(calls) != 0 {
		t.Fatalf("made calls %v on a cancelled context, want none", calls)
	}
}

// This test checks that a permanent error — one no node and no amount of
// waiting can change — is returned at once instead of spending the budget.
func TestClient_PermanentError_IsReturnedWithoutRetrying(t *testing.T) {
	invalid := func(context.Context, uint64, int) error {
		return status.Error(codes.InvalidArgument, "malformed request")
	}
	c, _, log := newFakeClient(t, fastPolicy, nil, invalid, accept, accept)

	err := c.Put(context.Background(), "x", "v")
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument returned as is", err)
	}
	if calls := log.calls(); len(calls) != 1 {
		t.Fatalf("made calls %v, want exactly one", calls)
	}
}

// This test checks the bug the integration tests found: a leader elected
// LATE in a call must still be found. A and B are never leader; C becomes
// leader 150ms into the call — as after a split vote. A budget counted in
// attempts, spent round-robin with growing gaps, visits C once before it
// wins and never again, and reports "no leader reachable" while a leader
// exists. A budget counted in time, with short capped backoff, keeps
// revisiting every node until the budget ends.
func TestClient_LeaderElectedLateIsStillFound(t *testing.T) {
	callStarted := time.Now()
	leaderAfter150ms := func(context.Context, uint64, int) error {
		if time.Since(callStarted) < 150*time.Millisecond {
			return status.Error(codes.Unavailable, "not leader")
		}
		return nil
	}
	c, _, log := newFakeClient(t, fastPolicy, nil, notLeader, notLeader, leaderAfter150ms)

	callStarted = time.Now()
	if err := c.Put(context.Background(), "x", "v"); err != nil {
		t.Fatalf("Put: %v (calls %v) — a leader elected 150ms in was never found", err, log.calls())
	}
}
