// Package dedup gives the server at-most-once semantics for client write
// requests. It knows nothing about gRPC, Raft, or the KV store's data model
// — it only knows how to recognize "I have already handled this exact
// client request" so a retried PUT or DELETE is never silently reapplied,
// which could otherwise overwrite a different client's newer write.
package dedup

import "sync"

// ClientID identifies a single client across the lifetime of its session.
// It is opaque to this package — callers decide how it's generated.
type ClientID string

// RequestID is a sequence number a client assigns to its own requests,
// increasing for each new logical request. A client retries a failed
// request by resending the SAME RequestID, never a new one, until it gets
// a response.
type RequestID uint64

// Table tracks, per client, the most recent RequestID that has been
// processed. It does not remember full history — only the latest request —
// which is sufficient as long as each client keeps at most one request in
// flight at a time (it waits for a response, or retries the same request,
// before ever sending the next one). This keeps memory bounded by the
// number of active clients rather than the number of requests ever made.
type Table struct {
	// mu protects lastSeen. It is a single global lock rather than one lock
	// per client: simpler, and there is no performance requirement yet that
	// justifies sharding it (same reasoning as store.Store's single lock).
	mu       sync.Mutex
	lastSeen map[ClientID]RequestID
}

// NewTable returns an empty, ready-to-use Table.
func NewTable() *Table {
	return &Table{
		lastSeen: make(map[ClientID]RequestID),
	}
}

// Execute runs applyFn at most once for a given (clientID, requestID) pair.
//
// If requestID has already been processed for this client — either it's an
// exact retry of the most recent request, or it's an even older, stale
// retry the client has already moved on from — Execute returns
// wasDuplicate=true without calling applyFn. Otherwise it calls applyFn.
//
// INVARIANT: requestID is recorded as seen ONLY if applyFn succeeds (returns
// a nil error). If applyFn fails — e.g. Milestone 4's WAL write hitting a
// disk error — this request must remain retryable: the operation never
// actually happened, so a client resending the same requestID must be
// treated as fresh work next time, not silently swallowed as "already
// handled." Recording lastSeen on a failed attempt would make a failed
// write permanently unretryable, which is a correctness bug, not a
// performance one.
//
// INVARIANT: applyFn runs while mu is held. This is what closes the race
// window that would otherwise exist if callers had to call a separate
// Check(...) followed later by a separate Record(...): two goroutines could
// both see "not a duplicate" before either recorded it, and both would
// apply the operation. Running applyFn inside the same critical section
// makes "check" and "apply and record" a single atomic step.
//
// Because of this, applyFn MUST be fast (it will just be a thin call into
// store.Put/Delete plus a wal.Append) and MUST NOT call back into this same
// Table — Go's sync.Mutex is not reentrant, so that would deadlock every
// time, not intermittently. Lock ordering elsewhere in the codebase must
// always acquire this Table's lock before store.Store's lock, never the
// reverse, to avoid a lock-ordering cycle between the two.
func (t *Table) Execute(clientID ClientID, requestID RequestID, applyFn func() error) (wasDuplicate bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if last, seenBefore := t.lastSeen[clientID]; seenBefore && requestID <= last {
		// requestID == last: this is a retry of the request we already
		// handled. requestID < last: this is an even staler retry from
		// before the client's most recent request — a correctly-behaving
		// client would never still be waiting on it, so it's safe to
		// silently ignore.
		return true, nil
	}

	if err := applyFn(); err != nil {
		return false, err
	}

	t.lastSeen[clientID] = requestID
	return false, nil
}

// Snapshot returns a copy of the latest applied RequestID for every client —
// the dedup state that must travel inside a state machine snapshot.
func (t *Table) Snapshot() map[ClientID]RequestID {
	t.mu.Lock()
	defer t.mu.Unlock()

	copied := make(map[ClientID]RequestID, len(t.lastSeen))
	for clientID, requestID := range t.lastSeen {
		copied[clientID] = requestID
	}
	return copied
}

// Restore replaces the table's entire contents with lastSeen, discarding
// whatever it held before.
func (t *Table) Restore(lastSeen map[ClientID]RequestID) {
	copied := make(map[ClientID]RequestID, len(lastSeen))
	for clientID, requestID := range lastSeen {
		copied[clientID] = requestID
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastSeen = copied
}
