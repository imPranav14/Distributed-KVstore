package dedup

import (
	"sync"
	"sync/atomic"
	"testing"
)

// This test checks the invariant that a client's very first request is
// always treated as new work — a Table that has never seen a client before
// must not mistake that for a duplicate.
func TestExecute_NewClientFirstRequest_Applies(t *testing.T) {
	table := NewTable()
	applied := false

	wasDuplicate := table.Execute("client-A", 1, func() {
		applied = true
	})

	if wasDuplicate {
		t.Fatalf("first-ever request: wasDuplicate = true, want false")
	}
	if !applied {
		t.Fatalf("first-ever request: applyFn was not called")
	}
}

// This test checks the invariant that retrying the exact same RequestID
// does not reapply the operation — the core at-most-once guarantee this
// package exists to provide.
func TestExecute_DuplicateRequest_SameID_DoesNotReapply(t *testing.T) {
	table := NewTable()
	applyCount := 0

	table.Execute("client-A", 1, func() { applyCount++ })
	wasDuplicate := table.Execute("client-A", 1, func() { applyCount++ })

	if !wasDuplicate {
		t.Fatalf("retried request: wasDuplicate = false, want true")
	}
	if applyCount != 1 {
		t.Fatalf("retried request: applyFn ran %d times, want exactly 1", applyCount)
	}
}

// This test checks the invariant that a genuinely new (larger) RequestID
// from an already-known client is treated as new work, not a duplicate —
// a client must be able to make progress past its first request.
func TestExecute_NewerRequestID_Applies(t *testing.T) {
	table := NewTable()
	applyCount := 0

	table.Execute("client-A", 1, func() { applyCount++ })
	wasDuplicate := table.Execute("client-A", 2, func() { applyCount++ })

	if wasDuplicate {
		t.Fatalf("newer request: wasDuplicate = true, want false")
	}
	if applyCount != 2 {
		t.Fatalf("newer request: applyFn ran %d times, want exactly 2", applyCount)
	}
}

// This test checks the invariant that a RequestID older than the latest one
// recorded for a client is also treated as a stale duplicate, even though
// it doesn't exactly equal the last-seen RequestID. A correctly-behaving
// client would never still be waiting on a response to a request older
// than the one it has already moved past.
func TestExecute_StaleOlderRequestID_DoesNotReapply(t *testing.T) {
	table := NewTable()
	applyCount := 0

	table.Execute("client-A", 5, func() { applyCount++ })
	wasDuplicate := table.Execute("client-A", 3, func() { applyCount++ })

	if !wasDuplicate {
		t.Fatalf("stale older request: wasDuplicate = false, want true")
	}
	if applyCount != 1 {
		t.Fatalf("stale older request: applyFn ran %d times, want exactly 1", applyCount)
	}
}

// This test checks the invariant that dedup state is tracked independently
// per client — two different clients reusing the same RequestID numbering
// (e.g. both starting at 1) must not be confused with each other.
func TestExecute_DifferentClients_IndependentTracking(t *testing.T) {
	table := NewTable()
	applyCount := 0

	wasDuplicateA := table.Execute("client-A", 1, func() { applyCount++ })
	wasDuplicateB := table.Execute("client-B", 1, func() { applyCount++ })

	if wasDuplicateA {
		t.Fatalf("client-A's first request: wasDuplicate = true, want false")
	}
	if wasDuplicateB {
		t.Fatalf("client-B's first request: wasDuplicate = true, want false — different client's request ID space must not collide")
	}
	if applyCount != 2 {
		t.Fatalf("applyFn ran %d times across two independent clients, want exactly 2", applyCount)
	}
}

// This test checks the invariant that closes the race window Execute exists
// to prevent: many goroutines racing to submit the SAME (clientID,
// requestID) concurrently must still result in applyFn running exactly
// once. If Execute only checked-then-recorded as two separate steps, this
// test would be expected to fail under -race and likely show applyCount > 1.
func TestExecute_ConcurrentSameRequest_AppliesExactlyOnce(t *testing.T) {
	table := NewTable()
	var applyCount int64

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			table.Execute("client-A", 1, func() {
				atomic.AddInt64(&applyCount, 1)
			})
		}()
	}

	wg.Wait()

	if applyCount != 1 {
		t.Fatalf("applyFn ran %d times across %d concurrent identical requests, want exactly 1", applyCount, goroutines)
	}
}
