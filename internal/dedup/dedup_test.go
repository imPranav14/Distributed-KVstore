package dedup

import (
	"errors"
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

	wasDuplicate, err := table.Execute("client-A", 1, func() error {
		applied = true
		return nil
	})

	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
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

	if _, err := table.Execute("client-A", 1, func() error { applyCount++; return nil }); err != nil {
		t.Fatalf("first Execute returned error: %v", err)
	}
	wasDuplicate, err := table.Execute("client-A", 1, func() error { applyCount++; return nil })
	if err != nil {
		t.Fatalf("retried Execute returned error: %v", err)
	}

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

	if _, err := table.Execute("client-A", 1, func() error { applyCount++; return nil }); err != nil {
		t.Fatalf("first Execute returned error: %v", err)
	}
	wasDuplicate, err := table.Execute("client-A", 2, func() error { applyCount++; return nil })
	if err != nil {
		t.Fatalf("second Execute returned error: %v", err)
	}

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

	if _, err := table.Execute("client-A", 5, func() error { applyCount++; return nil }); err != nil {
		t.Fatalf("first Execute returned error: %v", err)
	}
	wasDuplicate, err := table.Execute("client-A", 3, func() error { applyCount++; return nil })
	if err != nil {
		t.Fatalf("stale Execute returned error: %v", err)
	}

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

	wasDuplicateA, errA := table.Execute("client-A", 1, func() error { applyCount++; return nil })
	wasDuplicateB, errB := table.Execute("client-B", 1, func() error { applyCount++; return nil })

	if errA != nil || errB != nil {
		t.Fatalf("Execute returned error: A=%v B=%v", errA, errB)
	}
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
			_, _ = table.Execute("client-A", 1, func() error {
				atomic.AddInt64(&applyCount, 1)
				return nil
			})
		}()
	}

	wg.Wait()

	if applyCount != 1 {
		t.Fatalf("applyFn ran %d times across %d concurrent identical requests, want exactly 1", applyCount, goroutines)
	}
}

// This test checks the invariant introduced in Milestone 4: if applyFn
// fails, requestID must NOT be recorded as seen. A failed operation never
// actually happened, so a client retrying that exact requestID afterward
// must be treated as fresh work, not silently swallowed as a duplicate —
// otherwise a failed write would become permanently unretryable.
func TestExecute_ApplyFnFails_NotRecordedAsSeen(t *testing.T) {
	table := NewTable()
	applyCount := 0
	failFirst := true
	wantErr := errors.New("simulated disk failure")

	wasDuplicate1, err1 := table.Execute("client-A", 1, func() error {
		applyCount++
		if failFirst {
			return wantErr
		}
		return nil
	})
	if !errors.Is(err1, wantErr) {
		t.Fatalf("first Execute: err = %v, want %v", err1, wantErr)
	}
	if wasDuplicate1 {
		t.Fatalf("first Execute (failed): wasDuplicate = true, want false")
	}

	// Retry the SAME requestID, this time succeeding.
	failFirst = false
	wasDuplicate2, err2 := table.Execute("client-A", 1, func() error {
		applyCount++
		return nil
	})
	if err2 != nil {
		t.Fatalf("retried Execute: unexpected error: %v", err2)
	}
	if wasDuplicate2 {
		t.Fatalf("retried Execute after a prior failure: wasDuplicate = true, want false — the failed attempt must not have been recorded as seen")
	}
	if applyCount != 2 {
		t.Fatalf("applyFn ran %d times, want exactly 2 (one failed attempt, one successful retry)", applyCount)
	}
}
