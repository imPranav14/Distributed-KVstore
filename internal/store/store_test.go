package store

import (
	"strconv"
	"sync"
	"testing"
)

// This test checks the invariant that a key which was never Put returns
// found=false, not a zero-value string mistaken for a real stored value.
func TestGet_MissingKey_ReturnsFalse(t *testing.T) {
	s := NewStore()

	value, found := s.Get("does-not-exist")

	if found {
		t.Fatalf("Get on missing key: found = true, want false")
	}
	if value != "" {
		t.Fatalf("Get on missing key: value = %q, want empty string", value)
	}
}

// This test checks the invariant that Put makes a value immediately visible
// to a subsequent Get — the most basic read-your-writes guarantee this
// single-node store must provide.
func TestPutThenGet_ReturnsStoredValue(t *testing.T) {
	s := NewStore()

	s.Put("user:123", "alice")
	value, found := s.Get("user:123")

	if !found {
		t.Fatalf("Get after Put: found = false, want true")
	}
	if value != "alice" {
		t.Fatalf("Get after Put: value = %q, want %q", value, "alice")
	}
}

// This test checks the invariant that Put on an existing key overwrites the
// old value rather than being rejected or somehow keeping both — the store
// always reflects the most recent Put for a given key.
func TestPut_OverwritesExistingValue(t *testing.T) {
	s := NewStore()

	s.Put("user:123", "alice")
	s.Put("user:123", "bob")
	value, _ := s.Get("user:123")

	if value != "bob" {
		t.Fatalf("Get after overwriting Put: value = %q, want %q", value, "bob")
	}
}

// This test checks the invariant that Delete actually removes the key —
// after deletion, Get must report found=false, not a stale or empty value
// that could be confused with a real one.
func TestDelete_RemovesKey(t *testing.T) {
	s := NewStore()
	s.Put("user:123", "alice")

	s.Delete("user:123")
	_, found := s.Get("user:123")

	if found {
		t.Fatalf("Get after Delete: found = true, want false")
	}
}

// This test checks the invariant that Delete is idempotent: deleting a key
// that was never present (or already deleted) must not panic or error. This
// matters later because a crashed node replaying its WAL, or re-applying a
// Raft log entry, may run the same Delete more than once.
func TestDelete_MissingKey_NoOp(t *testing.T) {
	s := NewStore()

	// The test's assertion is simply that this does not panic.
	s.Delete("never-existed")

	_, found := s.Get("never-existed")
	if found {
		t.Fatalf("Get after deleting a missing key: found = true, want false")
	}
}

// This test checks the invariant that mu actually protects data: many
// goroutines calling Get, Put, and Delete concurrently on overlapping keys
// must never race. It doesn't assert on final values (concurrent writes to
// the same key have no defined "winner") — it only proves via `go test
// -race` that no data race is detected, which is the property Rule 7 exists
// to guarantee.
func TestConcurrentAccess_NoRace(t *testing.T) {
	s := NewStore()

	const goroutines = 50
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			key := "key:" + strconv.Itoa(id%10) // overlapping keys across goroutines
			for i := 0; i < opsPerGoroutine; i++ {
				s.Put(key, "value")
				s.Get(key)
				s.Delete(key)
			}
		}(g)
	}

	wg.Wait()
}
