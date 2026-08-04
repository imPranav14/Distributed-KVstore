// Package store implements the in-memory key-value state machine for a
// single node. It knows nothing about Raft, gRPC, or disk persistence — it
// is deliberately the simplest possible thing that can hold data, so it can
// be trusted and tested in isolation before anything else in the project
// depends on it.
package store

import "sync"

// Store is a concurrency-safe map of string keys to string values.
//
// INVARIANT: data must never be read or written without holding mu. Get is
// called from gRPC read-handler goroutines; Put and Delete will later be
// called from the Raft apply loop's goroutine. Without mu, two goroutines
// writing to a Go map at the same time doesn't just produce a wrong answer —
// it can crash the process, since Go's map implementation panics on
// concurrent read+write.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore returns an empty, ready-to-use Store.
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Get returns the value stored at key and whether it was present. A missing
// key is a normal outcome, not an error — callers check found, not an err.
func (s *Store) Get(key string) (value string, found bool) {
	// RLock, not Lock: multiple concurrent Get calls must be allowed to
	// proceed at the same time, since none of them mutate data.
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, found = s.data[key]
	return value, found
}

// Put stores value at key, overwriting any existing value. There is no
// error return here because a map write on an in-memory Go map cannot fail.
func (s *Store) Put(key string, value string) {
	// Full Lock, not RLock: this mutates data, so it must run exclusively
	// against every other Get, Put, and Delete.
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

// Delete removes key from the store. Deleting a key that does not exist is
// not an error — it is a no-op, matching Go's built-in delete() semantics.
// This idempotency matters later: replaying a WAL or re-applying a Raft log
// entry after a crash must be safe to do more than once.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}
