package raft

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// This test checks the invariant that a node's very first startup — no
// state file exists yet — gets term=0, votedFor=NoVote, not an error.
func TestLoadState_FreshFile_ReturnsZeroValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.dat")

	s, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState on a nonexistent file: %v", err)
	}
	if s.CurrentTerm() != 0 {
		t.Fatalf("CurrentTerm() = %d, want 0", s.CurrentTerm())
	}
	if s.VotedFor() != NoVote {
		t.Fatalf("VotedFor() = %d, want NoVote (%d)", s.VotedFor(), NoVote)
	}
}

// This test checks the invariant that persisted state survives a close and
// reopen — the basic durability contract this file exists to provide.
func TestSaveThenLoad_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.dat")

	s1, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if err := s1.Save(5, 2); err != nil {
		t.Fatalf("Save: %v", err)
	}

	s2, err := LoadState(path)
	if err != nil {
		t.Fatalf("reopen LoadState: %v", err)
	}
	if s2.CurrentTerm() != 5 {
		t.Fatalf("reopened CurrentTerm() = %d, want 5", s2.CurrentTerm())
	}
	if s2.VotedFor() != 2 {
		t.Fatalf("reopened VotedFor() = %d, want 2", s2.VotedFor())
	}
}

// This test checks the invariant that Save updates in-memory state
// immediately, not only on disk — a caller must be able to read back what
// it just saved without a reload.
func TestSave_UpdatesInMemoryState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.dat")
	s, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}

	if err := s.Save(3, 1); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if s.CurrentTerm() != 3 || s.VotedFor() != 1 {
		t.Fatalf("after Save: CurrentTerm()=%d VotedFor()=%d, want 3, 1", s.CurrentTerm(), s.VotedFor())
	}
}

// This test checks the invariant that a state file which isn't exactly 16
// bytes is treated as corruption and rejected with an error — this file is
// always rewritten whole, never appended to, so any other size can't be a
// safe, explainable truncation the way a WAL's trailing bytes can be.
func TestLoadState_CorruptFile_ReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.dat")
	if err := os.WriteFile(path, []byte{1, 2, 3}, 0644); err != nil {
		t.Fatalf("failed to write corrupt state file: %v", err)
	}

	_, err := LoadState(path)
	if err == nil {
		t.Fatalf("LoadState on a %d-byte file: err = nil, want an error", 3)
	}
}

// This test checks the invariant that successive Save calls each fully
// replace the prior state — later calls win, matching how a node's term
// only ever advances (a new election, a higher term observed elsewhere).
func TestSave_MultipleWrites_LastOneWins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.dat")
	s, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}

	if err := s.Save(1, 1); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	if err := s.Save(2, 2); err != nil {
		t.Fatalf("second Save: %v", err)
	}
	if err := s.Save(5, 3); err != nil {
		t.Fatalf("third Save: %v", err)
	}

	reloaded, err := LoadState(path)
	if err != nil {
		t.Fatalf("reopen LoadState: %v", err)
	}
	if reloaded.CurrentTerm() != 5 || reloaded.VotedFor() != 3 {
		t.Fatalf("after 3 Saves: CurrentTerm()=%d VotedFor()=%d, want 5, 3", reloaded.CurrentTerm(), reloaded.VotedFor())
	}
}

// This test checks the invariant that concurrent Save calls don't corrupt
// the state file — mu must actually serialize the temp-file+rename
// sequence, not just the in-memory field updates.
func TestSave_ConcurrentCalls_NoRace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.dat")
	s, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := uint64(i)
		go func() {
			defer wg.Done()
			_ = s.Save(i, i)
		}()
	}
	wg.Wait()

	// No assertion on WHICH save won (concurrent saves have no defined
	// order) — the property under test is that the file is left in SOME
	// valid, fully-formed state, not a torn mix of two different saves.
	reloaded, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState after concurrent Saves: %v — file was corrupted", err)
	}
	if reloaded.VotedFor() != reloaded.CurrentTerm() {
		t.Fatalf("state is internally inconsistent after concurrent Saves: term=%d votedFor=%d (every Save call used equal values, so these must match)", reloaded.CurrentTerm(), reloaded.VotedFor())
	}
}

// This test checks the invariant that a FAILED Save does not update
// in-memory state — a node that believes it voted, with no durable record
// of it, could vote again in the same term after a restart. Forces a
// failure by making the containing directory read-only, so os.CreateTemp
// cannot create the temp file at all.
func TestSave_DiskFailure_DoesNotUpdateMemory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft-state.dat")

	s, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if err := s.Save(1, 1); err != nil {
		t.Fatalf("initial Save: %v", err)
	}

	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatalf("failed to make dir read-only: %v", err)
	}
	defer os.Chmod(dir, 0700) // restore so t.TempDir() cleanup can remove it

	err = s.Save(2, 2)
	if err == nil {
		t.Fatalf("Save into a read-only directory: err = nil, want an error")
	}

	if s.CurrentTerm() != 1 || s.VotedFor() != 1 {
		t.Fatalf("after a FAILED Save: CurrentTerm()=%d VotedFor()=%d, want unchanged (1, 1) — memory was updated despite the disk write failing", s.CurrentTerm(), s.VotedFor())
	}
}
