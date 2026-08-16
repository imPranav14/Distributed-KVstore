// Package raft implements leader election: terms, roles, randomized
// election timeouts, RequestVote, and heartbeats. Log replication
// (Milestone 6) and wiring client writes through consensus (Milestone 7)
// come later — this package elects a leader and nothing more.
package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// NoVote is the sentinel for "hasn't voted this term." Node IDs are assigned
// starting at 1 (see config.Config.NodeID), so 0 can never collide with a
// real candidate ID.
const NoVote = 0

// State holds the two pieces of Raft state that MUST survive a crash:
// currentTerm and votedFor. Both must be durably persisted before a
// RequestVote response depends on them — see Save's invariant below for
// why.
type State struct {
	// mu protects both fields together AND serializes calls to Save, since
	// a torn concurrent write to the state file would be exactly the kind
	// of corruption this type exists to prevent.
	mu          sync.Mutex
	path        string
	currentTerm uint64
	votedFor    uint64
}

// LoadState reads persistent state from path, or returns a fresh State
// (term 0, no vote) if the file doesn't exist yet — the normal case for a
// node's very first startup.
func LoadState(path string) (*State, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &State{path: path, currentTerm: 0, votedFor: NoVote}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("raft: failed to read state file %s: %w", path, err)
	}

	// This file is always rewritten whole via Save, never appended to — so
	// unlike wal.ReadAll's tolerance for a truncated FINAL entry, any size
	// other than exactly 16 bytes here is not a safe, explainable
	// truncation. It's corruption.
	if len(data) != 16 {
		return nil, fmt.Errorf("raft: state file %s has %d bytes, want 16 (corrupt)", path, len(data))
	}

	return &State{
		path:        path,
		currentTerm: binary.BigEndian.Uint64(data[0:8]),
		votedFor:    binary.BigEndian.Uint64(data[8:16]),
	}, nil
}

// CurrentTerm returns the highest term this node has observed.
func (s *State) CurrentTerm() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentTerm
}

// VotedFor returns who this node voted for in CurrentTerm(), or NoVote.
func (s *State) VotedFor() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.votedFor
}

// Save durably persists term and votedFor together, then updates memory.
//
// INVARIANT: term and votedFor are always written together, as one atomic
// fact ("I am in term 5, and in term 5 I voted for node 1"), never
// independently. A setter that could change one without the other would
// let a caller bump the term and forget to clear the vote, leaving a stale
// vote from a previous term incorrectly applying to a new one.
//
// INVARIANT: disk is updated (durably) before memory. If persistence fails,
// Save returns an error WITHOUT touching s.currentTerm/s.votedFor. A node
// that believes in memory that it already voted, but has no durable record
// of it, is exactly the state that lets it vote twice in the same term
// after a restart — violating the safety property that at most one leader
// can exist per term.
//
// Written via temp-file + rename + directory fsync rather than an in-place
// rewrite of the 16 bytes: POSIX does not guarantee a partial write can
// never be torn, and a torn write here could mix a new term with an old
// vote, silently reintroducing the double-vote bug this file exists to
// prevent. Rename-based replacement is all-or-nothing; the directory fsync
// is required because the rename itself isn't guaranteed durable without
// it, even once the temp file's own contents are.
func (s *State) Save(term uint64, votedFor uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], term)
	binary.BigEndian.PutUint64(buf[8:16], votedFor)

	dir := filepath.Dir(s.path)
	tempFile, err := os.CreateTemp(dir, "raft-state-*.tmp")
	if err != nil {
		return fmt.Errorf("raft: failed to create temp state file in %s: %w", dir, err)
	}
	tempPath := tempFile.Name()

	if _, err := tempFile.Write(buf); err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return fmt.Errorf("raft: failed to write temp state file: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return fmt.Errorf("raft: failed to fsync temp state file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("raft: failed to close temp state file: %w", err)
	}

	if err := os.Rename(tempPath, s.path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("raft: failed to rename temp state file into place: %w", err)
	}

	dirHandle, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("raft: failed to open directory %s to fsync rename: %w", dir, err)
	}
	defer dirHandle.Close()
	if err := dirHandle.Sync(); err != nil {
		return fmt.Errorf("raft: failed to fsync directory %s: %w", dir, err)
	}

	s.currentTerm = term
	s.votedFor = votedFor
	return nil
}
