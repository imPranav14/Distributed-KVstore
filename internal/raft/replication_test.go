package raft

import (
	"testing"
	"time"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// seedLog appends entries directly to a node's log, bypassing the RPC path,
// to set up a specific starting state for a test.
func seedLog(t *testing.T, n *Node, entries ...*raftpb.LogEntry) {
	t.Helper()
	if err := n.log.Append(entries...); err != nil {
		t.Fatalf("seeding log: %v", err)
	}
}

// logCommands returns each entry's command as a string, for readable
// assertions about the log's exact contents.
func logCommands(n *Node) []string {
	entries, _ := n.log.EntriesFrom(n.log.SnapshotIndex() + 1)
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = string(e.Command)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// This test checks the invariant that an empty log accepts entries at
// PrevLogIndex=0 — the sentinel case that must work without a special
// branch, or a fresh cluster could never replicate its first entry.
func TestAppendEntries_EmptyLogAcceptsFirstEntries(t *testing.T) {
	n := newTestNode(t, 1)

	resp := n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []*raftpb.LogEntry{
			{Term: 1, Index: 1, Command: []byte("a")},
			{Term: 1, Index: 2, Command: []byte("b")},
		},
	})

	if !resp.Success {
		t.Fatalf("Success = false, want true for an empty log at PrevLogIndex=0")
	}
	if got := logCommands(n); !equalStrings(got, []string{"a", "b"}) {
		t.Fatalf("log = %v, want [a b]", got)
	}
}

// This test checks the Log Matching consistency check: an AppendEntries
// whose PrevLogIndex does not exist in our log must be REFUSED, so the
// leader backs up rather than punching a hole in our log.
func TestAppendEntries_RejectsWhenPrevLogIndexMissing(t *testing.T) {
	n := newTestNode(t, 1)
	seedLog(t, n, &raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")})

	// Leader thinks we have 5 entries; we have 1.
	resp := n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 5, PrevLogTerm: 1,
		Entries: []*raftpb.LogEntry{{Term: 1, Index: 6, Command: []byte("f")}},
	})

	if resp.Success {
		t.Fatalf("Success = true, want false — we have no entry at index 5")
	}
	if got := logCommands(n); !equalStrings(got, []string{"a"}) {
		t.Fatalf("log = %v, want [a] unchanged — a rejected AppendEntries must not modify the log", got)
	}
}

// This test checks the other half of the consistency check: the entry at
// PrevLogIndex exists but has the WRONG TERM, meaning our logs diverged
// before that point. Must be refused so the leader backs up further.
func TestAppendEntries_RejectsWhenPrevLogTermMismatches(t *testing.T) {
	n := newTestNode(t, 1)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 2, Index: 2, Command: []byte("b")},
	)

	// We have term 2 at index 2; leader claims term 3 there.
	resp := n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 5, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 3,
		Entries: []*raftpb.LogEntry{{Term: 5, Index: 3, Command: []byte("c")}},
	})

	if resp.Success {
		t.Fatalf("Success = true, want false — term at index 2 does not match")
	}
	if got := logCommands(n); !equalStrings(got, []string{"a", "b"}) {
		t.Fatalf("log = %v, want [a b] unchanged", got)
	}
}

// This test checks the conflict rule: an existing entry with the same index
// but a DIFFERENT term must be deleted along with everything after it, then
// replaced by the leader's version. This is how a follower that followed a
// since-deposed leader gets repaired.
func TestAppendEntries_ConflictingEntryTruncatesSuffix(t *testing.T) {
	n := newTestNode(t, 1)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 2, Index: 2, Command: []byte("stale-b")},
		&raftpb.LogEntry{Term: 2, Index: 3, Command: []byte("stale-c")},
	)

	// New leader in term 5 has different entries at 2 and 3.
	resp := n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 5, LeaderId: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*raftpb.LogEntry{
			{Term: 5, Index: 2, Command: []byte("new-b")},
			{Term: 5, Index: 3, Command: []byte("new-c")},
		},
	})

	if !resp.Success {
		t.Fatalf("Success = false, want true")
	}
	if got := logCommands(n); !equalStrings(got, []string{"a", "new-b", "new-c"}) {
		t.Fatalf("log = %v, want [a new-b new-c] — the conflicting suffix was not replaced", got)
	}
}

// This test checks the invariant that a DUPLICATE AppendEntries must not
// destroy entries the follower legitimately received later. The network can
// duplicate and reorder RPCs; a stale duplicate carrying entries 2-3,
// arriving after entries 4-5 were already replicated, must leave 4 and 5
// intact. A naive "truncate from PrevLogIndex+1 then append" implementation
// silently destroys committed data here.
func TestAppendEntries_DuplicateRequestDoesNotTruncateLaterEntries(t *testing.T) {
	n := newTestNode(t, 1)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 1, Index: 2, Command: []byte("b")},
		&raftpb.LogEntry{Term: 1, Index: 3, Command: []byte("c")},
		&raftpb.LogEntry{Term: 1, Index: 4, Command: []byte("d")},
		&raftpb.LogEntry{Term: 1, Index: 5, Command: []byte("e")},
	)

	// A delayed DUPLICATE of an earlier request, re-sending entries 2 and 3
	// that we already hold with matching terms.
	resp := n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*raftpb.LogEntry{
			{Term: 1, Index: 2, Command: []byte("b")},
			{Term: 1, Index: 3, Command: []byte("c")},
		},
	})

	if !resp.Success {
		t.Fatalf("Success = false, want true for an idempotent duplicate")
	}
	if got := logCommands(n); !equalStrings(got, []string{"a", "b", "c", "d", "e"}) {
		t.Fatalf("log = %v, want [a b c d e] — a duplicate request destroyed later entries", got)
	}
}

// This test checks the partial-overlap case: some incoming entries already
// match, and the rest are genuinely new. The matching prefix must be left
// alone and only the new suffix appended.
func TestAppendEntries_PartialOverlapAppendsOnlyNewEntries(t *testing.T) {
	n := newTestNode(t, 1)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 1, Index: 2, Command: []byte("b")},
	)

	resp := n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*raftpb.LogEntry{
			{Term: 1, Index: 2, Command: []byte("b")}, // already have
			{Term: 1, Index: 3, Command: []byte("c")}, // new
		},
	})

	if !resp.Success {
		t.Fatalf("Success = false, want true")
	}
	if got := logCommands(n); !equalStrings(got, []string{"a", "b", "c"}) {
		t.Fatalf("log = %v, want [a b c]", got)
	}
}

// This test checks that a follower adopts the leader's commit index, but
// never past the last entry it actually holds — commitIndex pointing beyond
// our own log would let us "commit" entries we have never seen.
func TestAppendEntries_AdvancesCommitIndexBoundedByOwnLog(t *testing.T) {
	n := newTestNode(t, 1)

	resp := n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []*raftpb.LogEntry{
			{Term: 1, Index: 1, Command: []byte("a")},
			{Term: 1, Index: 2, Command: []byte("b")},
		},
		// Leader claims to have committed through 99 — far beyond what it
		// has actually shipped us.
		LeaderCommit: 99,
	})

	if !resp.Success {
		t.Fatalf("Success = false, want true")
	}
	if n.CommitIndex() != 2 {
		t.Fatalf("CommitIndex() = %d, want 2 (capped at our last entry, not the leader's claim of 99)", n.CommitIndex())
	}
}

// This test checks that commitIndex advances on a plain heartbeat too — a
// leader that commits an entry needs a way to tell followers about it
// without shipping new entries.
func TestAppendEntries_HeartbeatAdvancesCommitIndex(t *testing.T) {
	n := newTestNode(t, 1)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 1, Index: 2, Command: []byte("b")},
	)

	resp := n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: nil, LeaderCommit: 2,
	})

	if !resp.Success {
		t.Fatalf("Success = false, want true")
	}
	if n.CommitIndex() != 2 {
		t.Fatalf("CommitIndex() = %d, want 2", n.CommitIndex())
	}
}

// This test checks that commitIndex never moves backward, even if a
// confused or delayed request reports a lower LeaderCommit — committed is a
// permanent, monotonic property.
func TestAppendEntries_CommitIndexNeverGoesBackward(t *testing.T) {
	n := newTestNode(t, 1)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		&raftpb.LogEntry{Term: 1, Index: 2, Command: []byte("b")},
	)

	n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 2,
	})
	if n.CommitIndex() != 2 {
		t.Fatalf("setup: CommitIndex() = %d, want 2", n.CommitIndex())
	}

	// A delayed request reporting an older commit index.
	n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 1,
	})

	if n.CommitIndex() != 2 {
		t.Fatalf("CommitIndex() = %d, want 2 — commit index moved backward", n.CommitIndex())
	}
}

// This test checks the invariant that entries are durably on disk before
// Success is returned. A follower that acknowledges an entry it has not
// persisted is lying to the leader, which may count that ack toward a
// majority and commit an entry that vanishes on the follower's restart.
func TestAppendEntries_EntriesAreDurableBeforeSuccess(t *testing.T) {
	n := newTestNode(t, 1)

	resp := n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []*raftpb.LogEntry{{Term: 1, Index: 1, Command: []byte("a")}},
	})
	if !resp.Success {
		t.Fatalf("Success = false, want true")
	}

	// Reopen the log file independently: if Success was returned before the
	// fsync, the entry might not be on disk at all.
	reopened, err := OpenLog(n.log.file.Name(), 0, 0)
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	defer reopened.Close()

	if reopened.LastIndex() != 1 {
		t.Fatalf("reopened log LastIndex() = %d, want 1 — Success was returned for an entry that is not durable", reopened.LastIndex())
	}
}

// This test checks that the election timer resets even on a REJECTED
// AppendEntries. A follower that is behind still heard from a legitimate
// leader; if only successful requests reset the timer, that follower would
// keep timing out and campaigning while the leader is actively repairing
// it — fighting the very repair that would fix it.
func TestAppendEntries_RejectedRequestStillResetsElectionTimer(t *testing.T) {
	n := newTestNode(t, 1)
	seedLog(t, n, &raftpb.LogEntry{Term: 1, Index: 1, Command: []byte("a")})

	n.mu.Lock()
	n.lastHeartbeat = n.lastHeartbeat.Add(-time.Hour) // pretend we haven't heard in ages
	before := n.lastHeartbeat
	n.mu.Unlock()

	// A request that WILL be rejected by the consistency check.
	resp := n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 99, PrevLogTerm: 1,
	})
	if resp.Success {
		t.Fatalf("setup: expected this request to be rejected")
	}

	n.mu.Lock()
	after := n.lastHeartbeat
	n.mu.Unlock()

	if !after.After(before) {
		t.Fatalf("lastHeartbeat was not reset by a rejected AppendEntries — the follower will campaign against a leader that is trying to repair it")
	}
}
