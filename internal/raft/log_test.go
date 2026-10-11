package raft

import (
	"os"
	"path/filepath"
	"testing"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

func newTestLog(t *testing.T) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "raft-log.dat")
	l, err := OpenLog(path, 0, 0)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, path
}

func entry(term, index uint64, command string) *raftpb.LogEntry {
	return &raftpb.LogEntry{Term: term, Index: index, Command: []byte(command)}
}

// This test checks the invariant that an empty log reports index 0 and term
// 0 — the sentinel meaning "there is nothing before the first entry," which
// is what lets prevLogIndex=0 work without a special case.
func TestLog_Empty_ReportsZeroSentinel(t *testing.T) {
	l, _ := newTestLog(t)

	if l.LastIndex() != 0 {
		t.Fatalf("LastIndex() = %d, want 0", l.LastIndex())
	}
	if l.LastTerm() != 0 {
		t.Fatalf("LastTerm() = %d, want 0", l.LastTerm())
	}
	term, ok := l.TermAt(0)
	if !ok || term != 0 {
		t.Fatalf("TermAt(0) = (%d, %v), want (0, true) — index 0 is the always-present sentinel", term, ok)
	}
}

// This test checks the invariant that appended entries survive a close and
// reopen, in order and with identical content — the durability contract the
// whole replication protocol rests on.
func TestLog_AppendThenReopen_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log.dat")
	l, err := OpenLog(path, 0, 0)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append(entry(1, 1, "a"), entry(1, 2, "b"), entry(2, 3, "c")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenLog(path, 0, 0)
	if err != nil {
		t.Fatalf("reopen OpenLog: %v", err)
	}
	defer reopened.Close()

	if reopened.LastIndex() != 3 {
		t.Fatalf("reopened LastIndex() = %d, want 3", reopened.LastIndex())
	}
	if reopened.LastTerm() != 2 {
		t.Fatalf("reopened LastTerm() = %d, want 2", reopened.LastTerm())
	}
	got, _ := reopened.EntriesFrom(1)
	if len(got) != 3 {
		t.Fatalf("EntriesFrom(1) returned %d entries, want 3", len(got))
	}
	if string(got[2].Command) != "c" || got[2].Term != 2 || got[2].Index != 3 {
		t.Fatalf("entry 3 = %+v, want term=2 index=3 command=c", got[2])
	}
}

// This test checks the invariant that TermAt reports the right term for a
// present index and reports not-ok for one past the end — the leader's
// consistency check depends on telling those two cases apart.
func TestLog_TermAt(t *testing.T) {
	l, _ := newTestLog(t)
	if err := l.Append(entry(3, 1, "a"), entry(7, 2, "b")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if term, ok := l.TermAt(1); !ok || term != 3 {
		t.Fatalf("TermAt(1) = (%d, %v), want (3, true)", term, ok)
	}
	if term, ok := l.TermAt(2); !ok || term != 7 {
		t.Fatalf("TermAt(2) = (%d, %v), want (7, true)", term, ok)
	}
	if _, ok := l.TermAt(3); ok {
		t.Fatalf("TermAt(3) reported ok = true for an index past the end, want false")
	}
}

// This test checks the invariant that EntriesFrom returns the correct
// suffix — this is exactly what a leader ships to a lagging follower, so an
// off-by-one here would send the wrong entries.
func TestLog_EntriesFrom(t *testing.T) {
	l, _ := newTestLog(t)
	if err := l.Append(entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, ok := l.EntriesFrom(2)
	if !ok {
		t.Fatalf("EntriesFrom(2) reported compacted on a log with no snapshot")
	}
	if len(got) != 2 {
		t.Fatalf("EntriesFrom(2) returned %d entries, want 2", len(got))
	}
	if string(got[0].Command) != "b" || string(got[1].Command) != "c" {
		t.Fatalf("EntriesFrom(2) = [%q %q], want [b c]", got[0].Command, got[1].Command)
	}
	if past, ok := l.EntriesFrom(4); past != nil || !ok {
		t.Fatalf("EntriesFrom past the end = (%v, %v), want (nil, true) — empty, but NOT compacted", past, ok)
	}
}

// This test checks the invariant that truncation durably deletes the entry
// at the given index AND everything after it — a follower repairing a
// conflict must not leave any part of the bad suffix behind.
func TestLog_TruncateFrom_RemovesSuffixDurably(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log.dat")
	l, err := OpenLog(path, 0, 0)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append(entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := l.TruncateFrom(2); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}
	if l.LastIndex() != 1 {
		t.Fatalf("LastIndex() after TruncateFrom(2) = %d, want 1", l.LastIndex())
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The truncation must have hit the FILE, not just the in-memory slice.
	reopened, err := OpenLog(path, 0, 0)
	if err != nil {
		t.Fatalf("reopen OpenLog: %v", err)
	}
	defer reopened.Close()
	if reopened.LastIndex() != 1 {
		t.Fatalf("reopened LastIndex() = %d, want 1 — truncation did not reach disk", reopened.LastIndex())
	}
}

// This test checks the invariant that appending AFTER a truncation produces
// a contiguous log — the offsets bookkeeping must be rolled back too, or
// the next append would write at a stale offset and corrupt the file.
func TestLog_AppendAfterTruncate_StaysContiguous(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log.dat")
	l, err := OpenLog(path, 0, 0)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append(entry(1, 1, "a"), entry(1, 2, "old-b"), entry(1, 3, "old-c")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.TruncateFrom(2); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}
	// A new leader overwrites positions 2 and 3 with different entries.
	if err := l.Append(entry(5, 2, "new-b"), entry(5, 3, "new-c")); err != nil {
		t.Fatalf("Append after truncate: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenLog(path, 0, 0)
	if err != nil {
		t.Fatalf("reopen OpenLog: %v — file was left corrupt after truncate+append", err)
	}
	defer reopened.Close()

	got, _ := reopened.EntriesFrom(1)
	if len(got) != 3 {
		t.Fatalf("reopened log has %d entries, want 3", len(got))
	}
	if string(got[1].Command) != "new-b" || got[1].Term != 5 {
		t.Fatalf("entry 2 = %+v, want term=5 command=new-b", got[1])
	}
	if string(got[2].Command) != "new-c" || got[2].Term != 5 {
		t.Fatalf("entry 3 = %+v, want term=5 command=new-c", got[2])
	}
}

// This test checks the invariant that a torn trailing entry is not just
// ignored but PHYSICALLY DISCARDED at recovery. This is where a Raft log
// differs from the KV WAL: the log is appended to right after recovery, so
// leftover partial bytes would sit between valid entries and corrupt every
// later read.
func TestLog_TornTrailingEntry_IsPhysicallyDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log.dat")
	l, err := OpenLog(path, 0, 0)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append(entry(1, 1, "a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash midway through writing entry 2: a partial header.
	raw, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Write([]byte{0, 0, 0, 0, 0}); err != nil {
		t.Fatalf("write partial header: %v", err)
	}
	raw.Close()

	recovered, err := OpenLog(path, 0, 0)
	if err != nil {
		t.Fatalf("OpenLog with a torn tail: %v, want a clean recovery", err)
	}
	if recovered.LastIndex() != 1 {
		t.Fatalf("LastIndex() = %d, want 1 (only the complete entry)", recovered.LastIndex())
	}

	// The critical part: appending now must produce a readable log. If the
	// partial bytes were merely skipped rather than truncated away, this
	// entry would land after them and the next reopen would fail.
	if err := recovered.Append(entry(2, 2, "b")); err != nil {
		t.Fatalf("Append after torn-tail recovery: %v", err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	final, err := OpenLog(path, 0, 0)
	if err != nil {
		t.Fatalf("reopen after torn-tail recovery + append: %v — partial bytes were not discarded", err)
	}
	defer final.Close()
	if final.LastIndex() != 2 {
		t.Fatalf("final LastIndex() = %d, want 2", final.LastIndex())
	}
}

// This test checks the invariant that a non-contiguous log (a gap or
// duplicate index) is rejected as corruption rather than silently loaded —
// unlike a torn tail, this cannot be explained by an interrupted write.
func TestLog_NonContiguousIndex_ReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log.dat")
	l, err := OpenLog(path, 0, 0)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	// Index 2 written first: the file's first entry claims index 2, not 1.
	if err := l.Append(entry(1, 2, "wrong")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	l.Close()

	if _, err := OpenLog(path, 0, 0); err == nil {
		t.Fatalf("OpenLog on a non-contiguous log: err = nil, want an error")
	}
}

// This test checks the election restriction from the Raft paper (5.4.1),
// which becomes real this milestone: a candidate's log is up-to-date if its
// last term is higher, or if terms tie and its log is at least as long.
// This is what prevents electing a leader that is missing committed entries.
func TestLog_IsUpToDate(t *testing.T) {
	l, _ := newTestLog(t)
	// Our log: index 2, last term 4.
	if err := l.Append(entry(2, 1, "a"), entry(4, 2, "b")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	tests := []struct {
		name         string
		candIndex    uint64
		candTerm     uint64
		wantUpToDate bool
	}{
		{"higher last term wins even with a shorter log", 1, 5, true},
		{"lower last term loses even with a longer log", 99, 3, false},
		{"same term, longer log wins", 3, 4, true},
		{"same term, same length ties and is allowed", 2, 4, true},
		{"same term, shorter log loses", 1, 4, false},
		{"empty candidate log against our non-empty log loses", 0, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := l.IsUpToDate(tt.candIndex, tt.candTerm); got != tt.wantUpToDate {
				t.Fatalf("IsUpToDate(index=%d, term=%d) = %v, want %v", tt.candIndex, tt.candTerm, got, tt.wantUpToDate)
			}
		})
	}
}

// This test checks the invariant that a candidate with an empty log is
// still granted a vote by a node whose log is ALSO empty — otherwise a
// brand-new cluster could never elect its first leader.
func TestLog_IsUpToDate_BothEmpty_AllowsFirstElection(t *testing.T) {
	l, _ := newTestLog(t)
	if !l.IsUpToDate(0, 0) {
		t.Fatalf("IsUpToDate(0, 0) on an empty log = false, want true — a fresh cluster could never elect a leader")
	}
}
