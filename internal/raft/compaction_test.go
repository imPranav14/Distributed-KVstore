package raft

import (
	"os"
	"path/filepath"
	"testing"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// fiveEntryLog writes entries 1..5 with terms 1,1,2,2,3 and returns the log
// and its path.
func fiveEntryLog(t *testing.T) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "raft-log.dat")
	l, err := OpenLog(path, 0, 0)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if err := l.Append(
		entry(1, 1, "a"), entry(1, 2, "b"), entry(2, 3, "c"), entry(2, 4, "d"), entry(3, 5, "e"),
	); err != nil {
		t.Fatalf("Append: %v", err)
	}
	return l, path
}

func commands(entries []*raftpb.LogEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = string(e.Command)
	}
	return out
}

// This test checks every accessor on all three sides of the snapshot
// boundary: below it (compacted, unknown), at it (the sentinel, reporting
// the snapshot's term), and above it (real entries).
func TestLog_Compact_KeepsSuffixAndReportsBoundary(t *testing.T) {
	l, _ := fiveEntryLog(t)

	if err := l.Compact(3, 2); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if l.SnapshotIndex() != 3 || l.SnapshotTerm() != 2 {
		t.Fatalf("snapshot boundary = (%d, %d), want (3, 2)", l.SnapshotIndex(), l.SnapshotTerm())
	}
	if l.LastIndex() != 5 || l.LastTerm() != 3 {
		t.Fatalf("last = (%d, %d), want (5, 3) — compaction must not change the log's end", l.LastIndex(), l.LastTerm())
	}
	if _, ok := l.TermAt(2); ok {
		t.Fatalf("TermAt(2) reported a term for a compacted index")
	}
	if term, ok := l.TermAt(3); !ok || term != 2 {
		t.Fatalf("TermAt(3) = (%d, %v), want (2, true) — the boundary is the sentinel", term, ok)
	}
	if term, ok := l.TermAt(4); !ok || term != 2 {
		t.Fatalf("TermAt(4) = (%d, %v), want (2, true)", term, ok)
	}
	if _, ok := l.EntriesFrom(3); ok {
		t.Fatalf("EntriesFrom(3) reported ok — entry 3 is compacted, the caller must be told to use the snapshot")
	}
	got, ok := l.EntriesFrom(4)
	if !ok || !equalStrings(commands(got), []string{"d", "e"}) {
		t.Fatalf("EntriesFrom(4) = (%v, %v), want ([d e], true)", commands(got), ok)
	}
}

// This test checks the most important invariant in Milestone 8a: a log whose
// entries have ALL been compacted still reports the snapshot's index and
// term as its last. If it reported (0, 0), it would look empty — and would
// vote for any candidate, including one missing entries this node knows
// were committed. The election restriction would be silently off.
func TestLog_FullyCompactedLogStillRefusesStaleCandidates(t *testing.T) {
	l, _ := fiveEntryLog(t)
	if err := l.Compact(5, 3); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if l.LastIndex() != 5 || l.LastTerm() != 3 {
		t.Fatalf("fully compacted log reports last = (%d, %d), want (5, 3)", l.LastIndex(), l.LastTerm())
	}
	if l.IsUpToDate(0, 0) {
		t.Fatalf("fully compacted log considers an EMPTY candidate up to date — the election restriction is off")
	}
	if l.IsUpToDate(4, 3) {
		t.Fatalf("fully compacted log considers a shorter same-term candidate up to date")
	}
	if !l.IsUpToDate(5, 3) {
		t.Fatalf("fully compacted log refuses an equally up-to-date candidate")
	}
}

// This test checks the same guard end to end through RequestVote, using the
// candidate that actually exploits the bug: a LONG log of STALE-term entries
// (a node that sat partitioned accumulating entries no leader committed).
//
// An empty-log candidate would NOT catch it: with our last term wrongly
// reported as 0, an empty candidate ties on term and loses on length (0 < 2)
// — refused anyway, for the wrong reason. A candidate at term 1 beats a
// reported 0 outright, however short our log is. Mutation testing showed the
// empty-candidate version of this test passing against the bug.
func TestHandleRequestVote_FullyCompactedNodeRefusesLongStaleCandidate(t *testing.T) {
	n := newTestNode(t, 1)
	seedLog(t, n,
		&raftpb.LogEntry{Term: 4, Index: 1, Command: []byte("x")},
		&raftpb.LogEntry{Term: 4, Index: 2, Command: []byte("y")},
	)
	if err := n.log.Compact(2, 4); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	resp := n.HandleRequestVote(&raftpb.RequestVoteRequest{Term: 9, CandidateId: 2, LastLogIndex: 10, LastLogTerm: 1})
	if resp.VoteGranted {
		t.Fatalf("a fully compacted node (last term 4) voted for a candidate whose newest entry is from term 1")
	}
}

// This test checks that compaction is durable and that appends continue
// contiguously afterwards, across a restart with the snapshot as base.
func TestLog_Compact_SurvivesReopenAndAcceptsAppends(t *testing.T) {
	l, path := fiveEntryLog(t)
	if err := l.Compact(3, 2); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := l.Append(entry(3, 6, "f")); err != nil {
		t.Fatalf("Append after Compact: %v", err)
	}
	l.Close()

	reopened, err := OpenLog(path, 3, 2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	got, _ := reopened.EntriesFrom(4)
	if !equalStrings(commands(got), []string{"d", "e", "f"}) {
		t.Fatalf("reopened log = %v, want [d e f]", commands(got))
	}
}

// This test checks the Raft rule for a snapshot whose last entry this log
// disagrees with: nothing in the log can be trusted, so all of it goes.
// (Used by InstallSnapshot when a leader's snapshot replaces a diverged log.)
func TestLog_Compact_TermMismatchDiscardsEverything(t *testing.T) {
	l, _ := fiveEntryLog(t)

	if err := l.Compact(4, 9); err != nil { // our entry 4 has term 2, not 9
		t.Fatalf("Compact: %v", err)
	}

	if l.LastIndex() != 4 || l.LastTerm() != 9 {
		t.Fatalf("after mismatched Compact: last = (%d, %d), want (4, 9)", l.LastIndex(), l.LastTerm())
	}
	if got, _ := l.EntriesFrom(5); len(got) != 0 {
		t.Fatalf("entry 5 survived a snapshot that contradicts entry 4 — it belongs to diverged history")
	}
}

// This test checks a snapshot that extends past the end of the log (a
// far-behind follower receiving InstallSnapshot): the log becomes empty with
// its boundary at the snapshot.
func TestLog_Compact_BeyondEndDiscardsEverything(t *testing.T) {
	l, _ := fiveEntryLog(t)

	if err := l.Compact(10, 7); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if l.LastIndex() != 10 || l.LastTerm() != 7 {
		t.Fatalf("last = (%d, %d), want (10, 7)", l.LastIndex(), l.LastTerm())
	}
}

// This test checks that compacting at or below the current boundary is a
// no-op — a stale snapshot must never move the boundary backwards.
func TestLog_Compact_AtOrBelowBoundaryIsNoOp(t *testing.T) {
	l, _ := fiveEntryLog(t)
	if err := l.Compact(3, 2); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := l.Compact(2, 1); err != nil {
		t.Fatalf("stale Compact: %v", err)
	}
	if l.SnapshotIndex() != 3 || l.LastIndex() != 5 {
		t.Fatalf("stale Compact moved state: snapshot=%d last=%d, want 3 and 5", l.SnapshotIndex(), l.LastIndex())
	}
}

// This test checks that the log refuses to truncate into its snapshot.
// Everything at or below the boundary was applied, hence committed;
// removing it would mean a safety property had already failed.
func TestLog_TruncateFrom_RefusesToCutIntoSnapshot(t *testing.T) {
	l, _ := fiveEntryLog(t)
	if err := l.Compact(3, 2); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	for _, index := range []uint64{2, 3} {
		if err := l.TruncateFrom(index); err == nil {
			t.Fatalf("TruncateFrom(%d) succeeded on a log compacted through 3", index)
		}
	}
	if err := l.TruncateFrom(4); err != nil {
		t.Fatalf("TruncateFrom(4): %v — the first entry after the boundary is fair game", err)
	}
	if l.LastIndex() != 3 {
		t.Fatalf("LastIndex() = %d, want 3", l.LastIndex())
	}
}

// This test checks the crash window the brief called harmless: the node
// saved a snapshot through 3, then crashed before compacting the log. On
// restart OpenLog must drop entries 1–3 and rewrite the file, so file and
// memory agree and appends stay contiguous.
func TestOpenLog_CrashAfterSnapshotBeforeCompact_DropsCoveredEntries(t *testing.T) {
	l, path := fiveEntryLog(t)
	l.Close() // "crash": the log still holds 1..5

	recovered, err := OpenLog(path, 3, 2)
	if err != nil {
		t.Fatalf("OpenLog with a snapshot ahead of an uncompacted log: %v", err)
	}
	if recovered.LastIndex() != 5 {
		t.Fatalf("LastIndex() = %d, want 5", recovered.LastIndex())
	}
	if _, ok := recovered.EntriesFrom(3); ok {
		t.Fatalf("entry 3 still reported as present — the snapshot covers it")
	}
	if err := recovered.Append(entry(3, 6, "f")); err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	recovered.Close()

	// The file must have been REWRITTEN, not just filtered in memory: opened
	// with no snapshot, it should now start at index 4 — which is a gap.
	if _, err := OpenLog(path, 0, 0); err == nil {
		t.Fatalf("file still starts at index 1 — the interrupted compaction was never finished on disk")
	}
	final, err := OpenLog(path, 3, 2)
	if err != nil {
		t.Fatalf("reopen with snapshot: %v", err)
	}
	defer final.Close()
	got, _ := final.EntriesFrom(4)
	if !equalStrings(commands(got), []string{"d", "e", "f"}) {
		t.Fatalf("log after recovery and append = %v, want [d e f]", commands(got))
	}
}

// This test checks the same crash window when the snapshot's last entry
// CONTRADICTS the log (a snapshot from InstallSnapshot, saved just before a
// crash). Replay must apply the same rule Compact does and drop everything,
// not keep a suffix that belongs to diverged history.
func TestOpenLog_CrashWindowWithDivergedLog_DropsEverything(t *testing.T) {
	l, path := fiveEntryLog(t)
	l.Close()

	recovered, err := OpenLog(path, 3, 7) // our entry 3 has term 2
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	defer recovered.Close()
	if recovered.LastIndex() != 3 || recovered.LastTerm() != 7 {
		t.Fatalf("last = (%d, %d), want (3, 7) — the diverged suffix must not survive", recovered.LastIndex(), recovered.LastTerm())
	}
}

// This test checks that a log which starts AFTER the snapshot's end is
// rejected: the entries in between exist nowhere, and guessing would mean
// running with a hole in the replicated history.
func TestOpenLog_GapBetweenSnapshotAndLog_IsError(t *testing.T) {
	l, path := fiveEntryLog(t)
	if err := l.Compact(4, 2); err != nil { // file now holds only entry 5
		t.Fatalf("Compact: %v", err)
	}
	l.Close()

	if _, err := OpenLog(path, 2, 1); err == nil {
		t.Fatalf("OpenLog accepted a log starting at 5 after a snapshot ending at 2 — entries 3 and 4 are missing")
	}
}

// --- snapshot file ---

// This test checks that a snapshot survives a save and load unchanged.
func TestSnapshot_SaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.dat")
	want := &Snapshot{LastIncludedIndex: 42, LastIncludedTerm: 7, Data: []byte("state machine bytes")}

	if err := SaveSnapshot(path, want); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	got, err := LoadSnapshot(path)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if got.LastIncludedIndex != 42 || got.LastIncludedTerm != 7 || string(got.Data) != "state machine bytes" {
		t.Fatalf("loaded %+v, want index=42 term=7 data=%q", got, want.Data)
	}
}

// This test checks that a node which has never snapshotted loads the empty
// snapshot (0, 0) — the log's original "index 0" sentinel.
func TestSnapshot_MissingFileIsEmptySnapshot(t *testing.T) {
	got, err := LoadSnapshot(filepath.Join(t.TempDir(), "snapshot.dat"))
	if err != nil {
		t.Fatalf("LoadSnapshot on a missing file: %v", err)
	}
	if got.LastIncludedIndex != 0 || got.LastIncludedTerm != 0 || len(got.Data) != 0 {
		t.Fatalf("missing file loaded as %+v, want the empty snapshot", got)
	}
}

// This test checks that a snapshot file whose size disagrees with its own
// header is rejected. It is only ever replaced whole, so there is no such
// thing as a safe partial write — wrong size means corruption.
func TestSnapshot_WrongSizeIsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.dat")
	if err := SaveSnapshot(path, &Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte("0123456789")}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := os.Truncate(path, 25); err != nil { // header + 5 of the 10 data bytes
		t.Fatalf("Truncate: %v", err)
	}

	if _, err := LoadSnapshot(path); err == nil {
		t.Fatalf("LoadSnapshot accepted a snapshot cut short")
	}
}

// --- AppendEntries against a compacted follower ---

// This test checks point 4 of the 8a design: a leader's guess can point
// INSIDE a follower's snapshot once followers compact on their own. The
// follower must accept, treating everything at or below its snapshot as
// matching. Without the trim, the consistency check rejects forever and a
// healthy follower never receives another entry.
func TestAppendEntries_PrevLogIndexInsideFollowerSnapshotIsAccepted(t *testing.T) {
	n := newTestNode(t, 1)
	for i := uint64(1); i <= 7; i++ {
		seedLog(t, n, &raftpb.LogEntry{Term: 1, Index: i, Command: []byte{byte('a' + i - 1)}})
	}
	if err := n.log.Compact(5, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	request := &raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 1, // inside our snapshot
		Entries: []*raftpb.LogEntry{
			{Term: 1, Index: 3, Command: []byte("c")},
			{Term: 1, Index: 4, Command: []byte("d")},
			{Term: 1, Index: 5, Command: []byte("e")},
			{Term: 1, Index: 6, Command: []byte("f")},
			{Term: 1, Index: 7, Command: []byte("g")},
			{Term: 1, Index: 8, Command: []byte("h")},
		},
		LeaderCommit: 8,
	}
	resp := n.HandleAppendEntries(request)

	if !resp.Success {
		t.Fatalf("follower rejected an AppendEntries whose prevLogIndex lies inside its snapshot")
	}
	got, _ := n.log.EntriesFrom(6)
	if !equalStrings(commands(got), []string{"f", "g", "h"}) {
		t.Fatalf("entries after the snapshot = %v, want [f g h]", commands(got))
	}
	if n.CommitIndex() != 8 {
		t.Fatalf("CommitIndex() = %d, want 8", n.CommitIndex())
	}
	if request.PrevLogIndex != 2 || len(request.Entries) != 6 {
		t.Fatalf("the caller's request was modified — trimming must work on a copy")
	}
}

// This test checks the edge where EVERY entry in the request is already in
// the follower's snapshot: success, and nothing changes.
func TestAppendEntries_EntirelyCoveredBySnapshotIsAccepted(t *testing.T) {
	n := newTestNode(t, 1)
	for i := uint64(1); i <= 5; i++ {
		seedLog(t, n, &raftpb.LogEntry{Term: 1, Index: i, Command: []byte("x")})
	}
	if err := n.log.Compact(5, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	resp := n.HandleAppendEntries(&raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*raftpb.LogEntry{{Term: 1, Index: 2}, {Term: 1, Index: 3}},
	})

	if !resp.Success {
		t.Fatalf("rejected a request whose entries are all inside our snapshot")
	}
	if n.log.LastIndex() != 5 {
		t.Fatalf("LastIndex() = %d, want 5 unchanged", n.log.LastIndex())
	}
}
