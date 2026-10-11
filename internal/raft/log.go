package raft

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// entryHeaderSize is the fixed prefix on every on-disk log entry:
// [8-byte term][8-byte index][4-byte commandLen], followed by commandLen
// bytes of command. No outer length frame is needed — the header is
// self-describing, and a short read of either the header or the command is
// exactly as detectable as a short read of a length prefix would be.
//
// The encoding is hand-rolled rather than proto.Marshal(entry), even though
// raftpb.LogEntry is right there: protobuf serialization is not canonical,
// so the same message can legally marshal to different bytes across library
// versions. For a durable log that may later be checksummed or
// byte-compared (Milestone 8), a fixed hand-specified layout is predictable
// in a way protobuf's wire format is not.
const entryHeaderSize = 20

// Log is a Raft node's replicated log: an ordered, durable, 1-indexed
// sequence of entries, of which only a SUFFIX may still be held — everything
// up to snapshotIndex has been compacted into a snapshot (Milestone 8).
//
// The snapshot boundary is a sentinel: TermAt(snapshotIndex) always reports
// snapshotTerm, so prevLogIndex == snapshotIndex needs no special case in the
// AppendEntries consistency check. A log that has never been compacted is
// simply the special case snapshotIndex = 0, snapshotTerm = 0 — which is the
// "index 0 sentinel" this file has had since Milestone 6.
//
// Unlike wal.WAL (Milestone 4, since deleted), this log supports truncation:
// a follower whose log conflicts with the leader's must delete the
// conflicting suffix.
type Log struct {
	// mu protects every field below. Lock ordering across the codebase:
	// Node.mu is always acquired BEFORE this lock, never the reverse —
	// same rule already established for State.mu.
	mu sync.Mutex

	path string
	file *os.File

	// snapshotPath is where this log's snapshot lives, and snapshot is that
	// file's contents, held in memory: the applier delivers it on restart
	// and InstallSnapshot (8c) ships it to lagging followers.
	//
	// The log owns its snapshot so that "snapshot durable BEFORE entries are
	// discarded" is enforced in exactly one place — InstallSnapshot — rather
	// than being a precondition every caller has to remember.
	snapshotPath string
	snapshot     *Snapshot

	// snapshotIndex/snapshotTerm identify the last entry folded into the
	// snapshot. Entries at or below snapshotIndex no longer exist here.
	// Always equal to snapshot.LastIncludedIndex/Term; kept as separate
	// fields because every accessor in this file uses them.
	snapshotIndex uint64
	snapshotTerm  uint64

	// entries[i] holds the entry at log index snapshotIndex+1+i. All the
	// arithmetic for that offset is confined to this file: every exported
	// method takes and returns real Raft indices.
	entries []*raftpb.LogEntry

	// offsets[i] is the byte offset in file where entries[i] begins. Kept
	// so TruncateFrom can be an O(1) file.Truncate syscall instead of
	// rewriting the whole file.
	offsets []int64

	size int64 // current logical file size in bytes
}

// OpenLog loads the snapshot at snapshotPath (a missing file is the empty
// snapshot), then opens (creating if necessary) the log file at logPath and
// replays it. The snapshot file — not the log file — is the authority on
// where the log begins; the log file carries no header of its own, so every
// log written before Milestone 8 is still valid as "a log whose snapshot is
// at 0". Loading both here means the two can never be opened out of step.
//
// Replay doubles as crash recovery for an interrupted compaction: if the
// node crashed after saving a snapshot but before compacting the log, the
// file still holds entries the snapshot covers. They are dropped here, and
// the file is rewritten so file and memory agree again.
func OpenLog(logPath, snapshotPath string) (*Log, error) {
	snapshot, err := LoadSnapshot(snapshotPath)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("raft: failed to open log %s: %w", logPath, err)
	}

	l := &Log{
		path:          logPath,
		file:          file,
		snapshotPath:  snapshotPath,
		snapshot:      snapshot,
		snapshotIndex: snapshot.LastIncludedIndex,
		snapshotTerm:  snapshot.LastIncludedTerm,
	}
	if err := l.replayFromDisk(); err != nil {
		l.file.Close()
		return nil, err
	}
	return l, nil
}

// Snapshot returns the log's current snapshot. Never nil: a log that has
// never been compacted returns the empty snapshot at (0, 0).
func (l *Log) Snapshot() *Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.snapshot
}

// SizeBytes returns the current size of the log file — what the snapshot
// trigger compares against its threshold.
func (l *Log) SizeBytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}

// InstallSnapshot makes snapshot this log's new base: it saves the snapshot
// durably, THEN discards every entry the snapshot covers. It is the only
// way to compact the log.
//
// INVARIANT: the snapshot is fsynced and renamed into place before a single
// entry is discarded. Discarding first and crashing before the snapshot
// lands would lose those entries from this node entirely — they would exist
// in neither file. The other order is harmless: a crash between the two
// steps leaves both files, and OpenLog drops the covered entries on the next
// start.
//
// A no-op if the snapshot is not ahead of the current one: a stale
// snapshot must never move the boundary backwards.
func (l *Log) InstallSnapshot(snapshot *Snapshot) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if snapshot.LastIncludedIndex <= l.snapshotIndex {
		return nil
	}
	if err := SaveSnapshot(l.snapshotPath, snapshot); err != nil {
		return err
	}
	if err := l.compactLocked(snapshot.LastIncludedIndex, snapshot.LastIncludedTerm); err != nil {
		return err
	}
	l.snapshot = snapshot
	return nil
}

// replayFromDisk reads every complete entry from the file, keeps the ones
// after the snapshot, and rewrites the file if anything had to be dropped.
// Called once, from OpenLog, before any concurrent access is possible —
// hence no locking.
func (l *Log) replayFromDisk() error {
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("raft: failed to seek log to start for replay: %w", err)
	}

	var fileEntries []*raftpb.LogEntry
	var fileOffsets []int64
	var offset int64
	header := make([]byte, entryHeaderSize)

	for {
		if _, err := io.ReadFull(l.file, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Clean end of log, or a crash partway through writing a
				// header. Either way everything read so far is valid.
				break
			}
			return fmt.Errorf("raft: failed to read log entry header at offset %d: %w", offset, err)
		}

		term := binary.BigEndian.Uint64(header[0:8])
		index := binary.BigEndian.Uint64(header[8:16])
		commandLen := binary.BigEndian.Uint32(header[16:20])

		command := make([]byte, commandLen)
		if _, err := io.ReadFull(l.file, command); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Crash after the header was written but before the
				// command finished — same safe truncation.
				break
			}
			return fmt.Errorf("raft: failed to read log entry command at offset %d: %w", offset, err)
		}

		// Entries in the file must be contiguous. A gap or a duplicate means
		// real corruption, not an explainable partial write.
		if len(fileEntries) > 0 {
			if previous := fileEntries[len(fileEntries)-1].Index; index != previous+1 {
				return fmt.Errorf("raft: log entry at offset %d has index %d, want %d (log is corrupt or non-contiguous)", offset, index, previous+1)
			}
		}

		fileEntries = append(fileEntries, &raftpb.LogEntry{Term: term, Index: index, Command: command})
		fileOffsets = append(fileOffsets, offset)
		offset += entryHeaderSize + int64(commandLen)
	}

	// The log must pick up exactly where the snapshot leaves off. A file
	// whose first entry starts AFTER snapshotIndex+1 has a hole that
	// nothing can fill.
	if len(fileEntries) > 0 && fileEntries[0].Index > l.snapshotIndex+1 {
		return fmt.Errorf("raft: log starts at index %d but snapshot ends at %d — entries %d..%d are missing",
			fileEntries[0].Index, l.snapshotIndex, l.snapshotIndex+1, fileEntries[0].Index-1)
	}

	survivors, survivorOffsets := l.entriesAfterSnapshot(fileEntries, fileOffsets)
	l.entries, l.offsets, l.size = survivors, survivorOffsets, offset

	if len(survivors) != len(fileEntries) {
		// Something was dropped — finish the interrupted compaction so the
		// file holds exactly what memory holds. Without this, the next
		// append would land after stale entries and break contiguity.
		return l.rewriteLocked(survivors)
	}

	// Physically discard any partial trailing entry. This log is appended
	// to immediately after recovery, so leftover partial bytes would sit
	// between valid entries and corrupt every subsequent read.
	if err := l.file.Truncate(l.size); err != nil {
		return fmt.Errorf("raft: failed to discard partial trailing log entry: %w", err)
	}
	return nil
}

// entriesAfterSnapshot applies Raft's rule for an existing log meeting a
// snapshot: if the log holds the snapshot's last entry with the same term,
// keep everything after it; otherwise the log diverged from the snapshot's
// history and nothing in it can be trusted, so keep nothing. offsets runs
// parallel to entries and is filtered the same way.
func (l *Log) entriesAfterSnapshot(entries []*raftpb.LogEntry, offsets []int64) ([]*raftpb.LogEntry, []int64) {
	var keptEntries []*raftpb.LogEntry
	var keptOffsets []int64
	for i, entry := range entries {
		if entry.Index == l.snapshotIndex && entry.Term != l.snapshotTerm {
			return nil, nil // diverged at the snapshot point
		}
		if entry.Index > l.snapshotIndex {
			keptEntries = append(keptEntries, entry)
			keptOffsets = append(keptOffsets, offsets[i])
		}
	}
	return keptEntries, keptOffsets
}

// SnapshotIndex returns the index of the last entry folded into the snapshot.
func (l *Log) SnapshotIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.snapshotIndex
}

// SnapshotTerm returns the term of the last entry folded into the snapshot.
func (l *Log) SnapshotTerm() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.snapshotTerm
}

// LastIndex returns the index of the final entry, counting compacted ones.
func (l *Log) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastIndexLocked()
}

func (l *Log) lastIndexLocked() uint64 {
	return l.snapshotIndex + uint64(len(l.entries))
}

// LastTerm returns the term of the final entry, counting compacted ones.
func (l *Log) LastTerm() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastTermLocked()
}

// INVARIANT: a log whose entries have ALL been compacted still reports the
// snapshot's term, not 0. IsUpToDate compares last terms; a fully compacted
// node that reported 0 would look like it holds nothing and would vote for
// any candidate — including one missing entries this node knows were
// committed. That is the election restriction silently switched off.
func (l *Log) lastTermLocked() uint64 {
	if len(l.entries) == 0 {
		return l.snapshotTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// TermAt returns the term of the entry at index, and whether it is known.
// snapshotIndex is the always-present sentinel and reports snapshotTerm.
// Indices below it are compacted (ok=false); so are indices past the end.
func (l *Log) TermAt(index uint64) (term uint64, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.termAtLocked(index)
}

func (l *Log) termAtLocked(index uint64) (term uint64, ok bool) {
	switch {
	case index < l.snapshotIndex:
		return 0, false
	case index == l.snapshotIndex:
		return l.snapshotTerm, true
	case index > l.lastIndexLocked():
		return 0, false
	default:
		return l.entries[index-l.snapshotIndex-1].Term, true
	}
}

// EntriesFrom returns a copy of every entry from index onward, for a leader
// to ship to a lagging follower or the applier to deliver.
//
// ok=false means index has been COMPACTED: those entries no longer exist,
// and the caller must fall back to the snapshot. That is deliberately a
// different answer from "nothing after the end" (empty slice, ok=true) —
// conflating them with a nil return would let a caller send nothing and
// believe the follower was caught up.
//
// The slice is freshly allocated, though the *LogEntry pointers are shared —
// entries are treated as immutable once appended.
func (l *Log) EntriesFrom(index uint64) (entries []*raftpb.LogEntry, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index <= l.snapshotIndex {
		return nil, false
	}
	if index > l.lastIndexLocked() {
		return nil, true
	}
	position := index - l.snapshotIndex - 1
	out := make([]*raftpb.LogEntry, uint64(len(l.entries))-position)
	copy(out, l.entries[position:])
	return out, true
}

// Append durably adds entries to the end of the log.
//
// INVARIANT: the write is fsynced BEFORE the in-memory slice is updated. A
// follower that acknowledges an entry it has not durably stored is telling
// the leader a lie — the leader may count that acknowledgement toward a
// majority and commit an entry that vanishes when the follower restarts.
func (l *Log) Append(entries ...*raftpb.LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	newOffsets := make([]int64, 0, len(entries))
	offset := l.size
	buf := make([]byte, 0, len(entries)*(entryHeaderSize+16))

	for _, entry := range entries {
		buf = appendEncoded(buf, entry)
		newOffsets = append(newOffsets, offset)
		offset += entryHeaderSize + int64(len(entry.Command))
	}

	if _, err := l.file.Write(buf); err != nil {
		return fmt.Errorf("raft: failed to write %d log entries: %w", len(entries), err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("raft: failed to fsync %d log entries: %w", len(entries), err)
	}

	l.entries = append(l.entries, entries...)
	l.offsets = append(l.offsets, newOffsets...)
	l.size = offset
	return nil
}

// appendEncoded appends entry's on-disk encoding to buf.
func appendEncoded(buf []byte, entry *raftpb.LogEntry) []byte {
	header := make([]byte, entryHeaderSize)
	binary.BigEndian.PutUint64(header[0:8], entry.Term)
	binary.BigEndian.PutUint64(header[8:16], entry.Index)
	binary.BigEndian.PutUint32(header[16:20], uint32(len(entry.Command)))
	buf = append(buf, header...)
	return append(buf, entry.Command...)
}

// TruncateFrom durably deletes the entry at index and everything after it.
// A no-op if index is past the end of the log.
//
// INVARIANT: never truncates into the snapshot. Everything at or below
// snapshotIndex was applied, so it was committed — and committed entries
// are never removed. A request to do so means a safety property has
// already been violated somewhere, so it is an error, not a no-op.
//
// INVARIANT: disk is truncated and fsynced BEFORE memory is updated, same
// ordering rule as Append and State.Save — memory is only ever a cache of
// what is durably on disk, never ahead of it.
func (l *Log) TruncateFrom(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index <= l.snapshotIndex {
		return fmt.Errorf("raft: refusing to truncate from index %d — at or below snapshot index %d (committed)", index, l.snapshotIndex)
	}
	if index > l.lastIndexLocked() {
		return nil
	}

	position := index - l.snapshotIndex - 1
	newSize := l.offsets[position]

	if err := l.file.Truncate(newSize); err != nil {
		return fmt.Errorf("raft: failed to truncate log from index %d: %w", index, err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("raft: failed to fsync log truncation at index %d: %w", index, err)
	}

	l.entries = l.entries[:position]
	l.offsets = l.offsets[:position]
	l.size = newSize
	return nil
}

// compactLocked discards every entry up to and including index and moves
// the snapshot boundary to (index, term). Callers must hold mu, and must
// already have saved a snapshot through index — which is why the only
// caller is InstallSnapshot.
//
// If this log holds the entry at index with the same term, the entries
// after it are kept. Otherwise — the snapshot is from history this log
// never agreed with, or extends past its end (a leader's snapshot sent to a
// far-behind follower) — every entry is discarded. A no-op if index is not
// beyond the current boundary.
func (l *Log) compactLocked(index, term uint64) error {
	if index <= l.snapshotIndex {
		return nil
	}

	var survivors []*raftpb.LogEntry
	if existingTerm, ok := l.termAtLocked(index); ok && existingTerm == term {
		survivors = append(survivors, l.entries[index-l.snapshotIndex:]...)
	}

	// Disk first, memory second: the boundary moves only once the rewritten
	// file is durably in place. (survivors is a fresh slice, not a window
	// into l.entries, so the compacted entries' memory is actually freed.)
	if err := l.rewriteLocked(survivors); err != nil {
		return fmt.Errorf("raft: failed to compact log through index %d: %w", index, err)
	}
	l.snapshotIndex, l.snapshotTerm = index, term
	return nil
}

// rewriteLocked atomically replaces the log file with exactly survivors and
// points the in-memory state at the new file. Callers must hold mu (or be
// in replay, before any concurrent access).
//
// Same all-or-nothing pattern as State.Save: write a temp file, fsync it,
// rename it over the old one, fsync the directory. Until the rename, the
// old file is intact; after it, the new one is complete. In-memory fields
// change only once the new file is durably in place.
func (l *Log) rewriteLocked(survivors []*raftpb.LogEntry) error {
	dir := filepath.Dir(l.path)
	temp, err := os.CreateTemp(dir, "raft-log-*.tmp")
	if err != nil {
		return fmt.Errorf("raft: failed to create temp log in %s: %w", dir, err)
	}
	tempPath := temp.Name()

	var buf []byte
	offsets := make([]int64, 0, len(survivors))
	var offset int64
	for _, entry := range survivors {
		offsets = append(offsets, offset)
		buf = appendEncoded(buf, entry)
		offset += entryHeaderSize + int64(len(entry.Command))
	}

	if _, err := temp.Write(buf); err != nil {
		temp.Close()
		os.Remove(tempPath)
		return fmt.Errorf("raft: failed to write temp log: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		os.Remove(tempPath)
		return fmt.Errorf("raft: failed to fsync temp log: %w", err)
	}
	if err := temp.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("raft: failed to close temp log: %w", err)
	}
	if err := os.Rename(tempPath, l.path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("raft: failed to rename compacted log into place: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return err
	}

	// The old handle still points at the replaced (now unlinked) file.
	// From here the new file is the durable truth; if it can't be reopened,
	// the log is unusable and the caller must treat that as fatal.
	newFile, err := os.OpenFile(l.path, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("raft: failed to reopen compacted log %s: %w", l.path, err)
	}
	l.file.Close()
	l.file = newFile
	l.entries, l.offsets, l.size = survivors, offsets, offset
	return nil
}

// syncDir fsyncs a directory, making a rename inside it durable. Without
// it, a crash right after a rename can lose the rename even though the
// renamed file's own contents are safely on disk.
func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("raft: failed to open directory %s to fsync: %w", dir, err)
	}
	defer handle.Close()
	if err := handle.Sync(); err != nil {
		return fmt.Errorf("raft: failed to fsync directory %s: %w", dir, err)
	}
	return nil
}

// IsUpToDate reports whether a candidate's log is at least as up-to-date as
// ours, per the Raft paper's election restriction: a later last-term always
// wins; on equal terms, the longer log wins. Compacted entries count: see
// lastTermLocked.
//
// INVARIANT: this is what prevents electing a leader whose log is missing
// entries a majority has already committed.
func (l *Log) IsUpToDate(candidateLastIndex, candidateLastTerm uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	ourLastTerm := l.lastTermLocked()
	if candidateLastTerm != ourLastTerm {
		return candidateLastTerm > ourLastTerm
	}
	return candidateLastIndex >= l.lastIndexLocked()
}

// Close closes the underlying file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("raft: failed to close log: %w", err)
	}
	return nil
}
