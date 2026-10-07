package raft

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
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
// sequence of entries. Index 0 is a sentinel meaning "before the first
// entry" — it is never occupied by a real entry, which is what lets
// prevLogIndex=0 mean "there is nothing before this" without a special case.
//
// Unlike wal.WAL, this log supports truncation: a follower whose log
// conflicts with the leader's must delete the conflicting suffix. That is a
// functional difference, not a stylistic one, and is why this is separate
// code rather than a second wal.WAL instance.
type Log struct {
	// mu protects every field below. Lock ordering across the codebase:
	// Node.mu is always acquired BEFORE this lock, never the reverse —
	// same rule already established for State.mu.
	mu sync.Mutex

	file *os.File

	// entries[i] holds the entry at log index i+1. The offset by one is
	// confined to this file: every exported method takes and returns real
	// 1-based Raft indices, so callers never do the arithmetic themselves.
	entries []*raftpb.LogEntry

	// offsets[i] is the byte offset in file where entries[i] begins. Kept
	// so TruncateFrom can be an O(1) file.Truncate syscall instead of
	// rewriting the whole file.
	offsets []int64

	size int64 // current logical file size in bytes
}

// OpenLog opens (creating if necessary) the log file at path and replays
// its contents into memory.
func OpenLog(path string) (*Log, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("raft: failed to open log %s: %w", path, err)
	}

	l := &Log{file: file}
	if err := l.replayFromDisk(); err != nil {
		file.Close()
		return nil, err
	}
	return l, nil
}

// replayFromDisk reads every complete entry from the start of the file,
// recording byte offsets as it goes. Called once, from OpenLog, before any
// concurrent access is possible — hence no locking.
func (l *Log) replayFromDisk() error {
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("raft: failed to seek log to start for replay: %w", err)
	}

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

		// The Nth entry in the file must carry index N. A gap or a
		// duplicate means real corruption, not an explainable partial
		// write, so it is a hard error rather than a silent stop.
		expectedIndex := uint64(len(l.entries)) + 1
		if index != expectedIndex {
			return fmt.Errorf("raft: log entry at offset %d has index %d, want %d (log is corrupt or non-contiguous)", offset, index, expectedIndex)
		}

		l.entries = append(l.entries, &raftpb.LogEntry{Term: term, Index: index, Command: command})
		l.offsets = append(l.offsets, offset)
		offset += entryHeaderSize + int64(commandLen)
	}

	l.size = offset

	// Physically discard any partial trailing entry. This is where a Raft
	// log differs from the KV WAL: wal.ReadAll can simply STOP at a torn
	// tail and leave those bytes on disk, because nothing is ever written
	// between them and the end of the file. This log gets appended to
	// immediately after recovery, so leftover partial bytes would sit
	// between valid entries and corrupt every subsequent read.
	if err := l.file.Truncate(l.size); err != nil {
		return fmt.Errorf("raft: failed to discard partial trailing log entry: %w", err)
	}

	return nil
}

// LastIndex returns the index of the final entry, or 0 if the log is empty.
func (l *Log) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastIndexLocked()
}

func (l *Log) lastIndexLocked() uint64 {
	return uint64(len(l.entries))
}

// LastTerm returns the term of the final entry, or 0 if the log is empty.
func (l *Log) LastTerm() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastTermLocked()
}

func (l *Log) lastTermLocked() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// TermAt returns the term of the entry at index, and whether that index
// exists. Index 0 is the sentinel "before the first entry" and always
// reports (0, true) — this is what makes prevLogIndex=0 work without a
// special case in the AppendEntries consistency check.
func (l *Log) TermAt(index uint64) (term uint64, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.termAtLocked(index)
}

func (l *Log) termAtLocked(index uint64) (term uint64, ok bool) {
	if index == 0 {
		return 0, true
	}
	if index > uint64(len(l.entries)) {
		return 0, false
	}
	return l.entries[index-1].Term, true
}

// EntriesFrom returns a copy of every entry from index onward, for a leader
// to ship to a lagging follower. Returns an empty slice if index is past
// the end. The slice is freshly allocated so callers can't mutate our
// backing array, though the *LogEntry pointers themselves are shared —
// entries are treated as immutable once appended.
func (l *Log) EntriesFrom(index uint64) []*raftpb.LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index < 1 {
		index = 1
	}
	if index > uint64(len(l.entries)) {
		return nil
	}
	out := make([]*raftpb.LogEntry, len(l.entries)-int(index-1))
	copy(out, l.entries[index-1:])
	return out
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
		header := make([]byte, entryHeaderSize)
		binary.BigEndian.PutUint64(header[0:8], entry.Term)
		binary.BigEndian.PutUint64(header[8:16], entry.Index)
		binary.BigEndian.PutUint32(header[16:20], uint32(len(entry.Command)))

		buf = append(buf, header...)
		buf = append(buf, entry.Command...)

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

// TruncateFrom durably deletes the entry at index and everything after it.
// A no-op if index is past the end of the log.
//
// INVARIANT: disk is truncated and fsynced BEFORE memory is updated, same
// ordering rule as Append and State.Save — memory is only ever a cache of
// what is durably on disk, never ahead of it.
func (l *Log) TruncateFrom(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index < 1 || index > uint64(len(l.entries)) {
		return nil
	}

	position := int(index - 1)
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

// IsUpToDate reports whether a candidate's log is at least as up-to-date as
// ours, per the Raft paper's election restriction: a later last-term always
// wins; on equal terms, the longer log wins.
//
// INVARIANT: this is what prevents electing a leader whose log is missing
// entries a majority has already committed. It was vacuously true through
// Milestone 5 (no log existed to compare); this milestone makes it real.
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
