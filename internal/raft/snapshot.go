package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// snapshotHeaderSize is [8-byte lastIncludedIndex][8-byte lastIncludedTerm]
// [4-byte dataLen], followed by dataLen bytes of state machine data.
const snapshotHeaderSize = 20

// Snapshot is a state machine's complete state as of LastIncludedIndex,
// plus the term of that entry. Raft treats Data as opaque — only the state
// machine knows how to produce or restore it.
//
// LastIncludedTerm is not bookkeeping: it becomes the log's sentinel term,
// so the AppendEntries consistency check, the election restriction, and the
// KV layer's term check (7b) all keep working at the snapshot boundary.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// LoadSnapshot reads the snapshot at path. A missing file is not an error:
// it is a node that has never snapshotted, which is exactly the empty
// snapshot (0, 0, nil) — the same "index 0 sentinel" the log has always had.
func LoadSnapshot(path string) (*Snapshot, error) {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &Snapshot{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("raft: failed to read snapshot %s: %w", path, err)
	}

	// Like the state file, this file is only ever replaced whole, never
	// appended to — so unlike the log, there is no such thing as a safe
	// partial write. Any size mismatch is corruption.
	if len(raw) < snapshotHeaderSize {
		return nil, fmt.Errorf("raft: snapshot %s is %d bytes, shorter than its %d-byte header (corrupt)", path, len(raw), snapshotHeaderSize)
	}
	dataLen := binary.BigEndian.Uint32(raw[16:20])
	if uint64(len(raw)) != snapshotHeaderSize+uint64(dataLen) {
		return nil, fmt.Errorf("raft: snapshot %s is %d bytes, header says %d (corrupt)", path, len(raw), snapshotHeaderSize+uint64(dataLen))
	}

	return &Snapshot{
		LastIncludedIndex: binary.BigEndian.Uint64(raw[0:8]),
		LastIncludedTerm:  binary.BigEndian.Uint64(raw[8:16]),
		Data:              raw[snapshotHeaderSize:],
	}, nil
}

// SaveSnapshot durably and atomically replaces the snapshot at path.
//
// Same all-or-nothing pattern as State.Save and Log.Compact: temp file,
// fsync, rename, fsync the directory. A crash at any point leaves either
// the complete old snapshot or the complete new one — never a mix that
// would pair new state with an old index or term.
func SaveSnapshot(path string, snapshot *Snapshot) error {
	buf := make([]byte, snapshotHeaderSize, snapshotHeaderSize+len(snapshot.Data))
	binary.BigEndian.PutUint64(buf[0:8], snapshot.LastIncludedIndex)
	binary.BigEndian.PutUint64(buf[8:16], snapshot.LastIncludedTerm)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(snapshot.Data)))
	buf = append(buf, snapshot.Data...)

	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("raft: failed to create temp snapshot in %s: %w", dir, err)
	}
	tempPath := temp.Name()

	if _, err := temp.Write(buf); err != nil {
		temp.Close()
		os.Remove(tempPath)
		return fmt.Errorf("raft: failed to write snapshot at index %d: %w", snapshot.LastIncludedIndex, err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		os.Remove(tempPath)
		return fmt.Errorf("raft: failed to fsync snapshot at index %d: %w", snapshot.LastIncludedIndex, err)
	}
	if err := temp.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("raft: failed to close temp snapshot: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("raft: failed to rename snapshot into place: %w", err)
	}
	return syncDir(dir)
}
