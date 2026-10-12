package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
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

// sendSnapshot ships our snapshot to a follower that needs entries we have
// compacted away. Called by replicateToPeer without mu held; returns, like
// replicateToPeer, whether the follower acknowledged us as leader of term.
func (n *Node) sendSnapshot(peerID uint64, peer Peer, term uint64, snapshot *Snapshot) (acknowledgedTerm bool) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	resp, err := peer.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          n.id,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Data:              snapshot.Data,
	})
	if err != nil {
		return false // retried on the next tick, same as a failed AppendEntries
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.state.CurrentTerm() {
		n.becomeFollowerLocked(resp.Term)
		return false
	}
	if n.role != Leader || n.state.CurrentTerm() != term {
		return false // a response for a term we no longer lead
	}

	// The follower now holds everything through the snapshot, so normal
	// replication resumes right after it. max() because a reordered,
	// delayed response must never move matchIndex backward.
	if snapshot.LastIncludedIndex > n.matchIndex[peerID] {
		n.matchIndex[peerID] = snapshot.LastIncludedIndex
	}
	n.nextIndex[peerID] = n.matchIndex[peerID] + 1
	n.advanceCommitIndexLocked()
	return resp.Term == term
}

// HandleInstallSnapshot replaces a lagging follower's state with the
// leader's snapshot.
//
// INVARIANT: a failure to store the snapshot is returned as an ERROR, never
// as a normal response. The response carries only a term — the Raft paper
// assumes an install cannot fail, so a reply means success — and the leader
// treats any reply in its own term as "this follower now holds everything
// through the snapshot," setting matchIndex accordingly. Answering normally
// after a failed disk write would credit this follower with entries it does
// not have, and the leader could count it toward a majority for them:
// Milestone 6's "committed on one machine," by a new route. An error takes
// the leader's existing failure path instead — no credit, retry next tick.
func (n *Node) HandleInstallSnapshot(req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Term handling, identical to AppendEntries: reject a stale leader;
	// adopt a higher term; a candidate that hears from this term's leader
	// has lost and steps down.
	if req.Term < n.state.CurrentTerm() {
		return &raftpb.InstallSnapshotResponse{Term: n.state.CurrentTerm()}, nil
	}
	if req.Term > n.state.CurrentTerm() {
		n.becomeFollowerLocked(req.Term)
	} else if n.role == Candidate {
		n.role = Follower
	}
	n.resetElectionTimerLocked() // it came from a legitimate leader

	// INVARIANT: install only a snapshot that goes BEYOND our commitIndex,
	// so commitIndex never moves backward.
	//
	// A snapshot at or below commitIndex is a delayed one: this follower
	// caught up through ordinary replication while the RPC was in flight.
	// Installing it would set commitIndex to the snapshot's index — BELOW
	// what is committed, and possibly below lastApplied (observed with this
	// check removed: commitIndex 2, lastApplied 3). It would also rewrite the
	// log and snapshot files to gain nothing.
	//
	// This check is NOT what keeps delivery ordered when a snapshot lands
	// while the applier is mid-batch. The applier does that by itself: it
	// sends the snapshot only at the start of a pass, and only if
	// lastApplied is still below it, and it is the only sender. A batch in
	// flight finishes first; a snapshot it has already passed is never sent.
	// (Mutation testing confirmed delivery stays strictly forward without
	// this check.)
	if req.LastIncludedIndex <= n.commitIndex {
		// Not a failure: we already hold committed entries through this
		// index, so the leader's "you have it" conclusion is true.
		return &raftpb.InstallSnapshotResponse{Term: n.state.CurrentTerm()}, nil
	}

	snapshot := &Snapshot{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	}
	// Saves first, then compacts: keeps entries after the snapshot if ours
	// match it at its last index, discards everything otherwise.
	if err := n.log.InstallSnapshot(snapshot); err != nil {
		slog.Error("raft: failed to install snapshot from leader",
			"node_id", n.id, "leader_id", req.LeaderId, "index", req.LastIncludedIndex, "error", err)
		return nil, fmt.Errorf("raft: node %d failed to install snapshot at index %d: %w", n.id, req.LastIncludedIndex, err)
	}

	// Everything in the snapshot is committed. The applier sees lastApplied
	// below the new snapshot and delivers it next.
	n.commitIndex = req.LastIncludedIndex
	n.signalApplierLocked()
	return &raftpb.InstallSnapshotResponse{Term: n.state.CurrentTerm()}, nil
}
