package raft

import (
	"context"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// HandleAppendEntries processes an AppendEntries RPC: the leader's mechanism
// for both replicating log entries and asserting its authority. An
// AppendEntries carrying no entries IS the heartbeat — there is no separate
// heartbeat RPC.
func (n *Node) HandleAppendEntries(req *raftpb.AppendEntriesRequest) *raftpb.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.state.CurrentTerm() {
		// Stale leader, from a term that's already over. Reject.
		return &raftpb.AppendEntriesResponse{Term: n.state.CurrentTerm(), Success: false}
	}

	if req.Term > n.state.CurrentTerm() {
		n.becomeFollowerLocked(req.Term)
	} else if n.role == Candidate {
		// Same term, but someone else already won this election — step
		// down without bumping the term (we're not behind, we just lost).
		n.role = Follower
	}

	// INVARIANT: the election timer resets HERE, before the consistency
	// check below — not after it, and not only on success. Even an
	// AppendEntries we are about to reject came from a legitimate current
	// leader. If we only reset on success, a follower that has fallen
	// behind would keep timing out and campaigning while the leader is
	// actively trying to repair it — fighting the very repair that would
	// fix it, and disrupting a healthy cluster in the process.
	n.resetElectionTimerLocked()

	// The consistency check, which gives Raft its Log Matching Property:
	// refuse unless our log contains an entry at PrevLogIndex whose term is
	// PrevLogTerm. Because that check must have passed for entry
	// PrevLogIndex to have been accepted in the first place, agreement at
	// one position inductively implies agreement at every position before
	// it — which is what lets a leader verify an entire log by comparing a
	// single entry. Index 0 always "exists" with term 0 (Log.TermAt's
	// sentinel), so an empty log needs no special case here.
	prevTerm, exists := n.log.TermAt(req.PrevLogIndex)
	if !exists || prevTerm != req.PrevLogTerm {
		// The leader will decrement its nextIndex for us and retry with an
		// earlier PrevLogIndex, walking backward until it finds where our
		// logs still agree.
		return &raftpb.AppendEntriesResponse{Term: n.state.CurrentTerm(), Success: false}
	}

	if err := n.appendEntriesLocked(req); err != nil {
		// A disk failure means we have NOT durably stored these entries, so
		// we must not claim success — see the fsync note in
		// appendEntriesLocked.
		return &raftpb.AppendEntriesResponse{Term: n.state.CurrentTerm(), Success: false}
	}

	// Advance our commit index toward the leader's, but never past the last
	// entry we actually have — the leader may have committed entries it
	// hasn't shipped us yet, and commitIndex must never point beyond our
	// own log.
	if req.LeaderCommit > n.commitIndex {
		indexOfLastNewEntry := req.PrevLogIndex + uint64(len(req.Entries))
		n.commitIndex = min(req.LeaderCommit, indexOfLastNewEntry)
	}

	return &raftpb.AppendEntriesResponse{Term: n.state.CurrentTerm(), Success: true}
}

// appendEntriesLocked writes req's entries into the log, truncating only
// where there is a genuine conflict. Callers must hold mu.
//
// INVARIANT: an existing entry is deleted ONLY when it has the same index
// but a DIFFERENT term than the incoming one. The naive implementation —
// truncate from PrevLogIndex+1 and append everything — is wrong and
// dangerous: the network can duplicate or reorder AppendEntries, so a
// delayed duplicate carrying entries 5-7, arriving after the leader already
// replicated 5-10, would blindly destroy entries 8, 9 and 10 that the
// leader may already have counted toward a majority and committed.
// Truncation must be driven by an actual term mismatch, never by "I am
// about to write here anyway."
//
// INVARIANT: Log.Append fsyncs before returning, and this function only
// returns nil once it has. A follower that acknowledges an entry it has not
// durably stored is lying to the leader, which may count that
// acknowledgement toward a majority and commit an entry that vanishes when
// the follower restarts.
func (n *Node) appendEntriesLocked(req *raftpb.AppendEntriesRequest) error {
	for i, newEntry := range req.Entries {
		index := req.PrevLogIndex + 1 + uint64(i)

		existingTerm, present := n.log.TermAt(index)
		if present && existingTerm == newEntry.Term {
			// We already hold this exact entry. Leave it — and everything
			// after it — alone.
			continue
		}

		if present {
			// Same index, different term: a genuine conflict. This entry
			// and every entry after it came from a leader whose term has
			// been superseded, so they are not part of the cluster's
			// history and must go.
			if err := n.log.TruncateFrom(index); err != nil {
				return err
			}
		}

		// Everything from i onward is new to us. Append it in one batch —
		// one fsync rather than one per entry.
		return n.log.Append(req.Entries[i:]...)
	}

	return nil
}

// Propose appends a command to the leader's log and returns the index it
// was assigned. It does NOT wait for the entry to be committed — the
// command is merely queued for replication, which happens on subsequent
// ticks. Milestone 7's KV server is the real caller; until then this is
// what makes replication drivable from tests.
//
// Returns isLeader=false without touching the log if this node is not the
// leader: only a leader may create entries, which is what gives the log a
// single authoritative ordering.
func (n *Node) Propose(command []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return 0, 0, false
	}

	term = n.state.CurrentTerm()
	index = n.log.LastIndex() + 1

	if err := n.log.Append(&raftpb.LogEntry{Term: term, Index: index, Command: command}); err != nil {
		return 0, 0, false
	}
	return index, term, true
}

// initializeLeaderStateLocked resets per-peer replication tracking. Called
// exactly once, at the moment this node becomes leader. Callers must hold mu.
func (n *Node) initializeLeaderStateLocked() {
	lastIndex := n.log.LastIndex()
	n.nextIndex = make(map[uint64]uint64, len(n.peers))
	n.matchIndex = make(map[uint64]uint64, len(n.peers))
	for peerID := range n.peers {
		n.nextIndex[peerID] = lastIndex + 1
		n.matchIndex[peerID] = 0
	}
}

// replicateToAll sends an AppendEntries to every peer in parallel. When a
// follower is already caught up the request carries no entries, which is
// exactly the heartbeat — there is one mechanism, not two.
//
// A no-op if this node is no longer leader by the time it runs (e.g. a tick
// fired right as an election was lost).
func (n *Node) replicateToAll() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.state.CurrentTerm()
	peers := make(map[uint64]Peer, len(n.peers))
	for id, p := range n.peers {
		peers[id] = p
	}
	n.mu.Unlock()

	for peerID, peer := range peers {
		peerID, peer := peerID, peer
		// Each goroutine's exit is unconditional: it performs at most one
		// RPC (itself bounded by rpcTimeout) and returns. Nothing blocks on
		// a channel, so none can leak (Rule 5).
		go n.replicateToPeer(peerID, peer, term)
	}
}

// replicateToPeer ships whatever this peer is missing, backing up on
// rejection until it finds where the two logs still agree.
func (n *Node) replicateToPeer(peerID uint64, peer Peer, term uint64) {
	n.mu.Lock()
	if n.role != Leader || n.state.CurrentTerm() != term {
		n.mu.Unlock()
		return
	}
	nextIndex := n.nextIndex[peerID]
	prevLogIndex := nextIndex - 1
	prevLogTerm, _ := n.log.TermAt(prevLogIndex)
	entries := n.log.EntriesFrom(nextIndex)
	commitIndex := n.commitIndex
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	resp, err := peer.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	})
	if err != nil {
		// A dead or slow peer simply isn't updated this round. The next
		// tick retries — Raft's own cadence is the retry mechanism, which
		// is why raftPeerClient deliberately has no retry logic of its own.
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.state.CurrentTerm() {
		n.becomeFollowerLocked(resp.Term)
		return
	}

	// Discard a response that belongs to an election we have since moved
	// past — same stale-result reasoning as startElection's relevance
	// check. Acting on it would update replication state for a term we are
	// no longer leading.
	if n.role != Leader || n.state.CurrentTerm() != term {
		return
	}

	if resp.Success {
		// Compute from what we SENT, not from our current log length: the
		// log may have grown since this request went out, and this peer has
		// only confirmed the entries actually in that request.
		newMatchIndex := prevLogIndex + uint64(len(entries))
		if newMatchIndex > n.matchIndex[peerID] {
			n.matchIndex[peerID] = newMatchIndex
		}
		n.nextIndex[peerID] = n.matchIndex[peerID] + 1
		n.advanceCommitIndexLocked()
		return
	}

	// Rejected: our guess about where this follower's log ends was too
	// optimistic. Back up one and retry on the next tick. (The Raft paper
	// notes an optimization where the follower returns a conflict hint so
	// the leader can skip back faster; not implemented here — see
	// UNDERSTANDING.md's open questions.)
	if n.nextIndex[peerID] > 1 {
		n.nextIndex[peerID]--
	}
}

// advanceCommitIndexLocked raises commitIndex to the highest entry that is
// both replicated on a majority and safe to commit. Callers must hold mu.
//
// INVARIANT: an entry may only be committed by counting replicas if it
// belongs to the CURRENT term. This is the Raft paper's Figure 8 rule, and
// it is not an optimization — omitting it destroys committed data:
//
//	S1 leads term 2, replicates entry 2 to S2 only (not a majority, not
//	committed), then crashes. S5 wins term 3 (S3/S4 vote for it — their logs
//	are empty at that position) and writes a DIFFERENT entry 2. S5 crashes.
//	S1 returns, wins term 4, and finishes replicating its old term-2 entry
//	to S3. Entry 2 now sits on S1, S2, S3 — a majority. If S1 commits it on
//	that basis and crashes, S5 can still win term 5 (its log is more
//	up-to-date by term) and overwrite entry 2 everywhere. An entry that was
//	reported committed is gone.
//
// Requiring a current-term entry first closes the hole: once an entry from
// the leader's own term is on a majority, no node lacking it can win an
// election again, and every earlier entry becomes safe transitively.
func (n *Node) advanceCommitIndexLocked() {
	currentTerm := n.state.CurrentTerm()
	majority := (len(n.peers)+1)/2 + 1

	for index := n.log.LastIndex(); index > n.commitIndex; index-- {
		termAtIndex, ok := n.log.TermAt(index)
		if !ok || termAtIndex != currentTerm {
			// Entries from earlier terms are skipped here, never committed
			// directly — they get committed transitively once a
			// current-term entry above them is.
			continue
		}

		replicas := 1 // ourselves
		for peerID := range n.peers {
			if n.matchIndex[peerID] >= index {
				replicas++
			}
		}

		if replicas >= majority {
			n.commitIndex = index
			return
		}
	}
}
