package raft

import (
	"context"
	"log/slog"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// HandleRequestVote decides whether to grant a vote. This is the actual
// decision logic behind the RequestVote RPC — Milestone 5c's gRPC handler
// will just call straight through to this.
func (n *Node) HandleRequestVote(req *raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Term rule: if we see a higher term, adopt it before evaluating
	// anything else — this is the mechanism that lets a stale leader or
	// candidate discover it's been superseded purely from an incoming
	// message, with no separate "you're fired" signal needed.
	if req.Term > n.state.CurrentTerm() {
		n.becomeFollowerLocked(req.Term)
	}

	if req.Term < n.state.CurrentTerm() {
		// Stale candidate, still campaigning on an old term. Reject, and
		// tell it our term so it can catch up.
		return &raftpb.RequestVoteResponse{Term: n.state.CurrentTerm(), VoteGranted: false}
	}

	votedFor := n.state.VotedFor()
	// INVARIANT: re-granting to the SAME candidate we already voted for
	// this term is fine — an idempotent retry of an RPC we already
	// answered. Only a DIFFERENT candidate is refused.
	alreadyVotedElsewhere := votedFor != NoVote && votedFor != req.CandidateId

	// Choice 3 from the concept brief: the log up-to-date check is
	// vacuously true until Milestone 6 gives nodes an actual log to
	// compare (last_log_index/last_log_term are always 0 on both sides
	// right now). Carried as a named variable rather than inlined so the
	// gap is visible, not silently absent.
	candidateLogIsUpToDate := true

	if alreadyVotedElsewhere || !candidateLogIsUpToDate {
		return &raftpb.RequestVoteResponse{Term: n.state.CurrentTerm(), VoteGranted: false}
	}

	// Persist the vote BEFORE responding — see state.go's Save invariant.
	// If we can't durably record that we voted, we must not claim to have
	// voted, so a disk failure here means refusing rather than granting.
	if err := n.state.Save(req.Term, req.CandidateId); err != nil {
		return &raftpb.RequestVoteResponse{Term: n.state.CurrentTerm(), VoteGranted: false}
	}

	// Granting a vote counts as "heard from a legitimate participant,"
	// same as a heartbeat would — resets how long until we'd start our own
	// election.
	n.resetElectionTimerLocked()
	return &raftpb.RequestVoteResponse{Term: req.Term, VoteGranted: true}
}

// startElection campaigns for votes in a new term. Called by run() when
// this node hasn't heard from a leader within its election timeout.
func (n *Node) startElection() {
	n.mu.Lock()
	n.role = Candidate
	electionTerm := n.state.CurrentTerm() + 1
	if err := n.state.Save(electionTerm, n.id); err != nil { // vote for self
		n.mu.Unlock()
		return
	}
	n.resetElectionTimerLocked()
	peers := make(map[uint64]Peer, len(n.peers))
	for id, p := range n.peers {
		peers[id] = p
	}
	n.mu.Unlock()

	slog.Info("raft: starting election", "node_id", n.id, "term", electionTerm)

	votes := 1 // ourselves
	voteCh := make(chan bool, len(peers))

	for _, peer := range peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			defer cancel()

			resp, err := peer.RequestVote(ctx, &raftpb.RequestVoteRequest{
				Term:         electionTerm,
				CandidateId:  n.id,
				LastLogIndex: 0, // vacuous until Milestone 6
				LastLogTerm:  0,
			})
			if err != nil {
				voteCh <- false
				return
			}

			n.mu.Lock()
			if resp.Term > n.state.CurrentTerm() {
				n.becomeFollowerLocked(resp.Term)
			}
			n.mu.Unlock()

			voteCh <- resp.VoteGranted
		}()
	}

	for i := 0; i < len(peers); i++ {
		if <-voteCh {
			votes++
		}

		n.mu.Lock()
		// A majority counted against a STALE election must be discarded:
		// by the time these responses arrive, this node may have already
		// stepped down (saw a higher term, or received a legitimate
		// heartbeat for this same term from whoever actually won). Both
		// conditions below must still hold for this result to be valid.
		stillRelevant := n.role == Candidate && n.state.CurrentTerm() == electionTerm
		haveMajority := votes > (len(n.peers)+1)/2
		if stillRelevant && haveMajority {
			n.role = Leader
			n.mu.Unlock()
			slog.Info("raft: elected leader", "node_id", n.id, "term", electionTerm, "votes", votes)
			n.sendHeartbeats() // immediately — don't wait for the next tick
			return
		}
		n.mu.Unlock()
	}
}

// HandleAppendEntries processes a heartbeat (or, from Milestone 6, real
// log entries). At this milestone entries is always empty — receiving the
// RPC at all IS the heartbeat.
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

	n.resetElectionTimerLocked()
	return &raftpb.AppendEntriesResponse{Term: n.state.CurrentTerm(), Success: true}
}

// sendHeartbeats fans AppendEntries out to every peer in parallel. A no-op
// if this node is no longer leader by the time it actually runs (e.g. a
// tick fired right as an election was lost).
func (n *Node) sendHeartbeats() {
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

	for _, peer := range peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			defer cancel()

			resp, err := peer.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
				Term:     term,
				LeaderId: n.id,
				Entries:  nil,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			if resp.Term > n.state.CurrentTerm() {
				n.becomeFollowerLocked(resp.Term)
			}
			n.mu.Unlock()
		}()
	}
}
