package raft

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrNotLeader means this node is not the leader; the caller should try
	// another node.
	ErrNotLeader = errors.New("raft: not leader")

	// ErrNotReadyForReads means this node is leader but has not yet
	// committed an entry in its own term — normally its no-op is still in
	// flight. Brief and expected right after every election; retry shortly.
	ErrNotReadyForReads = errors.New("raft: leader has not committed an entry in its current term yet")

	// ErrLeadershipNotConfirmed means a majority did not acknowledge this
	// node as leader — it may have been cut off and replaced.
	ErrLeadershipNotConfirmed = errors.New("raft: could not confirm leadership with a majority")
)

// ReadIndex returns an index that a linearizable read must wait for: once
// the state machine has applied through it, reading local state reflects
// every write that completed before this call began.
//
// This is the ReadIndex algorithm from the Raft thesis (section 6.4). It
// costs one round trip to a majority and no disk write — the alternative of
// putting every read through the log would add a majority fsync per GET.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}

	// INVARIANT: only read once this leader has committed an entry from its
	// OWN term. Right after an election, commitIndex can still sit below
	// entries the previous leader committed — this leader holds them (the
	// election restriction guarantees it) but cannot mark them committed
	// until something from its own term commits (Figure 8). Reading at that
	// stale commitIndex could miss a write that already returned OK. The
	// no-op every leader appends on election is what makes this condition
	// true quickly instead of waiting for the next client write.
	currentTerm := n.state.CurrentTerm()
	commitTerm, _ := n.log.TermAt(n.commitIndex)
	if commitTerm != currentTerm {
		n.mu.Unlock()
		return 0, ErrNotReadyForReads
	}

	readIndex := n.commitIndex
	peers := make(map[uint64]Peer, len(n.peers))
	for id, p := range n.peers {
		peers[id] = p
	}
	n.mu.Unlock()

	if err := n.confirmLeadership(ctx, currentTerm, peers); err != nil {
		return 0, err
	}
	return readIndex, nil
}

// confirmLeadership runs one round of AppendEntries and succeeds once a
// majority (ourselves included) has acknowledged us as leader of term.
//
// INVARIANT: this round must happen AFTER readIndex is recorded. Its job is
// to prove that no newer leader had been elected by the time the read
// started. A newer leader needs a majority; any majority overlaps ours; so
// if a newer leader existed, at least one of our acknowledgers would have
// answered with the higher term instead. A leader cut off by a partition
// cannot collect a majority at all, so it fails here rather than serving
// data a newer leader has already changed.
func (n *Node) confirmLeadership(ctx context.Context, term uint64, peers map[uint64]Peer) error {
	clusterSize := len(peers) + 1
	majority := clusterSize/2 + 1
	acknowledgements := 1 // ourselves
	if acknowledgements >= majority {
		return nil // single-node cluster: we are the majority
	}

	// Buffered to len(peers): every goroutine can always send and exit, even
	// after this function has returned early — none can leak (Rule 5).
	results := make(chan bool, len(peers))
	for peerID, peer := range peers {
		peerID, peer := peerID, peer
		go func() { results <- n.replicateToPeer(peerID, peer, term) }()
	}

	for answered := 0; answered < len(peers); answered++ {
		select {
		case acknowledged := <-results:
			if acknowledged {
				acknowledgements++
			}
			if acknowledgements >= majority {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("%w: %d of %d nodes acknowledged term %d, need %d",
		ErrLeadershipNotConfirmed, acknowledgements, clusterSize, term, majority)
}
