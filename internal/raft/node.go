package raft

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// Role is which of the three states a node is currently in.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// Peer is the ability to call RequestVote/AppendEntries on another node.
// Deliberately mirrors the two gRPC methods exactly rather than some more
// abstracted domain type — unlike kv_server.go's translation from wire
// types into store/dedup calls, Raft's own concepts (term, candidate,
// vote) ARE the domain model here, so there's nothing simpler to translate
// into. This interface is what lets every test in this package run
// entirely in-process, with fake Peers calling directly into other Node
// instances' handler methods — Milestone 5c's real gRPC client is just a
// thin implementation of this same interface.
type Peer interface {
	RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
}

const (
	tickInterval       = 10 * time.Millisecond
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	// rpcTimeout bounds a single RequestVote/AppendEntries call. Shorter
	// than the election timeout range so a candidate never blocks forever
	// waiting on a genuinely dead peer — in a 3-node cluster, this is what
	// lets the remaining majority still elect a leader when one node is
	// truly down, not just slow.
	rpcTimeout = 100 * time.Millisecond
)

// Node runs Raft leader election for one cluster member.
type Node struct {
	// mu protects every field below. Every RPC handler, the background
	// timer loop, and every peer-response goroutine spawned by
	// startElection/replicateToAll reads or writes through this same lock —
	// there is exactly one lock for all of a node's mutable state, same
	// simplicity-first reasoning as store.Store's single RWMutex.
	mu sync.Mutex

	id    uint64
	peers map[uint64]Peer
	state *State // Milestone 5a — persistent term/votedFor
	log   *Log   // Milestone 6a — the replicated log

	role            Role
	lastHeartbeat   time.Time     // reset on: valid AppendEntries, granting a vote, starting our own election
	electionTimeout time.Duration // randomized, re-rolled on every reset — see resetElectionTimerLocked

	// commitIndex is the highest log index known to be committed
	// (replicated on a majority, and therefore guaranteed to survive any
	// future leader election).
	//
	// Deliberately NOT persisted, per the Raft paper: it is volatile state.
	// A restarted node starts at 0 and safely re-learns its commit index
	// from the next leader's AppendEntries. Persisting it would buy nothing
	// — the entries themselves are already durable, and an over-eager
	// commitIndex restored from disk would be strictly more dangerous than
	// an under-eager one rebuilt from the leader.
	commitIndex uint64

	// nextIndex and matchIndex are leader-only state, reinitialized every
	// time this node wins an election and meaningless otherwise.
	//
	// The asymmetry in how they start is deliberate and load-bearing:
	// nextIndex is OPTIMISTIC (assume the follower matches us exactly, and
	// back off on rejection), while matchIndex is PESSIMISTIC (assume we
	// know nothing until a follower actually confirms). Optimism on
	// nextIndex costs at most a few wasted round trips; optimism on
	// matchIndex would let a brand-new leader count entries as replicated
	// that no follower has, and commit them.
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	stopCh chan struct{} // closed by Stop(); read by run()'s select loop
}

// NewNode constructs a Node in the Follower role, matching Raft's own
// rule that every server begins as a follower on startup. It does not
// start the background timer loop — call Start() separately, so tests can
// exercise handler methods directly without a timer race in the
// background.
func NewNode(id uint64, state *State, log *Log, peers map[uint64]Peer) *Node {
	n := &Node{
		id:     id,
		peers:  peers,
		state:  state,
		log:    log,
		role:   Follower,
		stopCh: make(chan struct{}),
	}
	n.resetElectionTimerLocked()
	return n
}

// Start launches the background timer loop. Its documented exit condition
// (Rule 5) is stopCh being closed by Stop().
func (n *Node) Start() {
	go n.run()
}

// Stop signals the background loop to exit. NOT idempotent — calling it
// twice panics (closing an already-closed channel), same as the standard
// library's own close(). A real shutdown sequence only ever calls this
// once; callers that might otherwise stop the same Node from two places
// must guard against that themselves.
func (n *Node) Stop() {
	close(n.stopCh)
}

// Role returns the node's current role. Exported for tests and for
// main.go/logging to observe state without reaching into internals.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// CommitIndex returns the highest log index known to be committed.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func (n *Node) run() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			role := n.role
			timedOut := time.Since(n.lastHeartbeat) > n.electionTimeout
			n.mu.Unlock()

			switch {
			case role == Leader:
				n.replicateToAll()
			case timedOut:
				n.startElection()
			}
		}
	}
}

// becomeFollowerLocked transitions to Follower and adopts term as the new
// current term. Callers must hold mu.
//
// INVARIANT: entering a new term always clears any previous vote — a vote
// cast in term 4 must never be mistaken for a vote in term 5. This is why
// Save is called with NoVote here rather than leaving votedFor untouched.
func (n *Node) becomeFollowerLocked(term uint64) {
	slog.Info("raft: becoming follower", "node_id", n.id, "term", term)
	n.role = Follower
	n.state.Save(term, NoVote)
	n.resetElectionTimerLocked()
}

// resetElectionTimerLocked re-rolls a fresh random timeout AND records
// "now" as the last time we heard from a legitimate participant. Callers
// must hold mu.
//
// INVARIANT: the timeout is randomized on every reset, not chosen once at
// startup — CLAUDE.md's Milestone 5 break-it protocol targets exactly this
// (fixed, identical timeouts cause every node to time out in lockstep and
// split every election forever).
func (n *Node) resetElectionTimerLocked() {
	n.lastHeartbeat = time.Now()
	span := int64(electionTimeoutMax - electionTimeoutMin)
	n.electionTimeout = electionTimeoutMin + time.Duration(rand.Int64N(span))
}
