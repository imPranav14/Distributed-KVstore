// Package server implements the gRPC-facing services: KVStore (for
// clients) and Raft (for other nodes).
//
// As of Milestone 7b, every write goes through Raft: the handler proposes
// a command, waits until the replicated state machine has applied it, and
// only then replies. There is no local-only write path and no separate WAL
// anymore — the Raft log IS the write-ahead log.
//
// As of Milestone 9c, a process can host several Raft groups, one per
// shard. Every request is served by the group that owns its key.
package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/ring"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
)

// applyWaitTimeout caps how long a write handler waits for its entry to be
// applied. Clients normally set a shorter deadline of their own (the
// client's perAttemptTimeout is 2s); this is the backstop for callers that
// set none, so a partitioned leader can't hold a handler forever.
const applyWaitTimeout = 5 * time.Second

// GroupReplica is this process's replica of one Raft group: the node, and
// the state machine consuming its apply channel.
type GroupReplica struct {
	Raft         *raft.Node
	StateMachine *StateMachine
}

// KVServer implements kvpb.KVStoreServer on top of one or more Raft groups.
type KVServer struct {
	// UnimplementedKVStoreServer must be embedded (by value, per its own
	// doc comment) for forward compatibility: if a future version of
	// kv.proto adds a new RPC method, this embedded type supplies a default
	// (returns Unimplemented) so this file doesn't fail to compile the
	// moment the .proto grows.
	kvpb.UnimplementedKVStoreServer

	// keyRing and replicas are built in NewKVServer and never modified, so
	// every handler goroutine reads them with no mutex (Rule 7: read-only
	// after construction).
	keyRing  *ring.Ring
	replicas map[ring.GroupID]GroupReplica
}

// NewKVServer wires a KVServer to this process's group replicas. Building
// the replicas is main.go's job (Rule 2), not this package's.
//
// The ring is built from exactly the groups hosted here, so within one
// process the ring can never name a group that has no local replica.
func NewKVServer(replicas map[ring.GroupID]GroupReplica) (*KVServer, error) {
	if len(replicas) == 0 {
		return nil, fmt.Errorf("server: NewKVServer: no group replicas to serve")
	}
	if _, hasZero := replicas[0]; hasZero {
		return nil, fmt.Errorf("server: NewKVServer: group 0 is reserved — it is what an unset group_id reads as")
	}

	groups := make([]ring.GroupID, 0, len(replicas))
	copied := make(map[ring.GroupID]GroupReplica, len(replicas))
	for group, replica := range replicas {
		groups = append(groups, group)
		copied[group] = replica
	}
	keyRing, err := ring.New(groups)
	if err != nil {
		return nil, fmt.Errorf("server: NewKVServer: %w", err)
	}
	return &KVServer{keyRing: keyRing, replicas: copied}, nil
}

// replicaFor returns the replica of the group that owns key, provided the
// request named that same group.
//
// INVARIANT: an operation runs only in the group that owns its key, and the
// check happens BEFORE anything is proposed. A write proposed in the wrong
// group would commit durably where no correct read will ever look, and a
// read served by the wrong group returns a silent "not found". The check is
// never made at apply time: applying must be deterministic on every
// replica, and a check that depended on each replica's local config could
// let replicas with different configs diverge.
//
// The request's group is the one the CLIENT computed from its own ring.
// Requiring it to match this node's ring turns any ring disagreement into
// an error rather than a write in a group the other side would not look in.
//
// INVARIANT: every handler calls this FIRST — ownership is checked before
// leadership (before ReadIndex or Propose). The client relies on the order:
// a NotLeader reply then proves this node's ring AGREES with the client's,
// so the client keeps the node eligible instead of treating it as a routing
// refusal. Checking leadership first would let a node whose ring disagrees
// answer NotLeader, and the client would keep retrying a node that can
// never serve it.
func (s *KVServer) replicaFor(requestedGroup uint64, key string) (GroupReplica, error) {
	owner := s.keyRing.Owner(key)
	if ring.GroupID(requestedGroup) != owner {
		return GroupReplica{}, status.Errorf(codes.FailedPrecondition,
			"wrong group: key %q belongs to group %d on this node, the request named group %d (0 means not set)",
			key, owner, requestedGroup)
	}
	return s.replicas[owner], nil
}

// Get performs a linearizable read: the value returned reflects every write
// that completed before Get was called, on any node.
//
// Neither "read on any node" nor "read on whoever believes it's leader" is
// enough. A follower can be behind. A leader cut off by a partition still
// believes it leads while a newer leader accepts writes it never sees.
// ReadIndex closes both: it confirms with a majority that this node is
// still leader, and returns the commit index to wait for. Cost: one round
// trip to a majority, no disk write.
func (s *KVServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	replica, err := s.replicaFor(req.GroupId, req.Key)
	if err != nil {
		return nil, err
	}

	waitCtx, cancel := context.WithTimeout(ctx, applyWaitTimeout)
	defer cancel()

	readIndex, err := replica.Raft.ReadIndex(waitCtx)
	if err != nil {
		return nil, readError(err)
	}

	// The commit index is known; this node's state machine may not have
	// applied that far yet. Reading before it does could miss a write that
	// is committed but not yet applied locally.
	if err := replica.StateMachine.WaitForApplied(waitCtx, readIndex); err != nil {
		if errors.Is(err, errStateMachineStopped) {
			return nil, status.Error(codes.Unavailable, "node is shutting down")
		}
		return nil, status.FromContextError(err).Err()
	}

	value, found := replica.StateMachine.Get(req.Key)
	return &kvpb.GetResponse{Value: value, Found: found}, nil
}

// readError maps a ReadIndex failure to a gRPC status. Every failure that
// another node (or a moment's wait) might fix is Unavailable, which the
// client treats as "try the next node."
func readError(err error) error {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return status.Error(codes.Unavailable, "not leader")
	case errors.Is(err, raft.ErrNotReadyForReads):
		return status.Error(codes.Unavailable, "leader not ready for reads yet")
	case errors.Is(err, raft.ErrLeadershipNotConfirmed):
		return status.Errorf(codes.Unavailable, "leadership not confirmed: %v", err)
	default:
		return status.FromContextError(err).Err()
	}
}

// Put replicates a write through Raft and replies once it is applied.
func (s *KVServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	replica, err := s.replicaFor(req.GroupId, req.Key)
	if err != nil {
		return nil, err
	}
	cmd := &kvpb.Command{
		Op:        kvpb.Command_OP_PUT,
		Key:       req.Key,
		Value:     req.Value,
		ClientId:  req.ClientId,
		RequestId: req.RequestId,
	}
	if err := s.replicate(ctx, replica, cmd); err != nil {
		return nil, err
	}
	return &kvpb.PutResponse{}, nil
}

// Delete replicates a delete through Raft and replies once it is applied.
func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	replica, err := s.replicaFor(req.GroupId, req.Key)
	if err != nil {
		return nil, err
	}
	cmd := &kvpb.Command{
		Op:        kvpb.Command_OP_DELETE,
		Key:       req.Key,
		ClientId:  req.ClientId,
		RequestId: req.RequestId,
	}
	if err := s.replicate(ctx, replica, cmd); err != nil {
		return nil, err
	}
	return &kvpb.DeleteResponse{}, nil
}

// replicate proposes cmd in replica's group, waits until that group's state
// machine has applied the entry at the proposed index, and confirms that
// entry is ours.
//
// INVARIANT: an error from this function means the outcome is UNKNOWN, not
// that the write failed. A timeout can fire one instant before the entry
// commits; a lost-leadership error is only certain about THIS proposal,
// not about an earlier attempt of the same request. That is exactly why the
// client always retries with the SAME request ID: if any earlier attempt
// did apply, the replicated dedup table in StateMachine skips the retry on
// every replica.
func (s *KVServer) replicate(ctx context.Context, replica GroupReplica, cmd *kvpb.Command) error {
	data, err := encodeCommand(cmd)
	if err != nil {
		return status.Errorf(codes.Internal, "encode failed: %v", err)
	}

	index, term, isLeader := replica.Raft.Propose(data)
	if !isLeader {
		// Also returned if the leader's own log append failed. Either way
		// this node did not accept the write; the client should try
		// another node.
		return status.Error(codes.Unavailable, "not leader")
	}

	waitCtx, cancel := context.WithTimeout(ctx, applyWaitTimeout)
	defer cancel()

	if err := replica.StateMachine.WaitForApplied(waitCtx, index); err != nil {
		if errors.Is(err, errStateMachineStopped) {
			return status.Error(codes.Unavailable, "node is shutting down")
		}
		// The deadline or cancellation came from ctx: report it as such,
		// so the client sees DeadlineExceeded/Canceled rather than a
		// generic failure.
		return status.FromContextError(err).Err()
	}

	// INVARIANT: the entry applied at index is ours if and only if it has
	// the term we proposed it in. Within one term there is exactly one
	// leader, and it assigns each index exactly once — so by the Log
	// Matching Property, (index, term) identifies one entry. A different
	// term means this node lost leadership before our entry committed, and
	// a later leader's entry took the slot. Returning OK here would tell
	// the client its write succeeded when it was overwritten.
	appliedTerm, ok := replica.Raft.TermAt(index)
	if !ok || appliedTerm != term {
		return status.Error(codes.Unavailable, "leadership lost before the write committed")
	}
	return nil
}
