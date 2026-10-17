package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/ring"
	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// RaftServer implements raftpb.RaftServer for every Raft group this process
// hosts. Since Milestone 9 one process runs one Raft group per shard, all on
// the same port, so each incoming message is dispatched by its group_id to
// that group's Node. Beyond the dispatch there is no translation at all —
// Raft's own concepts (term, candidate, vote) are what raft.Node's handlers
// operate on.
type RaftServer struct {
	raftpb.UnimplementedRaftServer

	// nodes maps each hosted group to its Raft node. Copied in NewRaftServer
	// and never modified afterwards, so the handlers — each running on its
	// own gRPC goroutine — read it with no mutex (Rule 7: read-only after
	// construction, and the caller's map cannot change it later).
	nodes map[ring.GroupID]*raft.Node
}

// NewRaftServer builds the dispatcher for the given groups.
func NewRaftServer(nodes map[ring.GroupID]*raft.Node) (*RaftServer, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("server: NewRaftServer: no Raft groups to serve")
	}
	if _, hasZero := nodes[0]; hasZero {
		return nil, fmt.Errorf("server: NewRaftServer: group 0 is reserved — it is what an unset group_id reads as")
	}

	copied := make(map[ring.GroupID]*raft.Node, len(nodes))
	for group, node := range nodes {
		copied[group] = node
	}
	return &RaftServer{nodes: copied}, nil
}

// nodeFor returns the Node of the group a request names.
//
// INVARIANT: a Raft message is handled only by the group it names. A message
// for group 2 handled by group 1 would put another log's entries into group
// 1's log — and no consistency check would catch it, because terms and
// indexes from two independent logs can line up by coincidence. A group this
// process does not host — including group 0, which is what an unset group_id
// reads as — is refused rather than handed to any group.
func (s *RaftServer) nodeFor(groupID uint64) (*raft.Node, error) {
	node, hosted := s.nodes[ring.GroupID(groupID)]
	if !hosted {
		return nil, status.Errorf(codes.NotFound,
			"server: RaftServer: this process hosts no Raft group %d (0 means group_id was never set)", groupID)
	}
	return node, nil
}

// RequestVote hands off to the named group's HandleRequestVote. Apart from
// an unknown group there is no error path: a disk failure while persisting a
// vote is handled inside the node by refusing the vote.
func (s *RaftServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	node, err := s.nodeFor(req.GroupId)
	if err != nil {
		return nil, err
	}
	return node.HandleRequestVote(req), nil
}

// AppendEntries hands off to the named group's HandleAppendEntries.
func (s *RaftServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	node, err := s.nodeFor(req.GroupId)
	if err != nil {
		return nil, err
	}
	return node.HandleAppendEntries(req), nil
}

// InstallSnapshot hands off to the named group's HandleInstallSnapshot.
// Unlike the other two, the handler itself CAN fail: a follower that could
// not store the snapshot must say so as an error, so the leader does not
// credit it with entries it does not hold (see the invariant on
// HandleInstallSnapshot).
func (s *RaftServer) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	node, err := s.nodeFor(req.GroupId)
	if err != nil {
		return nil, err
	}
	return node.HandleInstallSnapshot(req)
}
