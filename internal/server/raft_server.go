package server

import (
	"context"

	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// RaftServer implements raftpb.RaftServer. Unlike KVServer, it needs no
// translation logic at all — Raft's own concepts (term, candidate, vote)
// are already what raft.Node's handler methods operate on, so this is
// purely a hand-off to the real decision logic in internal/raft.
type RaftServer struct {
	raftpb.UnimplementedRaftServer
	node *raft.Node
}

// NewRaftServer wires a RaftServer to an already-constructed Node.
func NewRaftServer(node *raft.Node) *RaftServer {
	return &RaftServer{node: node}
}

// RequestVote hands off directly to Node.HandleRequestVote. No error path:
// that method can't fail — a disk failure while persisting a vote is
// handled internally by refusing the vote, not by returning an error here.
func (s *RaftServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return s.node.HandleRequestVote(req), nil
}

// AppendEntries hands off directly to Node.HandleAppendEntries.
func (s *RaftServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return s.node.HandleAppendEntries(req), nil
}
