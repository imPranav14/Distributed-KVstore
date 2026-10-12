package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// raftPeerClient is the real, gRPC-backed implementation of raft.Peer.
// internal/raft never imports gRPC directly — this is where the Peer
// interface actually meets the network, kept in internal/server the same
// way internal/client holds the KV-facing gRPC client without
// internal/store/internal/dedup knowing gRPC exists.
//
// Deliberately has NO retry logic, unlike internal/client.Client. Raft
// already has its own timeout-and-retry semantics built into the protocol:
// a dead peer simply isn't counted this round, and the next election
// timeout or heartbeat tick tries again naturally. Retrying here too would
// be redundant with, and could interact badly with, raft.Node's own
// rpcTimeout — which is deliberately shorter than the election timeout
// range specifically so one hung call can't block a whole round.
type raftPeerClient struct {
	stub raftpb.RaftClient
}

// NewRaftPeerClient dials addr (e.g. "node2:50051") and returns a
// ready-to-use raft.Peer. Dialing is lazy, same as internal/client.NewClient
// — grpc.NewClient doesn't block waiting for the peer to be reachable.
func NewRaftPeerClient(addr string) (raft.Peer, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("server: failed to dial raft peer %s: %w", addr, err)
	}
	return &raftPeerClient{stub: raftpb.NewRaftClient(conn)}, nil
}

func (c *raftPeerClient) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return c.stub.RequestVote(ctx, req)
}

func (c *raftPeerClient) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return c.stub.AppendEntries(ctx, req)
}

func (c *raftPeerClient) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return c.stub.InstallSnapshot(ctx, req)
}
