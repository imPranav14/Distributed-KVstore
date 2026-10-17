package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/ring"
	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// RaftPeerConn is this process's connection to one other node. Since
// Milestone 9 that node hosts several Raft groups, and all of them share
// this ONE connection — gRPC multiplexes concurrent calls over it — so the
// number of connections grows with the number of nodes, not nodes × groups.
// ForGroup gives each group its own raft.Peer on top of it.
//
// internal/raft never imports gRPC directly; this file is where the Peer
// interface meets the network, kept in internal/server the same way
// internal/client holds the KV-facing gRPC client.
type RaftPeerConn struct {
	conn *grpc.ClientConn
	stub raftpb.RaftClient
}

// DialRaftPeer connects to addr (e.g. "node2:50051"). Dialing is lazy, same
// as internal/client.NewClient — grpc.NewClient doesn't block waiting for
// the peer to be reachable, which matters because docker-compose starts all
// nodes at roughly the same time.
func DialRaftPeer(addr string) (*RaftPeerConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("server: failed to dial raft peer %s: %w", addr, err)
	}
	return &RaftPeerConn{conn: conn, stub: raftpb.NewRaftClient(conn)}, nil
}

// ForGroup returns group's view of this peer: a raft.Peer whose every
// request names group.
func (c *RaftPeerConn) ForGroup(group ring.GroupID) raft.Peer {
	return &groupPeer{stub: c.stub, group: group}
}

// Close closes the connection, for every group using it.
func (c *RaftPeerConn) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("server: failed to close raft peer connection: %w", err)
	}
	return nil
}

// groupPeer is the real, gRPC-backed implementation of raft.Peer for one
// group.
//
// Deliberately has NO retry logic, unlike internal/client.Client. Raft
// already has its own timeout-and-retry semantics built into the protocol:
// a dead peer simply isn't counted this round, and the next election
// timeout or heartbeat tick tries again naturally. Retrying here too would
// be redundant with, and could interact badly with, raft.Node's own
// rpcTimeout — which is deliberately shorter than the election timeout
// range specifically so one hung call can't block a whole round.
type groupPeer struct {
	stub  raftpb.RaftClient
	group ring.GroupID
}

// INVARIANT (all three methods): every outgoing Raft request names this
// peer's group. raft.Node never sets group_id — it does not know which
// group it is — so this stamp is the only thing that lets the receiving
// process route the message. Without it every message would arrive as
// group 0 and be refused.
//
// Writing into req is safe because of the raft.Peer contract: each request
// is built for this single call and never reused or shared.

func (p *groupPeer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	req.GroupId = uint64(p.group)
	return p.stub.RequestVote(ctx, req)
}

func (p *groupPeer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	req.GroupId = uint64(p.group)
	return p.stub.AppendEntries(ctx, req)
}

func (p *groupPeer) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	req.GroupId = uint64(p.group)
	return p.stub.InstallSnapshot(ctx, req)
}
