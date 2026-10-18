package server

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/grpc"

	"github.com/imPranav14/Distributed-KVstore/internal/config"
	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/ring"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// NodeRuntime is one running node: a replica of every shard group, plus the
// single gRPC server and the peer connections those groups share.
//
// It is built by StartNode, which both main.go and the integration tests
// call — so the tests exercise exactly the wiring that ships, rather than a
// copy of it (Milestone 9f).
type NodeRuntime struct {
	// Groups maps each shard group to this node's replica of it. Read-only
	// after StartNode returns. Exported so tests can observe a group's state
	// or stop a single group's replica.
	Groups map[ring.GroupID]GroupReplica

	grpcServer *grpc.Server
	peerConns  map[uint64]*RaftPeerConn
	logs       map[ring.GroupID]*raft.Log

	// fatal receives one error per group whose state machine stops on an
	// error. Its capacity is the number of groups, and each group sends at
	// most once, so a failing group never blocks.
	fatal chan error

	stopOnce sync.Once
}

// StartNode opens every shard group's durable state under cfg.DataDir,
// starts the gRPC server on listener, and starts every group's Raft node.
//
// StartNode owns listener from the moment it is called: on success the gRPC
// server serves it until Stop; on failure it is closed. On failure every
// file and connection opened so far is released too. When this code lived
// in main.go a failure meant os.Exit and the OS cleaned up; a function that
// returns an error has to do that itself.
func StartNode(cfg *config.Config, listener net.Listener) (_ *NodeRuntime, err error) {
	// Built in a local, NOT in a named result: every `return nil, err` below
	// assigns nil to the named results BEFORE deferred functions run, so a
	// cleanup reading a named result would find nil and dereference it.
	// Only err is named, because the cleanup needs to know whether we failed.
	nodeRuntime := &NodeRuntime{
		Groups:    make(map[ring.GroupID]GroupReplica, cfg.ShardGroups),
		peerConns: make(map[uint64]*RaftPeerConn, len(cfg.PeerAddrs)),
		logs:      make(map[ring.GroupID]*raft.Log, cfg.ShardGroups),
		fatal:     make(chan error, cfg.ShardGroups),
	}
	defer func() {
		if err != nil {
			nodeRuntime.shutdown()
			listener.Close() // the gRPC server never took it over
		}
	}()

	// DataDir may not exist yet on a brand-new node (fresh docker volume,
	// fresh local run) — os.OpenFile's O_CREATE only creates the files
	// themselves, not missing parent directories.
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("server: StartNode: failed to create data dir %s: %w", cfg.DataDir, err)
	}

	// INVARIANT: the group count is pinned BEFORE any group directory is
	// created, so a crash between the two leaves no group data that the
	// pinned count does not describe. Starting with a different count than
	// the data directory was created with would strand existing keys in
	// groups the ring no longer sends them to.
	if err := config.PinShardGroups(cfg.DataDir, cfg.ShardGroups); err != nil {
		return nil, fmt.Errorf("server: StartNode: %w", err)
	}

	// One connection per peer NODE, shared by every group: ForGroup gives
	// each group a Peer that stamps its group_id. Dialing is lazy — this
	// doesn't block waiting for peers to be up, which matters since
	// docker-compose starts all nodes at roughly the same time.
	for peerID, addr := range cfg.PeerAddrs {
		peerConn, err := DialRaftPeer(addr)
		if err != nil {
			return nil, fmt.Errorf("server: StartNode: peer %d: %w", peerID, err)
		}
		nodeRuntime.peerConns[peerID] = peerConn
	}

	// Open every group before starting anything. A failure partway through
	// then leaves nothing running — in particular no state machine blocked
	// forever on an apply channel that nothing will ever close.
	applyChannels := make(map[ring.GroupID]chan raft.ApplyMsg, cfg.ShardGroups)
	for _, group := range cfg.Groups() {
		replica, log, applyCh, err := openGroup(cfg, group, nodeRuntime.peerConns)
		if err != nil {
			return nil, err
		}
		nodeRuntime.Groups[group] = replica
		nodeRuntime.logs[group] = log
		applyChannels[group] = applyCh
	}

	raftNodes := make(map[ring.GroupID]*raft.Node, len(nodeRuntime.Groups))
	for group, replica := range nodeRuntime.Groups {
		raftNodes[group] = replica.Raft
	}
	kvServer, err := NewKVServer(nodeRuntime.Groups)
	if err != nil {
		return nil, fmt.Errorf("server: StartNode: %w", err)
	}
	raftServer, err := NewRaftServer(raftNodes)
	if err != nil {
		return nil, fmt.Errorf("server: StartNode: %w", err)
	}

	nodeRuntime.grpcServer = grpc.NewServer()
	kvpb.RegisterKVStoreServer(nodeRuntime.grpcServer, kvServer)
	raftpb.RegisterRaftServer(nodeRuntime.grpcServer, raftServer) // same server, same port as KVStore

	// Exit (Rule 5): Serve returns once Stop's GracefulStop closes the
	// listener and finishes in-flight RPCs.
	go func() {
		if err := nodeRuntime.grpcServer.Serve(listener); err != nil {
			slog.Error("server: grpc server stopped unexpectedly", "error", err)
		}
	}()

	for group, replica := range nodeRuntime.Groups {
		group, stateMachine, applyCh := group, replica.StateMachine, applyChannels[group]
		// Exit (Rule 5): Run returns when Stop makes this group's applier
		// close applyCh. An ERROR from Run — an entry it cannot decode — is
		// reported on Fatal(); the caller decides what that means.
		go func() {
			if err := stateMachine.Run(applyCh); err != nil {
				nodeRuntime.fatal <- fmt.Errorf("server: group %d state machine failed: %w", group, err)
			}
		}()
		replica.Raft.Start()
	}

	slog.Info("server: node serving KVStore and Raft",
		"node_id", cfg.NodeID, "addr", listener.Addr().String(), "shard_groups", cfg.ShardGroups)
	return nodeRuntime, nil
}

// openGroup opens one shard group's durable state under
// DATA_DIR/group-<id>/ and builds its Raft node and state machine. Nothing
// is started yet.
func openGroup(cfg *config.Config, group ring.GroupID, peerConns map[uint64]*RaftPeerConn) (GroupReplica, *raft.Log, chan raft.ApplyMsg, error) {
	// Every group needs its OWN directory: two groups sharing one would
	// append two independent logs into the same file, and overwrite each
	// other's term and vote (Milestone 9d, break B3).
	groupDir := filepath.Join(cfg.DataDir, fmt.Sprintf("group-%d", group))
	if err := os.MkdirAll(groupDir, 0755); err != nil {
		return GroupReplica{}, nil, nil, fmt.Errorf("server: failed to create %s: %w", groupDir, err)
	}

	raftStatePath := filepath.Join(groupDir, "raft-state.dat")
	raftState, err := raft.LoadState(raftStatePath)
	if err != nil {
		return GroupReplica{}, nil, nil, fmt.Errorf("server: failed to load raft state %s: %w", raftStatePath, err)
	}

	// OpenLog loads the snapshot too, so the log's starting point and the
	// snapshot can never be opened out of step. The state machine is
	// restored from that same snapshot: the applier delivers it before any
	// entry, so the store never runs with the log's base but without the
	// data the snapshot holds.
	raftLogPath := filepath.Join(groupDir, "raft-log.dat")
	raftLog, err := raft.OpenLog(raftLogPath, filepath.Join(groupDir, "snapshot.dat"))
	if err != nil {
		return GroupReplica{}, nil, nil, fmt.Errorf("server: failed to open raft log %s: %w", raftLogPath, err)
	}

	peers := make(map[uint64]raft.Peer, len(peerConns))
	for peerID, peerConn := range peerConns {
		peers[peerID] = peerConn.ForGroup(group)
	}

	// The replicated state machine starts EMPTY — store and dedup table
	// both. On restart the applier first delivers the snapshot (if any),
	// then the committed log after it: that is the entire recovery path.
	//
	// The node is built first because the state machine snapshots through
	// it once the log passes SNAPSHOT_THRESHOLD_BYTES.
	applyCh := make(chan raft.ApplyMsg)
	raftNode := raft.NewNode(uint64(cfg.NodeID), raftState, raftLog, peers, applyCh,
		raft.WithLogger(slog.With("group_id", group)))
	stateMachine := NewStateMachine(store.NewStore(), dedup.NewTable(), raftNode, cfg.SnapshotThresholdBytes)

	return GroupReplica{Raft: raftNode, StateMachine: stateMachine}, raftLog, applyCh, nil
}

// Fatal reports a group whose state machine stopped on an error. A node
// with a possibly diverged replica in ANY group must not keep serving, so
// main exits on it; tests fail on it.
func (r *NodeRuntime) Fatal() <-chan error {
	return r.fatal
}

// Stop shuts the node down. Safe to call more than once.
func (r *NodeRuntime) Stop() {
	r.stopOnce.Do(r.shutdown)
}

// shutdown releases everything, in an order that matters. Stopping Raft
// first makes each group's applier close its applyCh, which ends that
// group's state machine, which releases every Put/Delete handler still
// waiting in WaitForApplied. Only then does GracefulStop have nothing left
// to wait on. In the reverse order, GracefulStop would sit waiting for
// handlers that are themselves waiting on applies that will never come, for
// up to applyWaitTimeout each.
//
// Also used to clean up after a failed StartNode, where some of these were
// never created.
func (r *NodeRuntime) shutdown() {
	for _, replica := range r.Groups {
		replica.Raft.Stop() // safe on a node that was never started, or already stopped
	}
	if r.grpcServer != nil {
		r.grpcServer.GracefulStop()
	}
	for peerID, peerConn := range r.peerConns {
		if err := peerConn.Close(); err != nil {
			slog.Error("server: failed to close raft peer connection", "peer_id", peerID, "error", err)
		}
	}
	for group, log := range r.logs {
		if err := log.Close(); err != nil {
			slog.Error("server: failed to close raft log", "group_id", group, "error", err)
		}
	}
}
