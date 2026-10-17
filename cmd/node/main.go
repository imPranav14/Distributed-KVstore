// Command node is the entry point for a single KV store node. As of
// Milestone 7b, every client write goes through Raft: the leader proposes
// it, a majority stores it, and every node's state machine applies it in
// the same order. The Raft log is the only write-ahead log — the separate
// KV WAL from Milestone 4 is gone, and a restarted node rebuilds its store
// by re-applying its committed log from index 1.
package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"google.golang.org/grpc"

	"github.com/imPranav14/Distributed-KVstore/internal/config"
	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/ring"
	"github.com/imPranav14/Distributed-KVstore/internal/server"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// onlyGroup is the single Raft group this binary runs until Milestone 9c
// starts one group per shard. It is 1, not 0: group 0 is reserved for "the
// sender never set group_id" and is refused by every RaftServer.
const onlyGroup ring.GroupID = 1

func main() {
	cfg, err := config.Load()
	if err != nil {
		// Config errors are fatal: a node that doesn't know its own ID or
		// data directory cannot safely participate in the cluster at all.
		slog.Error("node: failed to load config", "error", err)
		os.Exit(1)
	}

	slog.Info("node: starting",
		"node_id", cfg.NodeID,
		"peer_addrs", cfg.PeerAddrs,
		"grpc_port", cfg.GRPCPort,
		"data_dir", cfg.DataDir,
	)

	// DataDir may not exist yet on a brand-new node (fresh docker volume,
	// fresh local run) — os.OpenFile's O_CREATE only creates the files
	// themselves, not missing parent directories.
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		slog.Error("node: failed to create data directory", "data_dir", cfg.DataDir, "error", err)
		os.Exit(1)
	}

	raftStatePath := filepath.Join(cfg.DataDir, "raft-state.dat")
	raftState, err := raft.LoadState(raftStatePath)
	if err != nil {
		slog.Error("node: failed to load raft state", "path", raftStatePath, "error", err)
		os.Exit(1)
	}

	raftLogPath := filepath.Join(cfg.DataDir, "raft-log.dat")
	// OpenLog loads the snapshot too, so the log's starting point and the
	// snapshot can never be opened out of step. The state machine is
	// restored from that same snapshot below: the applier delivers it before
	// any entry, so the store never runs with the log's base but without the
	// data the snapshot holds.
	raftLog, err := raft.OpenLog(raftLogPath, filepath.Join(cfg.DataDir, "snapshot.dat"))
	if err != nil {
		slog.Error("node: failed to open raft log", "path", raftLogPath, "error", err)
		os.Exit(1)
	}

	// Dialing is lazy (see raftPeerClient's doc comment) — this doesn't
	// block waiting for peers to be up, which matters since docker-compose
	// starts all three containers at roughly the same time and none of
	// them are guaranteed to be listening yet.
	//
	// One connection per peer node. In 9c every group on this node shares
	// it; ForGroup gives each group a Peer that stamps its group_id.
	peers := make(map[uint64]raft.Peer, len(cfg.PeerAddrs))
	peerConns := make([]*server.RaftPeerConn, 0, len(cfg.PeerAddrs))
	for peerID, addr := range cfg.PeerAddrs {
		peerConn, err := server.DialRaftPeer(addr)
		if err != nil {
			slog.Error("node: failed to create raft peer client", "peer_id", peerID, "addr", addr, "error", err)
			os.Exit(1)
		}
		peerConns = append(peerConns, peerConn)
		peers[peerID] = peerConn.ForGroup(onlyGroup)
	}

	// The replicated state machine starts EMPTY — store and dedup table
	// both. On restart the applier first delivers the snapshot (if any),
	// then the committed log after it: that is the entire recovery path.
	//
	// The node is built first because the state machine snapshots through
	// it once the log passes SNAPSHOT_THRESHOLD_BYTES.
	applyCh := make(chan raft.ApplyMsg)
	raftNode := raft.NewNode(uint64(cfg.NodeID), raftState, raftLog, peers, applyCh)
	stateMachine := server.NewStateMachine(store.NewStore(), dedup.NewTable(), raftNode, cfg.SnapshotThresholdBytes)

	// Exit (Rule 5): Run returns when raftNode.Stop() makes the applier
	// close applyCh. An apply ERROR is fatal: the state machine fail-stops on
	// an entry it cannot decode, and a node whose state might have diverged
	// from the other replicas must not keep serving.
	go func() {
		if err := stateMachine.Run(applyCh); err != nil {
			slog.Error("node: state machine failed; exiting", "error", err)
			os.Exit(1)
		}
	}()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		slog.Error("node: failed to listen", "grpc_port", cfg.GRPCPort, "error", err)
		os.Exit(1)
	}

	kvServer := server.NewKVServer(raftNode, stateMachine)
	raftServer, err := server.NewRaftServer(map[ring.GroupID]*raft.Node{onlyGroup: raftNode})
	if err != nil {
		slog.Error("node: failed to create raft server", "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	kvpb.RegisterKVStoreServer(grpcServer, kvServer)
	raftpb.RegisterRaftServer(grpcServer, raftServer) // same server, same port as KVStore

	// This goroutine's exit condition is grpcServer.GracefulStop() below,
	// called once a shutdown signal is received — Serve returns once
	// GracefulStop closes the listener and finishes in-flight RPCs.
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			slog.Error("node: grpc server stopped unexpectedly", "error", err)
		}
	}()
	slog.Info("node: serving KVStore and Raft", "addr", listener.Addr().String())

	// raftNode's timer loop and applier both exit on raftNode.Stop() below —
	// same Rule 5 pattern as the gRPC Serve goroutine above.
	raftNode.Start()

	// Block here until the process receives SIGINT (Ctrl+C) or SIGTERM
	// (sent by `docker compose stop` / `make kill1`). Without this, main()
	// would return immediately and the container would exit right after
	// printing the startup log — nothing would be left running to stop.
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)
	receivedSignal := <-shutdownSignal

	slog.Info("node: shutting down", "signal", receivedSignal.String())

	// Order matters. Stopping Raft first makes the applier close applyCh,
	// which ends stateMachine.Run, which closes its stopped channel — and
	// that immediately releases every Put/Delete handler still waiting in
	// WaitForApplied. Only then does GracefulStop have nothing left to wait
	// on. In the reverse order, GracefulStop would sit waiting for handlers
	// that are themselves waiting on applies that will never come, for up to
	// applyWaitTimeout each.
	raftNode.Stop()
	grpcServer.GracefulStop()

	for _, peerConn := range peerConns {
		if err := peerConn.Close(); err != nil {
			slog.Error("node: failed to close raft peer connection", "error", err)
		}
	}

	if err := raftLog.Close(); err != nil {
		slog.Error("node: failed to close raft log", "error", err)
	}
}
