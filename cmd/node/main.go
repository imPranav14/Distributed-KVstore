// Command node is the entry point for a single KV store node. As of
// Milestone 5, nodes actually talk to each other: each one runs Raft leader
// election against its peers (terms, randomized timeouts, RequestVote,
// heartbeats) and the cluster elects — and, if the leader disappears,
// re-elects — a single leader. Log replication and routing client writes
// through consensus are still Milestone 6/7; a node accepts KV requests
// against its own local state regardless of its Raft role for now.
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
	"github.com/imPranav14/Distributed-KVstore/internal/server"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	"github.com/imPranav14/Distributed-KVstore/internal/wal"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// replayWAL rebuilds a Store's state from every entry previously recorded
// in w, in order. It must run to completion BEFORE the gRPC server starts
// accepting requests — serving a request against a store that hasn't
// finished recovering could return stale or missing data. Every Op value
// here is guaranteed valid (OpPut or OpDelete) because wal.DecodeEntry
// already rejected anything else during ReadAll — no default case needed
// for a state that can't occur.
func replayWAL(w *wal.WAL) (*store.Store, error) {
	entries, err := w.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("main: failed to read WAL for replay: %w", err)
	}

	s := store.NewStore()
	for _, entry := range entries {
		switch entry.Op {
		case wal.OpPut:
			s.Put(entry.Key, entry.Value)
		case wal.OpDelete:
			s.Delete(entry.Key)
		}
	}

	slog.Info("node: replayed WAL", "entries_replayed", len(entries))
	return s, nil
}

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
	// fresh local run) — os.OpenFile's O_CREATE only creates the WAL file
	// itself, not missing parent directories.
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		slog.Error("node: failed to create data directory", "data_dir", cfg.DataDir, "error", err)
		os.Exit(1)
	}

	walPath := filepath.Join(cfg.DataDir, "wal.log")
	w, err := wal.Open(walPath)
	if err != nil {
		slog.Error("node: failed to open WAL", "path", walPath, "error", err)
		os.Exit(1)
	}

	recoveredStore, err := replayWAL(w)
	if err != nil {
		slog.Error("node: failed to replay WAL", "error", err)
		os.Exit(1)
	}

	raftStatePath := filepath.Join(cfg.DataDir, "raft-state.dat")
	raftState, err := raft.LoadState(raftStatePath)
	if err != nil {
		slog.Error("node: failed to load raft state", "path", raftStatePath, "error", err)
		os.Exit(1)
	}

	// Dialing is lazy (see raftPeerClient's doc comment) — this doesn't
	// block waiting for peers to be up, which matters since docker-compose
	// starts all three containers at roughly the same time and none of
	// them are guaranteed to be listening yet.
	peers := make(map[uint64]raft.Peer, len(cfg.PeerAddrs))
	for peerID, addr := range cfg.PeerAddrs {
		peerClient, err := server.NewRaftPeerClient(addr)
		if err != nil {
			slog.Error("node: failed to create raft peer client", "peer_id", peerID, "addr", addr, "error", err)
			os.Exit(1)
		}
		peers[peerID] = peerClient
	}

	raftLogPath := filepath.Join(cfg.DataDir, "raft-log.dat")
	raftLog, err := raft.OpenLog(raftLogPath)
	if err != nil {
		slog.Error("node: failed to open raft log", "path", raftLogPath, "error", err)
		os.Exit(1)
	}

	raftNode := raft.NewNode(uint64(cfg.NodeID), raftState, raftLog, peers)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		slog.Error("node: failed to listen", "grpc_port", cfg.GRPCPort, "error", err)
		os.Exit(1)
	}

	// dedup.NewTable() is deliberately fresh, not reconstructed from the
	// WAL: dedup state is an accepted, named gap across restarts (see
	// UNDERSTANDING.md's Milestone 4 entry) — PUT/DELETE are naturally
	// idempotent, so a client retry landing after a restart just reapplies
	// the same value rather than corrupting anything.
	kvServer := server.NewKVServer(recoveredStore, dedup.NewTable(), w)
	raftServer := server.NewRaftServer(raftNode)

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

	// raftNode.run()'s exit condition is raftNode.Stop() below, on
	// shutdown — same Rule 5 pattern as the gRPC Serve goroutine above.
	raftNode.Start()

	// Block here until the process receives SIGINT (Ctrl+C) or SIGTERM
	// (sent by `docker compose stop` / `make kill1`). Without this, main()
	// would return immediately and the container would exit right after
	// printing the startup log — nothing would be left running to stop.
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)
	receivedSignal := <-shutdownSignal

	slog.Info("node: shutting down", "signal", receivedSignal.String())
	raftNode.Stop()
	// GracefulStop lets any in-flight RPC finish instead of cutting it off
	// mid-request, then closes the listener, which is what makes the Serve
	// goroutine above return.
	grpcServer.GracefulStop()

	if err := w.Close(); err != nil {
		slog.Error("node: failed to close WAL", "error", err)
	}
	if err := raftLog.Close(); err != nil {
		slog.Error("node: failed to close raft log", "error", err)
	}
}
