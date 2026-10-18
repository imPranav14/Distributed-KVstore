// Command node is the entry point for a single KV store node: it reads its
// configuration, starts the node, and shuts it down on a signal.
//
// Everything a node consists of — one Raft group per shard, each with its
// own data under DATA_DIR/group-<id>/, behind one gRPC port — is wired by
// server.StartNode (Milestone 9f). It lives there rather than here so the
// integration tests run exactly the wiring that ships.
package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/imPranav14/Distributed-KVstore/internal/config"
	"github.com/imPranav14/Distributed-KVstore/internal/server"
)

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
		"shard_groups", cfg.ShardGroups,
	)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		slog.Error("node: failed to listen", "grpc_port", cfg.GRPCPort, "error", err)
		os.Exit(1)
	}

	runtime, err := server.StartNode(cfg, listener)
	if err != nil {
		slog.Error("node: refusing to start", "error", err)
		os.Exit(1)
	}

	// Block until the process receives SIGINT (Ctrl+C) or SIGTERM (sent by
	// `docker compose stop` / `make kill1`), or a group's state machine
	// fails. Without this, main() would return immediately and the
	// container would exit right after printing the startup log.
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)

	select {
	case receivedSignal := <-shutdownSignal:
		slog.Info("node: shutting down", "signal", receivedSignal.String())
		runtime.Stop()

	case err := <-runtime.Fatal():
		// Fail-stop, exactly as before 9f: a replica whose state may have
		// diverged from the others — in ANY group — must not keep serving,
		// not even long enough for a graceful shutdown.
		slog.Error("node: state machine failed; exiting", "error", err)
		os.Exit(1)
	}
}
