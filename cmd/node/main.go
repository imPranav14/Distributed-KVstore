// Command node is the entry point for a single KV store node. At this
// milestone it does not yet run a Raft state machine or serve gRPC — it only
// proves that a node can read its identity from the environment and that
// docker-compose is wiring NODE_ID / PEER_ADDRS / GRPC_PORT / DATA_DIR
// correctly to each of the three containers. Later milestones replace the
// "wait for shutdown" block below with actual server startup.
package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/imPranav14/Distributed-KVstore/internal/config"
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
	)

	// Block here until the process receives SIGINT (Ctrl+C) or SIGTERM
	// (sent by `docker compose stop` / `make kill1`). Without this, main()
	// would return immediately and the container would exit right after
	// printing the startup log — nothing would be left running to stop.
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)
	receivedSignal := <-shutdownSignal

	slog.Info("node: shutting down", "signal", receivedSignal.String())
}
