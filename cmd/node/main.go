// Command node is the entry point for a single KV store node. As of
// Milestone 3, it serves real KV requests over gRPC against its own local
// store — no replication or consensus yet (Milestones 5-7), so each node is
// independent of its peers for now even though PEER_ADDRS is already read
// from config.
package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/imPranav14/Distributed-KVstore/internal/config"
	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/server"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
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

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		slog.Error("node: failed to listen", "grpc_port", cfg.GRPCPort, "error", err)
		os.Exit(1)
	}

	kvServer := server.NewKVServer(store.NewStore(), dedup.NewTable())
	grpcServer := grpc.NewServer()
	kvpb.RegisterKVStoreServer(grpcServer, kvServer)

	// This goroutine's exit condition is grpcServer.GracefulStop() below,
	// called once a shutdown signal is received — Serve returns once
	// GracefulStop closes the listener and finishes in-flight RPCs.
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			slog.Error("node: grpc server stopped unexpectedly", "error", err)
		}
	}()
	slog.Info("node: serving KVStore", "addr", listener.Addr().String())

	// Block here until the process receives SIGINT (Ctrl+C) or SIGTERM
	// (sent by `docker compose stop` / `make kill1`). Without this, main()
	// would return immediately and the container would exit right after
	// printing the startup log — nothing would be left running to stop.
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)
	receivedSignal := <-shutdownSignal

	slog.Info("node: shutting down", "signal", receivedSignal.String())
	// GracefulStop lets any in-flight RPC finish instead of cutting it off
	// mid-request, then closes the listener, which is what makes the Serve
	// goroutine above return.
	grpcServer.GracefulStop()
}
