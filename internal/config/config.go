// Package config reads this node's identity and cluster topology from
// environment variables. Per CLAUDE.md, the project uses environment-only
// configuration — no config files — so every node in docker-compose.yml
// can be described by a handful of env vars instead of a mounted file.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds everything a node needs to know about itself and its peers
// before it can start listening for RPCs or opening its data directory.
//
// These fields are populated once, at startup, by Load(). They are never
// mutated afterward, so — unlike fields on RaftNode — no mutex is needed
// here: a value that is only ever read after construction cannot race.
type Config struct {
	// NodeID uniquely identifies this node within the cluster (e.g. 1, 2, 3).
	// Raft uses this to know which node cast which vote and which node
	// authored which log entry.
	NodeID int

	// PeerAddrs maps every OTHER node's ID to its host:port (not including
	// this node itself). Populated from PEER_ADDRS, e.g.
	// "2=node2:50051,3=node3:50051" -> {2: "node2:50051", 3: "node3:50051"}.
	// Keyed by ID, not just a bare address list, because raft.Node's peers
	// are looked up by node ID — RequestVote/AppendEntries need to know
	// WHICH node an address belongs to, not just that it exists.
	PeerAddrs map[uint64]string

	// GRPCPort is the TCP port this node's gRPC server listens on.
	GRPCPort int

	// DataDir is the directory on disk where this node's WAL, snapshots,
	// and any other persistent state are stored. Each node must have its
	// own directory — sharing one between nodes would corrupt both.
	DataDir string

	// SnapshotThresholdBytes is the Raft log size at which the state machine
	// takes a snapshot and compacts the log. Optional
	// (SNAPSHOT_THRESHOLD_BYTES); defaults to DefaultSnapshotThresholdBytes.
	SnapshotThresholdBytes int64
}

// DefaultSnapshotThresholdBytes is 1 MiB: small enough that compaction
// actually happens in a local cluster, large enough that a node isn't
// snapshotting after every handful of writes.
const DefaultSnapshotThresholdBytes = 1 << 20

// Load reads NODE_ID, PEER_ADDRS, GRPC_PORT, and DATA_DIR from the process
// environment and returns a validated Config. It returns an error naming the
// specific variable that is missing or malformed, so a misconfigured
// container fails fast at startup instead of failing confusingly later.
func Load() (*Config, error) {
	nodeIDRaw := os.Getenv("NODE_ID")
	if nodeIDRaw == "" {
		return nil, fmt.Errorf("config: required env var NODE_ID is not set")
	}
	nodeID, err := strconv.Atoi(nodeIDRaw)
	if err != nil {
		return nil, fmt.Errorf("config: NODE_ID=%q is not a valid integer: %w", nodeIDRaw, err)
	}

	grpcPortRaw := os.Getenv("GRPC_PORT")
	if grpcPortRaw == "" {
		return nil, fmt.Errorf("config: required env var GRPC_PORT is not set")
	}
	grpcPort, err := strconv.Atoi(grpcPortRaw)
	if err != nil {
		return nil, fmt.Errorf("config: GRPC_PORT=%q is not a valid integer: %w", grpcPortRaw, err)
	}

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		return nil, fmt.Errorf("config: required env var DATA_DIR is not set")
	}

	// PEER_ADDRS is allowed to be empty in principle (a single-node cluster
	// for local testing), so we only split on commas — we don't error on
	// an empty string. strings.Split("", ",") would return [""] (one empty
	// element, not zero elements), so we guard that case explicitly.
	peerAddrs := make(map[uint64]string)
	if raw := os.Getenv("PEER_ADDRS"); raw != "" {
		for _, entry := range strings.Split(raw, ",") {
			idAndAddr := strings.SplitN(entry, "=", 2)
			if len(idAndAddr) != 2 {
				return nil, fmt.Errorf("config: PEER_ADDRS entry %q is not in ID=address form", entry)
			}
			peerID, err := strconv.ParseUint(idAndAddr[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("config: PEER_ADDRS entry %q has a non-integer ID: %w", entry, err)
			}
			peerAddrs[peerID] = idAndAddr[1]
		}
	}

	snapshotThreshold := int64(DefaultSnapshotThresholdBytes)
	if raw := os.Getenv("SNAPSHOT_THRESHOLD_BYTES"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("config: SNAPSHOT_THRESHOLD_BYTES=%q is not a positive integer", raw)
		}
		snapshotThreshold = parsed
	}

	return &Config{
		NodeID:    nodeID,
		PeerAddrs: peerAddrs,
		GRPCPort:  grpcPort,
		DataDir:   dataDir,

		SnapshotThresholdBytes: snapshotThreshold,
	}, nil
}
