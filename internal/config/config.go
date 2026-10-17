// Package config reads this node's identity and cluster topology from
// environment variables. Per CLAUDE.md, the project uses environment-only
// configuration — no config files — so every node in docker-compose.yml
// can be described by a handful of env vars instead of a mounted file.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/imPranav14/Distributed-KVstore/internal/ring"
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

	// ShardGroups is how many Raft groups — shards — every node runs
	// (Milestone 9). Required, with no default: it decides which group owns
	// every key, so a default silently applied on one node that forgot to
	// set it would be exactly the ring mismatch the system defends against.
	// It is also pinned in the data directory (PinShardGroups), because
	// changing it later would strand existing data.
	ShardGroups int
}

// Groups returns the IDs of this cluster's shard groups: 1..ShardGroups.
// They start at 1 because group 0 is reserved for "group_id was not set".
func (c *Config) Groups() []ring.GroupID {
	groups := make([]ring.GroupID, c.ShardGroups)
	for i := range groups {
		groups[i] = ring.GroupID(i + 1)
	}
	return groups
}

// DefaultSnapshotThresholdBytes is 1 MiB: small enough that compaction
// actually happens in a local cluster, large enough that a node isn't
// snapshotting after every handful of writes.
const DefaultSnapshotThresholdBytes = 1 << 20

// Load reads NODE_ID, PEER_ADDRS, GRPC_PORT, DATA_DIR and SHARD_GROUPS from the process
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

	shardGroupsRaw := os.Getenv("SHARD_GROUPS")
	if shardGroupsRaw == "" {
		return nil, fmt.Errorf("config: required env var SHARD_GROUPS is not set")
	}
	shardGroups, err := strconv.Atoi(shardGroupsRaw)
	if err != nil || shardGroups <= 0 {
		return nil, fmt.Errorf("config: SHARD_GROUPS=%q is not a positive integer", shardGroupsRaw)
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
		ShardGroups:            shardGroups,
	}, nil
}

// shardGroupsFile, inside DATA_DIR, records the group count the directory
// was created with.
const shardGroupsFile = "shard-groups"

// preShardingFiles are the files a node from before Milestone 9 kept at the
// ROOT of its data directory, for its single Raft group. From 9d on they
// live in one group-<id> subdirectory per group.
var preShardingFiles = []string{"raft-state.dat", "raft-log.dat", "snapshot.dat"}

// PinShardGroups makes the group count part of the data directory: recorded
// on first use, and required to match on every start after that.
//
// This is the one piece of configuration that is also persisted, because
// changing it is not a configuration change — it is a data migration. Going
// from 3 groups to 4 sends about a quarter of all keys to a new, empty
// group 4: their data sits on disk in the old groups, unreachable, and new
// writes create second copies. Resharding is not supported (Milestone 9,
// choice 5A), so a mismatch refuses to start rather than doing that.
//
// INVARIANT: main.go calls this BEFORE creating any group directory, so a
// crash between the two leaves no group data that the marker does not
// describe.
func PinShardGroups(dataDir string, shardGroups int) error {
	markerPath := filepath.Join(dataDir, shardGroupsFile)
	raw, err := os.ReadFile(markerPath)

	switch {
	case err == nil:
		recorded, parseErr := strconv.Atoi(strings.TrimSpace(string(raw)))
		if parseErr != nil {
			return fmt.Errorf("config: PinShardGroups: %s holds %q, not a group count (corrupt)", markerPath, raw)
		}
		if recorded != shardGroups {
			return fmt.Errorf("config: PinShardGroups: data dir %s was created with SHARD_GROUPS=%d; starting with %d "+
				"would send keys to other groups and make existing data unreachable. Resharding is not supported: "+
				"restore SHARD_GROUPS=%d, or wipe the data dir", dataDir, recorded, shardGroups, recorded)
		}
		return nil

	case os.IsNotExist(err):
		for _, name := range preShardingFiles {
			if _, statErr := os.Stat(filepath.Join(dataDir, name)); statErr == nil {
				return fmt.Errorf("config: PinShardGroups: data dir %s holds %s from before sharding; "+
					"migrating it is not supported — wipe the data dir", dataDir, name)
			}
		}
		return writeFileAtomically(dataDir, markerPath, []byte(strconv.Itoa(shardGroups)+"\n"))

	default:
		return fmt.Errorf("config: PinShardGroups: failed to read %s: %w", markerPath, err)
	}
}

// writeFileAtomically replaces path with data so that a crash at any moment
// leaves either no file or the complete one: temp file, fsync, rename, then
// fsync the directory so the rename itself survives a crash. The same
// pattern as raft.SaveSnapshot.
func writeFileAtomically(dir, path string, data []byte) error {
	temp, err := os.CreateTemp(dir, filepath.Base(path)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("config: failed to create temp file for %s: %w", path, err)
	}
	tempPath := temp.Name()

	if _, err := temp.Write(data); err != nil {
		temp.Close()
		os.Remove(tempPath)
		return fmt.Errorf("config: failed to write %s: %w", tempPath, err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		os.Remove(tempPath)
		return fmt.Errorf("config: failed to fsync %s: %w", tempPath, err)
	}
	if err := temp.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("config: failed to close %s: %w", tempPath, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("config: failed to rename %s into place: %w", path, err)
	}

	dirHandle, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("config: failed to open %s to fsync it: %w", dir, err)
	}
	defer dirHandle.Close()
	if err := dirHandle.Sync(); err != nil {
		return fmt.Errorf("config: failed to fsync directory %s: %w", dir, err)
	}
	return nil
}
