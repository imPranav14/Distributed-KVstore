package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// setEnv sets the given env vars for the duration of the test, restoring
// whatever was there before (including "unset") once the test finishes —
// so tests never leak environment state into each other or into whatever
// runs after `go test`.
func setEnv(t *testing.T, vars map[string]string) {
	t.Helper()
	for k, v := range vars {
		registerEnvRestore(t, k)
		os.Setenv(k, v)
	}
}

// unsetEnv clears an env var for the duration of the test, restoring
// whatever was there before once the test finishes — same restoration
// guarantee as setEnv, just for the "must be absent" case.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	registerEnvRestore(t, key)
	os.Unsetenv(key)
}

// registerEnvRestore captures key's CURRENT value before any test-induced
// change, and restores exactly that (present-with-value, or absent) when
// the test finishes. Must be called before the test mutates key.
func registerEnvRestore(t *testing.T, key string) {
	t.Helper()
	old, existed := os.LookupEnv(key)
	t.Cleanup(func() {
		if existed {
			os.Setenv(key, old)
		} else {
			os.Unsetenv(key)
		}
	})
}

func baseEnv() map[string]string {
	return map[string]string{
		"NODE_ID":      "1",
		"GRPC_PORT":    "50051",
		"DATA_DIR":     "/data/node1",
		"SHARD_GROUPS": "3",
	}
}

// This test checks the invariant that a fully valid environment, including
// multiple PEER_ADDRS entries, parses into a correctly ID-keyed map.
func TestLoad_ValidConfig_ParsesPeerAddrsByID(t *testing.T) {
	env := baseEnv()
	env["PEER_ADDRS"] = "2=node2:50051,3=node3:50051"
	setEnv(t, env)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.NodeID != 1 || cfg.GRPCPort != 50051 || cfg.DataDir != "/data/node1" {
		t.Fatalf("basic fields = %+v, want NodeID=1 GRPCPort=50051 DataDir=/data/node1", cfg)
	}
	if len(cfg.PeerAddrs) != 2 {
		t.Fatalf("len(PeerAddrs) = %d, want 2", len(cfg.PeerAddrs))
	}
	if cfg.PeerAddrs[2] != "node2:50051" {
		t.Fatalf("PeerAddrs[2] = %q, want %q", cfg.PeerAddrs[2], "node2:50051")
	}
	if cfg.PeerAddrs[3] != "node3:50051" {
		t.Fatalf("PeerAddrs[3] = %q, want %q", cfg.PeerAddrs[3], "node3:50051")
	}
}

// This test checks the invariant that an empty PEER_ADDRS (a single-node
// cluster for local testing) produces an empty, non-nil map rather than an
// error.
func TestLoad_EmptyPeerAddrs_ProducesEmptyMap(t *testing.T) {
	setEnv(t, baseEnv()) // no PEER_ADDRS set at all

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.PeerAddrs) != 0 {
		t.Fatalf("len(PeerAddrs) = %d, want 0", len(cfg.PeerAddrs))
	}
}

// This test checks the invariant that a PEER_ADDRS entry missing the
// "ID=" prefix is rejected with a specific error, not silently
// misinterpreted or panicking on a malformed split.
func TestLoad_PeerAddrsMissingEquals_ReturnsError(t *testing.T) {
	env := baseEnv()
	env["PEER_ADDRS"] = "node2:50051" // no "2=" prefix
	setEnv(t, env)

	_, err := Load()
	if err == nil {
		t.Fatalf("Load with a malformed PEER_ADDRS entry: err = nil, want an error")
	}
}

// This test checks the invariant that a non-integer peer ID is rejected
// with a specific error naming the bad value.
func TestLoad_PeerAddrsNonIntegerID_ReturnsError(t *testing.T) {
	env := baseEnv()
	env["PEER_ADDRS"] = "two=node2:50051"
	setEnv(t, env)

	_, err := Load()
	if err == nil {
		t.Fatalf("Load with a non-integer peer ID: err = nil, want an error")
	}
}

// This test checks the invariant that each required env var is validated
// independently, with an error naming the specific missing variable.
func TestLoad_MissingRequiredVars_ReturnsSpecificError(t *testing.T) {
	tests := []struct {
		name    string
		missing string
	}{
		{"missing NODE_ID", "NODE_ID"},
		{"missing GRPC_PORT", "GRPC_PORT"},
		{"missing DATA_DIR", "DATA_DIR"},
		{"missing SHARD_GROUPS", "SHARD_GROUPS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := baseEnv()
			delete(env, tt.missing)
			setEnv(t, env)
			unsetEnv(t, tt.missing)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load with %s unset: err = nil, want an error", tt.missing)
			}
		})
	}
}

// This test checks the invariant that a non-integer NODE_ID or GRPC_PORT
// is rejected rather than silently zero-valued.
func TestLoad_NonIntegerNodeID_ReturnsError(t *testing.T) {
	env := baseEnv()
	env["NODE_ID"] = "not-a-number"
	setEnv(t, env)

	_, err := Load()
	if err == nil {
		t.Fatalf("Load with non-integer NODE_ID: err = nil, want an error")
	}
}

// This test checks that SNAPSHOT_THRESHOLD_BYTES is optional and defaults
// to DefaultSnapshotThresholdBytes when unset.
func TestLoad_SnapshotThreshold_DefaultsWhenUnset(t *testing.T) {
	setEnv(t, baseEnv())
	unsetEnv(t, "SNAPSHOT_THRESHOLD_BYTES")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.SnapshotThresholdBytes != DefaultSnapshotThresholdBytes {
		t.Fatalf("SnapshotThresholdBytes = %d, want default %d", cfg.SnapshotThresholdBytes, DefaultSnapshotThresholdBytes)
	}
}

// This test checks that a valid override is used, and that zero, negative
// and non-integer values are rejected rather than silently disabling or
// mangling compaction.
func TestLoad_SnapshotThreshold_OverrideAndValidation(t *testing.T) {
	env := baseEnv()
	env["SNAPSHOT_THRESHOLD_BYTES"] = "4096"
	setEnv(t, env)
	cfg, err := Load()
	if err != nil || cfg.SnapshotThresholdBytes != 4096 {
		t.Fatalf("Load with 4096 = (%v, %v), want 4096", cfg, err)
	}

	for _, bad := range []string{"0", "-5", "lots"} {
		t.Run(bad, func(t *testing.T) {
			env := baseEnv()
			env["SNAPSHOT_THRESHOLD_BYTES"] = bad
			setEnv(t, env)
			if _, err := Load(); err == nil {
				t.Fatalf("SNAPSHOT_THRESHOLD_BYTES=%q accepted, want an error", bad)
			}
		})
	}
}

// This test checks that SHARD_GROUPS is parsed into the group list every
// process builds its ring from — 1..N, never starting at 0, which is
// reserved for "group_id not set" — and that a value that could silently
// produce a different ring (non-integer, zero, negative) is refused.
func TestLoad_ShardGroups_ParsedIntoGroupsAndValidated(t *testing.T) {
	setEnv(t, baseEnv())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	groups := cfg.Groups()
	if len(groups) != 3 || groups[0] != 1 || groups[1] != 2 || groups[2] != 3 {
		t.Fatalf("Groups() = %v, want [1 2 3]", groups)
	}

	for _, bad := range []string{"three", "0", "-1"} {
		env := baseEnv()
		env["SHARD_GROUPS"] = bad
		setEnv(t, env)
		if _, err := Load(); err == nil {
			t.Fatalf("Load with SHARD_GROUPS=%q: err = nil, want an error", bad)
		}
	}
}

// This test checks the first and second start of a data directory: the
// first records the group count, and a restart with the SAME count passes.
func TestPinShardGroups_RecordsCountAndAcceptsTheSameCountAgain(t *testing.T) {
	dir := t.TempDir()
	if err := PinShardGroups(dir, 3); err != nil {
		t.Fatalf("first PinShardGroups: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, shardGroupsFile))
	if err != nil || strings.TrimSpace(string(raw)) != "3" {
		t.Fatalf("marker = %q, %v; want \"3\"", raw, err)
	}
	if err := PinShardGroups(dir, 3); err != nil {
		t.Fatalf("restart with the same count: %v", err)
	}
}

// This test checks the INVARIANT that the group count is fixed when the
// data directory is created. Restarting with 4 groups would move about a
// quarter of all keys to a brand-new, empty group 4: their data still on
// disk in the old groups, unreachable, and new writes making second copies.
// The refusal must name both counts so an operator knows what to restore.
func TestPinShardGroups_RefusesAChangedCount(t *testing.T) {
	dir := t.TempDir()
	if err := PinShardGroups(dir, 3); err != nil {
		t.Fatalf("PinShardGroups: %v", err)
	}

	err := PinShardGroups(dir, 4)
	if err == nil {
		t.Fatalf("restart with SHARD_GROUPS=4 on a 3-group data dir succeeded")
	}
	if !strings.Contains(err.Error(), "SHARD_GROUPS=3") || !strings.Contains(err.Error(), "4") {
		t.Fatalf("error %q should name both the recorded count (3) and the new one (4)", err)
	}
}

// This test checks that a data directory from before sharding — its single
// group's files at the root — is refused, not treated as fresh. Treating it
// as fresh would start empty groups beside the old files, and every key
// written before the upgrade would look lost.
func TestPinShardGroups_RefusesPreShardingData(t *testing.T) {
	for _, name := range []string{"raft-state.dat", "raft-log.dat", "snapshot.dat"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, name), []byte("old"), 0o644); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			if err := PinShardGroups(dir, 3); err == nil {
				t.Fatalf("a data dir with a root-level %s was accepted as fresh", name)
			}
			if _, err := os.Stat(filepath.Join(dir, shardGroupsFile)); !os.IsNotExist(err) {
				t.Fatalf("a marker was written into a refused pre-sharding data dir")
			}
		})
	}
}

// This test checks that an unreadable marker is an error. Treating it as
// "no marker" would re-pin whatever count this start happens to use.
func TestPinShardGroups_RefusesACorruptMarker(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, shardGroupsFile), []byte("three\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := PinShardGroups(dir, 3); err == nil {
		t.Fatalf("a corrupt marker was accepted")
	}
}
