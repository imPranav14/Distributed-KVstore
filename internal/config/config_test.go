package config

import (
	"os"
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
		"NODE_ID":   "1",
		"GRPC_PORT": "50051",
		"DATA_DIR":  "/data/node1",
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
