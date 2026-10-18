package server

import (
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/imPranav14/Distributed-KVstore/internal/config"
	"github.com/imPranav14/Distributed-KVstore/internal/raft"
)

// singleNodeConfig is a one-node cluster (no peers) over dataDir. With no
// peers, every group elects itself and commits on its own.
func singleNodeConfig(dataDir string, shardGroups int) *config.Config {
	return &config.Config{
		NodeID:                 1,
		PeerAddrs:              map[uint64]string{},
		DataDir:                dataDir,
		ShardGroups:            shardGroups,
		SnapshotThresholdBytes: config.DefaultSnapshotThresholdBytes,
	}
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	return listener
}

// startAndStop runs StartNode once and shuts it down, leaving a data
// directory exactly as a real node would.
func startAndStop(t *testing.T, dataDir string, shardGroups int) {
	t.Helper()
	nodeRuntime, err := StartNode(singleNodeConfig(dataDir, shardGroups), listenLocal(t))
	if err != nil {
		t.Fatalf("StartNode: %v", err)
	}
	for _, replica := range nodeRuntime.Groups {
		waitForRole(t, replica.Raft, raft.Leader) // with no peers, every group elects itself
	}
	nodeRuntime.Stop()
}

// appendRawLogEntry writes one entry in the Raft log's on-disk format:
// [8-byte term][8-byte index][4-byte command length][command].
func appendRawLogEntry(t *testing.T, logPath string, term, index uint64, command []byte) {
	t.Helper()
	file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer file.Close()
	header := make([]byte, 20)
	binary.BigEndian.PutUint64(header[0:8], term)
	binary.BigEndian.PutUint64(header[8:16], index)
	binary.BigEndian.PutUint32(header[16:20], uint32(len(command)))
	if _, err := file.Write(append(header, command...)); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

// openFileCount counts this process's open file descriptors by listing
// /dev/fd by NAME. (os.ReadDir would also stat each entry, which fails on
// macOS for descriptors that change while the directory is read.)
func openFileCount(t *testing.T) int {
	t.Helper()
	dir, err := os.Open("/dev/fd")
	if err != nil {
		t.Skipf("cannot count open files here: %v", err)
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		t.Skipf("cannot count open files here: %v", err)
	}
	return len(names)
}

// This test checks that the PRODUCTION start path actually calls the
// group-count pin. 9d tested PinShardGroups on its own; this catches the
// wiring forgetting to call it — which would let a node restarted with a
// different SHARD_GROUPS silently strand existing keys.
func TestStartNode_RefusesADataDirCreatedWithAnotherGroupCount(t *testing.T) {
	dataDir := t.TempDir()
	startAndStop(t, dataDir, 3)

	listener := listenLocal(t)
	if _, err := StartNode(singleNodeConfig(dataDir, 4), listener); err == nil || !strings.Contains(err.Error(), "SHARD_GROUPS=3") {
		t.Fatalf("StartNode with 4 groups on a 3-group data dir: err = %v, want the pin's refusal", err)
	}

	// The INVARIANT that the pin runs BEFORE any group directory is created:
	// a refused start must leave no trace of the group it refused.
	if _, err := os.Stat(filepath.Join(dataDir, "group-4")); !os.IsNotExist(err) {
		t.Fatalf("a refused start created group-4/ — group directories were created before the pin was checked")
	}
}

// This test checks that every group gets its OWN directory. Groups sharing
// one would append independent logs into the same file (9d, break B3).
func TestStartNode_GivesEachGroupItsOwnDirectory(t *testing.T) {
	dataDir := t.TempDir()
	startAndStop(t, dataDir, 3)

	for _, name := range []string{"shard-groups", "group-1/raft-log.dat", "group-2/raft-log.dat", "group-3/raft-log.dat"} {
		if _, err := os.Stat(filepath.Join(dataDir, name)); err != nil {
			t.Fatalf("%s missing after a start: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dataDir, "raft-log.dat")); !os.IsNotExist(err) {
		t.Fatalf("a raft log at the data dir root — groups are not separated")
	}
}

// This test checks that a start that fails partway leaves nothing behind.
// Group 2's log is corrupt, so StartNode fails AFTER opening group 1. When
// main exited on failure, the OS cleaned up; a library function must do it
// itself — every file it opened closed, and the listener it was handed
// released, so the same address can be used again.
func TestStartNode_FailedStartReleasesEverythingItOpened(t *testing.T) {
	dataDir := t.TempDir()
	startAndStop(t, dataDir, 3)
	// An entry at index 5 in a log with no snapshot: entries 1..4 are
	// missing, which OpenLog reports as corruption.
	corruptLog := filepath.Join(dataDir, "group-2", "raft-log.dat")
	if err := os.Remove(corruptLog); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	appendRawLogEntry(t, corruptLog, 1, 5, nil)

	listener := listenLocal(t)
	address := listener.Addr().String()
	filesBefore := openFileCount(t) // includes the listener, which StartNode must close
	goroutinesBefore := runtime.NumGoroutine()

	_, err := StartNode(singleNodeConfig(dataDir, 3), listener)
	if err == nil || !strings.Contains(err.Error(), "group-2") {
		t.Fatalf("StartNode with a corrupt group 2 log: err = %v, want a group 2 failure", err)
	}

	if filesAfter := openFileCount(t); filesAfter != filesBefore-1 {
		t.Fatalf("open files: %d before (with the listener), %d after; want %d — the failed start leaked files",
			filesBefore, filesAfter, filesBefore-1)
	}
	// Goroutines take a moment to exit; a leak never does.
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > goroutinesBefore && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if goroutinesAfter := runtime.NumGoroutine(); goroutinesAfter > goroutinesBefore {
		t.Fatalf("goroutines: %d before, %d after — the failed start left goroutines running", goroutinesBefore, goroutinesAfter)
	}
	relisten, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("the listener handed to the failed StartNode was not released: %v", err)
	}
	relisten.Close()
}

// This test checks the path that replaces main's os.Exit: a state machine
// that stops on an entry it cannot decode is reported on Fatal(), naming
// its group, so the caller can fail-stop. Group 1's log gets a committed
// entry whose command is not a valid Command.
func TestStartNode_ReportsAFailedStateMachineOnFatal(t *testing.T) {
	dataDir := t.TempDir()
	startAndStop(t, dataDir, 1) // leaves the leader's no-op at index 1
	appendRawLogEntry(t, filepath.Join(dataDir, "group-1", "raft-log.dat"), 1, 2, []byte{0xff, 0xff})

	nodeRuntime, err := StartNode(singleNodeConfig(dataDir, 1), listenLocal(t))
	if err != nil {
		t.Fatalf("StartNode: %v", err)
	}
	defer nodeRuntime.Stop()

	select {
	case fatal := <-nodeRuntime.Fatal():
		if !strings.Contains(fatal.Error(), "group 1") {
			t.Fatalf("Fatal() = %v, want it to name group 1", fatal)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("an undecodable committed entry was never reported on Fatal()")
	}
}

// This test checks that Stop is safe to call twice — after a single group
// was stopped on its own, the node's full shutdown stops it again.
func TestNodeRuntime_StopIsSafeToCallTwice(t *testing.T) {
	nodeRuntime, err := StartNode(singleNodeConfig(t.TempDir(), 3), listenLocal(t))
	if err != nil {
		t.Fatalf("StartNode: %v", err)
	}
	nodeRuntime.Groups[2].Raft.Stop() // one group stopped on its own first
	nodeRuntime.Stop()
	nodeRuntime.Stop()
}
