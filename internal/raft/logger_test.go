package raft

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	raftpb "github.com/imPranav14/Distributed-KVstore/proto/raft"
)

// This test checks that a node built WithLogger writes its log lines through
// that logger. Once a process runs one Raft group per shard, three groups'
// "becoming follower node_id=1 term=5" lines interleave in one output; only
// the group label the logger carries says which group each came from.
func TestWithLogger_LabelsTheNodesLogLines(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil)).With("group_id", 2)

	dir := t.TempDir()
	state, err := LoadState(filepath.Join(dir, "raft-state.dat"))
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	log, err := OpenLog(filepath.Join(dir, "raft-log.dat"), filepath.Join(dir, "snapshot.dat"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	node := NewNode(1, state, log, map[uint64]Peer{}, make(chan ApplyMsg, 1), WithLogger(logger))

	node.HandleAppendEntries(&raftpb.AppendEntriesRequest{Term: 5, LeaderId: 2}) // a newer term: "becoming follower"

	if !strings.Contains(output.String(), "becoming follower") || !strings.Contains(output.String(), "group_id=2") {
		t.Fatalf("log output %q, want a \"becoming follower\" line labelled group_id=2", output.String())
	}
}

// This test checks the default: a node built without WithLogger still logs,
// through slog's default logger, rather than panicking on a nil logger.
func TestNewNode_WithoutLoggerUsesTheDefault(t *testing.T) {
	node := newTestNode(t, 1)
	node.HandleAppendEntries(&raftpb.AppendEntriesRequest{Term: 5, LeaderId: 2})
	if node.logger == nil {
		t.Fatalf("node.logger is nil without WithLogger")
	}
}
