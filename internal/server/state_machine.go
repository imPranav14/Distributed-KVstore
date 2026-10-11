package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
)

// encodeCommand serializes a KV command into the bytes that go into a Raft
// log entry.
//
// INVARIANT: never produces empty bytes. An empty command is how Raft
// represents a no-op, and proto3 omits default-valued fields — so a Command
// with Op=UNSPECIFIED and every other field empty would marshal to zero
// bytes and be indistinguishable from a no-op. Rejecting UNSPECIFIED here
// is what keeps the two impossible to confuse.
func encodeCommand(cmd *kvpb.Command) ([]byte, error) {
	if cmd.Op == kvpb.Command_OP_UNSPECIFIED {
		return nil, fmt.Errorf("server: refusing to encode command for key=%q with unspecified op", cmd.Key)
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("server: failed to encode command for key=%q: %w", cmd.Key, err)
	}
	return data, nil
}

// decodeCommand parses bytes from a committed log entry back into a command.
// Callers must have already handled the empty (no-op) case.
func decodeCommand(data []byte) (*kvpb.Command, error) {
	cmd := &kvpb.Command{}
	if err := proto.Unmarshal(data, cmd); err != nil {
		return nil, fmt.Errorf("server: failed to decode command (%d bytes): %w", len(data), err)
	}
	if cmd.Op == kvpb.Command_OP_UNSPECIFIED {
		return nil, fmt.Errorf("server: decoded command for key=%q has unspecified op", cmd.Key)
	}
	return cmd, nil
}

// StateMachine is the replicated KV state: the store AND the dedup table,
// driven only by committed Raft entries.
//
// Dedup lives here, inside the replicated state, rather than in the gRPC
// handler where it sat through Milestone 6. A handler-level table only
// exists in one leader's memory: a client whose write committed on leader
// L1 just before L1 crashed retries against L2, L2 has never seen the
// request, and the write enters the log twice. Here, every replica applies
// the same log in the same order and so builds the SAME dedup table — the
// second copy is skipped identically everywhere. It also closes Milestone
// 4's open question ("dedup state does not survive a crash"): replaying the
// log on restart rebuilds the dedup table along with the data.
type StateMachine struct {
	store *store.Store
	dedup *dedup.Table

	// snapshotter is the Raft node, through the narrow interface the state
	// machine needs. nil means "never snapshot" (most tests). Read only by
	// the Run goroutine; set once at construction.
	snapshotter       Snapshotter
	snapshotThreshold int64 // take a snapshot once the Raft log exceeds this many bytes

	// mu protects lastApplied and appliedCh. Both are written only by the
	// Run goroutine; read by gRPC handler goroutines waiting in
	// WaitForApplied for their entry to be applied.
	mu          sync.Mutex
	lastApplied uint64

	// appliedCh is a broadcast: closed (waking every waiter at once) and
	// replaced with a fresh channel on every apply. Used instead of
	// sync.Cond because a Cond cannot be selected against ctx.Done().
	appliedCh chan struct{}

	// stopped is closed when Run returns. After that nothing will ever be
	// applied again, so waiters must give up immediately rather than sit
	// out their full timeout — otherwise every in-flight Put would stall
	// a graceful shutdown for up to applyWaitTimeout.
	stopped chan struct{}
}

// Snapshotter is what the state machine needs from Raft to take snapshots.
// raft.Node implements it; the interface keeps this package testable
// without a running node.
type Snapshotter interface {
	LogSize() int64
	Snapshot(index uint64, data []byte) error
}

// NewStateMachine wraps an EMPTY store and dedup table. Its state is built
// by applying what Raft delivers: a snapshot first if one exists, then the
// committed log after it.
//
// When the Raft log grows past thresholdBytes, the state machine snapshots
// itself (Milestone 8, Choice 1A). Pass a nil snapshotter to disable that.
func NewStateMachine(s *store.Store, d *dedup.Table, snapshotter Snapshotter, thresholdBytes int64) *StateMachine {
	return &StateMachine{
		store:             s,
		dedup:             d,
		snapshotter:       snapshotter,
		snapshotThreshold: thresholdBytes,
		appliedCh:         make(chan struct{}),
		stopped:           make(chan struct{}),
	}
}

// errStateMachineStopped is returned by WaitForApplied once Run has exited.
var errStateMachineStopped = errors.New("server: state machine stopped")

// WaitForApplied blocks until the entry at index has been applied, the state
// machine stops, or ctx ends.
//
// INVARIANT: the "already applied?" check and the read of appliedCh happen
// under ONE lock acquisition. Split them across two, and the entry can be
// applied in the gap: the waiter would then sleep on a channel that has
// already been closed and replaced, missing the wakeup entirely, and return
// only when its timeout fired — the classic lost-wakeup bug.
func (sm *StateMachine) WaitForApplied(ctx context.Context, index uint64) error {
	for {
		sm.mu.Lock()
		if sm.lastApplied >= index {
			sm.mu.Unlock()
			return nil
		}
		wakeup := sm.appliedCh
		sm.mu.Unlock()

		select {
		case <-wakeup:
			// Something was applied — loop and re-check, since it may not
			// have been our index yet.
		case <-sm.stopped:
			return errStateMachineStopped
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Get reads a key from the replicated store. Safe to call concurrently with
// Run: store.Store has its own lock.
func (sm *StateMachine) Get(key string) (value string, found bool) {
	return sm.store.Get(key)
}

// Run applies every message from applyCh until the channel is closed, which
// raft.Node does when it stops. Exit (Rule 5): applyCh closing.
//
// It is the ONLY goroutine that ever modifies the store or the dedup table.
// That single-writer property is what makes application deterministic:
// there is no second goroutine to interleave with, so every replica applies
// identical commands in identical order.
//
// Returns the first apply error. Callers must treat that as fatal for this
// node — see apply for why continuing is unsafe.
func (sm *StateMachine) Run(applyCh <-chan raft.ApplyMsg) error {
	// Release every WaitForApplied caller on ANY exit — clean shutdown or
	// fail-stop — since nothing will be applied after this returns.
	defer close(sm.stopped)

	for msg := range applyCh {
		if err := sm.apply(msg); err != nil {
			return err
		}
		sm.maybeSnapshot()
	}
	return nil
}

// maybeSnapshot takes a snapshot if the Raft log has grown past the
// threshold.
//
// INVARIANT: runs only on the Run goroutine, between two applies. That is
// what makes the snapshot exactly "the state after lastApplied": the Run
// goroutine is the only writer, so nothing can change the store or the dedup
// table between reading lastApplied and encoding them.
func (sm *StateMachine) maybeSnapshot() {
	if sm.snapshotter == nil || sm.snapshotter.LogSize() <= sm.snapshotThreshold {
		return
	}

	index := sm.LastApplied()
	data, err := sm.encodeSnapshot()
	if err != nil {
		slog.Error("server: failed to encode snapshot", "index", index, "error", err)
		return
	}

	// A failure here costs disk space, not correctness: the log still holds
	// every entry the snapshot would have replaced. So it is logged and
	// retried after the next apply rather than treated as fatal.
	if err := sm.snapshotter.Snapshot(index, data); err != nil {
		slog.Error("server: failed to take snapshot", "index", index, "error", err)
	}
}

// encodeSnapshot serializes the KV map AND the dedup table.
//
// Deterministic marshalling: protobuf writes maps in random order by
// default, so the same state would produce different bytes on every call.
// Nothing compares snapshot bytes today, but making equal state encode to
// equal bytes costs nothing and makes snapshots testable byte for byte.
func (sm *StateMachine) encodeSnapshot() ([]byte, error) {
	lastSeen := sm.dedup.Snapshot()
	lastRequestByClient := make(map[string]uint64, len(lastSeen))
	for clientID, requestID := range lastSeen {
		lastRequestByClient[string(clientID)] = uint64(requestID)
	}

	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(&kvpb.KVSnapshot{
		Data:                sm.store.Snapshot(),
		LastRequestByClient: lastRequestByClient,
	})
	if err != nil {
		return nil, fmt.Errorf("server: failed to marshal snapshot: %w", err)
	}
	return data, nil
}

// apply executes one committed entry. Deterministic: the same sequence of
// messages produces the same store and dedup table on every replica.
func (sm *StateMachine) apply(msg raft.ApplyMsg) error {
	if msg.Snapshot != nil {
		return sm.restoreSnapshot(msg)
	}

	sm.mu.Lock()
	expected := sm.lastApplied + 1
	sm.mu.Unlock()

	// INVARIANT: entries arrive exactly once, in index order. Raft's
	// single applier guarantees it; this check turns a violation of that
	// guarantee into a loud error instead of silently applying a command
	// twice or skipping one — either of which would make this replica
	// diverge from the others without any visible symptom.
	if msg.Index != expected {
		return fmt.Errorf("server: state machine received index %d, want %d (out of order or duplicate delivery)", msg.Index, expected)
	}

	// A no-op changes no state, but STILL advances lastApplied. ReadIndex
	// waits until lastApplied reaches the read index, and right after an
	// election that index is the no-op's — skipping it here would make the
	// first read after every leader change wait forever.
	if len(msg.Command) == 0 {
		sm.markApplied(msg.Index)
		return nil
	}

	cmd, err := decodeCommand(msg.Command)
	if err != nil {
		// Fail-stop rather than skip. Log entries carry no checksum
		// (Milestone 4's open question), so an undecodable entry may be
		// corruption on THIS replica's disk alone. Skipping it would make
		// this replica's state silently differ from the others' — worse
		// than stopping, which matches CLAUDE.md's crash-stop failure model.
		return fmt.Errorf("server: cannot apply log index %d: %w", msg.Index, err)
	}

	clientID := dedup.ClientID(cmd.ClientId)
	requestID := dedup.RequestID(cmd.RequestId)
	if _, err := sm.dedup.Execute(clientID, requestID, func() error {
		switch cmd.Op {
		case kvpb.Command_OP_PUT:
			sm.store.Put(cmd.Key, cmd.Value)
		case kvpb.Command_OP_DELETE:
			sm.store.Delete(cmd.Key)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("server: dedup failed applying log index %d: %w", msg.Index, err)
	}

	sm.markApplied(msg.Index)
	return nil
}

// restoreSnapshot replaces the entire state with a delivered snapshot.
//
// INVARIANT: a snapshot only ever moves state FORWARD. One at or below
// lastApplied would roll the store back to an older state, undoing writes
// clients were told succeeded — so it is an error, not a no-op.
func (sm *StateMachine) restoreSnapshot(msg raft.ApplyMsg) error {
	if current := sm.LastApplied(); msg.Index <= current {
		return fmt.Errorf("server: refusing snapshot at index %d — already applied through %d", msg.Index, current)
	}

	var snapshot kvpb.KVSnapshot
	if err := proto.Unmarshal(msg.Snapshot.Data, &snapshot); err != nil {
		// Same fail-stop reasoning as an undecodable command.
		return fmt.Errorf("server: cannot decode snapshot at index %d: %w", msg.Index, err)
	}

	lastSeen := make(map[dedup.ClientID]dedup.RequestID, len(snapshot.LastRequestByClient))
	for clientID, requestID := range snapshot.LastRequestByClient {
		lastSeen[dedup.ClientID(clientID)] = dedup.RequestID(requestID)
	}

	sm.store.Restore(snapshot.Data)
	sm.dedup.Restore(lastSeen)
	sm.markApplied(msg.Index)
	return nil
}

func (sm *StateMachine) markApplied(index uint64) {
	sm.mu.Lock()
	sm.lastApplied = index
	// Wake every waiter, then arm a fresh channel for the next apply.
	close(sm.appliedCh)
	sm.appliedCh = make(chan struct{})
	sm.mu.Unlock()
}

// LastApplied returns the highest log index applied to the store.
func (sm *StateMachine) LastApplied() uint64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.lastApplied
}
