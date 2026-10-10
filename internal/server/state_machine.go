package server

import (
	"context"
	"errors"
	"fmt"
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

// NewStateMachine wraps an EMPTY store and dedup table. Its state is built
// entirely by applying the committed log from index 1 — there is no other
// recovery path, which is what makes the KV WAL redundant.
func NewStateMachine(s *store.Store, d *dedup.Table) *StateMachine {
	return &StateMachine{
		store:     s,
		dedup:     d,
		appliedCh: make(chan struct{}),
		stopped:   make(chan struct{}),
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
	}
	return nil
}

// apply executes one committed entry. Deterministic: the same sequence of
// messages produces the same store and dedup table on every replica.
func (sm *StateMachine) apply(msg raft.ApplyMsg) error {
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
