package server

import (
	"testing"
	"time"

	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
)

func newTestStateMachine() *StateMachine {
	return NewStateMachine(store.NewStore(), dedup.NewTable())
}

// cmdMsg builds an ApplyMsg carrying an encoded KV command.
func cmdMsg(t *testing.T, index uint64, op kvpb.Command_Op, key, value, clientID string, requestID uint64) raft.ApplyMsg {
	t.Helper()
	data, err := encodeCommand(&kvpb.Command{Op: op, Key: key, Value: value, ClientId: clientID, RequestId: requestID})
	if err != nil {
		t.Fatalf("encodeCommand: %v", err)
	}
	return raft.ApplyMsg{Index: index, Term: 1, Command: data}
}

func applyAll(t *testing.T, sm *StateMachine, msgs ...raft.ApplyMsg) {
	t.Helper()
	for _, m := range msgs {
		if err := sm.apply(m); err != nil {
			t.Fatalf("apply index %d: %v", m.Index, err)
		}
	}
}

func valueOf(sm *StateMachine, key string) (string, bool) {
	return sm.store.Get(key)
}

// This test checks the invariant that keeps a real command from ever being
// mistaken for a Raft no-op: an UNSPECIFIED op is refused at encode time,
// because proto3 would marshal it to zero bytes — the no-op's exact shape.
func TestEncodeCommand_RejectsUnspecifiedOp(t *testing.T) {
	if _, err := encodeCommand(&kvpb.Command{}); err == nil {
		t.Fatalf("encodeCommand with Op=UNSPECIFIED: err = nil, want an error")
	}
	// And the reason it matters: a zero-valued Command really does encode
	// to nothing at all.
	data, _ := encodeCommand(&kvpb.Command{Op: kvpb.Command_OP_PUT})
	if len(data) == 0 {
		t.Fatalf("a real PUT command encoded to zero bytes — it would be read back as a no-op")
	}
}

// This test checks that PUT and DELETE are applied in log order, and that
// lastApplied tracks the index of the last applied entry.
func TestStateMachine_AppliesPutAndDeleteInOrder(t *testing.T) {
	sm := newTestStateMachine()

	applyAll(t, sm,
		cmdMsg(t, 1, kvpb.Command_OP_PUT, "x", "1", "c1", 1),
		cmdMsg(t, 2, kvpb.Command_OP_PUT, "y", "2", "c1", 2),
		cmdMsg(t, 3, kvpb.Command_OP_DELETE, "x", "", "c1", 3),
	)

	if _, found := valueOf(sm, "x"); found {
		t.Fatalf("x still present after DELETE at index 3")
	}
	if v, found := valueOf(sm, "y"); !found || v != "2" {
		t.Fatalf("y = %q found=%v, want \"2\"", v, found)
	}
	if sm.LastApplied() != 3 {
		t.Fatalf("LastApplied() = %d, want 3", sm.LastApplied())
	}
}

// This test checks that a no-op changes no state but STILL advances
// lastApplied. ReadIndex waits for lastApplied to reach the read index,
// which right after an election is the no-op's — so a no-op that didn't
// count would stall the first read after every leader change.
func TestStateMachine_NoOpAdvancesLastAppliedWithoutChangingState(t *testing.T) {
	sm := newTestStateMachine()
	applyAll(t, sm, cmdMsg(t, 1, kvpb.Command_OP_PUT, "x", "1", "c1", 1))

	if err := sm.apply(raft.ApplyMsg{Index: 2, Term: 2, Command: nil}); err != nil {
		t.Fatalf("apply no-op: %v", err)
	}

	if sm.LastApplied() != 2 {
		t.Fatalf("LastApplied() = %d, want 2 — the no-op must count", sm.LastApplied())
	}
	if v, _ := valueOf(sm, "x"); v != "1" {
		t.Fatalf("x = %q after a no-op, want unchanged \"1\"", v)
	}
}

// This test checks the exactly-once, in-order invariant at the consumer:
// a gap or a repeated index is an error, never a silent skip or double
// apply — either of which would make this replica diverge invisibly.
func TestStateMachine_RejectsOutOfOrderOrDuplicateIndex(t *testing.T) {
	sm := newTestStateMachine()
	applyAll(t, sm, cmdMsg(t, 1, kvpb.Command_OP_PUT, "x", "1", "c1", 1))

	if err := sm.apply(cmdMsg(t, 1, kvpb.Command_OP_PUT, "x", "again", "c1", 2)); err == nil {
		t.Fatalf("re-delivering index 1: err = nil, want an error")
	}
	if err := sm.apply(cmdMsg(t, 3, kvpb.Command_OP_PUT, "x", "skip", "c1", 3)); err == nil {
		t.Fatalf("delivering index 3 after 1 (gap): err = nil, want an error")
	}
	if v, _ := valueOf(sm, "x"); v != "1" {
		t.Fatalf("x = %q, want \"1\" — a rejected delivery must not change state", v)
	}
}

// This test checks the scenario from the concept brief: a client's write
// commits on one leader, that leader dies before replying, and the client's
// retry is proposed again by the next leader — so the SAME request appears
// in the log twice, with another client's write in between. Dedup inside
// the state machine must skip the second copy; otherwise it overwrites the
// other client's newer value (Milestone 2's lost update, now reachable
// through a leader change).
func TestStateMachine_RetryLoggedTwiceAcrossLeaderChangeAppliesOnce(t *testing.T) {
	sm := newTestStateMachine()

	applyAll(t, sm,
		cmdMsg(t, 1, kvpb.Command_OP_PUT, "x", "a", "client-1", 7), // committed on L1, which then crashed
		cmdMsg(t, 2, kvpb.Command_OP_PUT, "x", "b", "client-2", 1), // another client, via L2
		cmdMsg(t, 3, kvpb.Command_OP_PUT, "x", "a", "client-1", 7), // client-1's retry, proposed again by L2
	)

	if v, _ := valueOf(sm, "x"); v != "b" {
		t.Fatalf("x = %q, want \"b\" — the retry was applied twice and erased client-2's newer write", v)
	}
	if sm.LastApplied() != 3 {
		t.Fatalf("LastApplied() = %d, want 3 — a skipped duplicate still occupies its index", sm.LastApplied())
	}
}

// This test checks determinism: two replicas fed the same committed log end
// in identical state — including the dedup table, which this proves is
// rebuilt by replay. That is what closes Milestone 4's "dedup does not
// survive a crash" gap: a restarted node is just a fresh replica replaying
// the same log.
func TestStateMachine_SameLogProducesSameStateOnEveryReplica(t *testing.T) {
	log := []raft.ApplyMsg{
		cmdMsg(t, 1, kvpb.Command_OP_PUT, "x", "1", "c1", 1),
		{Index: 2, Term: 2, Command: nil}, // a no-op from a leader change
		cmdMsg(t, 3, kvpb.Command_OP_PUT, "y", "2", "c2", 1),
		cmdMsg(t, 4, kvpb.Command_OP_PUT, "x", "1", "c1", 1), // duplicate
		cmdMsg(t, 5, kvpb.Command_OP_DELETE, "y", "", "c2", 2),
		cmdMsg(t, 6, kvpb.Command_OP_PUT, "z", "3", "c1", 2),
	}

	replicaA, replicaB := newTestStateMachine(), newTestStateMachine()
	applyAll(t, replicaA, log...)
	applyAll(t, replicaB, log...)

	for _, key := range []string{"x", "y", "z"} {
		va, fa := valueOf(replicaA, key)
		vb, fb := valueOf(replicaB, key)
		if va != vb || fa != fb {
			t.Fatalf("key %q: replica A = (%q, %v), replica B = (%q, %v) — replicas diverged", key, va, fa, vb, fb)
		}
	}

	// Both dedup tables must also agree: the next copy of (c1, 2) is a
	// duplicate on BOTH replicas.
	for name, replica := range map[string]*StateMachine{"A": replicaA, "B": replicaB} {
		applyAll(t, replica, cmdMsg(t, 7, kvpb.Command_OP_PUT, "z", "overwritten", "c1", 2))
		if v, _ := valueOf(replica, "z"); v != "3" {
			t.Fatalf("replica %s: z = %q, want \"3\" — dedup tables diverged", name, v)
		}
	}
}

// This test checks fail-stop on an undecodable entry: apply returns an error
// and does NOT advance lastApplied. With no per-entry checksum, garbage may
// be corruption on this replica alone; skipping it would silently diverge.
func TestStateMachine_UndecodableCommandFailsStop(t *testing.T) {
	sm := newTestStateMachine()

	err := sm.apply(raft.ApplyMsg{Index: 1, Term: 1, Command: []byte{0xFF, 0xFF, 0xFF}})

	if err == nil {
		t.Fatalf("apply of undecodable bytes: err = nil, want an error")
	}
	if sm.LastApplied() != 0 {
		t.Fatalf("LastApplied() = %d, want 0 — a failed entry must not count as applied", sm.LastApplied())
	}
}

// This test checks Run's exit condition (Rule 5): it applies everything
// sent, then returns nil once the channel is closed — which raft.Node does
// on Stop.
func TestStateMachine_RunAppliesUntilChannelClosed(t *testing.T) {
	sm := newTestStateMachine()
	applyCh := make(chan raft.ApplyMsg)
	done := make(chan error, 1)

	go func() { done <- sm.Run(applyCh) }()

	applyCh <- cmdMsg(t, 1, kvpb.Command_OP_PUT, "x", "1", "c1", 1)
	applyCh <- raft.ApplyMsg{Index: 2, Term: 2}
	close(applyCh)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil after a clean close", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Run did not return after applyCh was closed — goroutine leak")
	}
	if sm.LastApplied() != 2 {
		t.Fatalf("LastApplied() = %d, want 2", sm.LastApplied())
	}
}
