package raft

import "fmt"

// signalApplierLocked wakes runApplier because commitIndex has advanced.
// Callers must hold mu. It never blocks: applyNotify has capacity 1, and if
// a wakeup is already pending, this one is redundant — runApplier always
// drains everything up to the CURRENT commitIndex, not one entry per signal.
func (n *Node) signalApplierLocked() {
	select {
	case n.applyNotify <- struct{}{}:
	default:
	}
}

// runApplier is the single goroutine that delivers committed entries to the
// state machine, strictly in index order. Exactly one exists per Node, which
// is what makes the ordering guarantee trivial: there is no second sender to
// interleave with.
//
// Exit (Rule 5): stopCh closing, checked both while idle and while blocked
// on a send. On exit it closes applyCh — this node is that channel's only
// sender — which is what ends the consumer's range loop.
func (n *Node) runApplier() {
	defer close(n.applyCh)

	for {
		select {
		case <-n.applyNotify:
		case <-n.stopCh:
			return
		}

		n.mu.Lock()
		var pending []ApplyMsg
		if snapshot := n.log.Snapshot(); n.lastApplied < snapshot.LastIncludedIndex {
			// The state machine is behind the snapshot — on restart it is
			// empty while the snapshot holds everything up to N. Deliver the
			// snapshot ALONE: the entries after it go out on the next pass,
			// once lastApplied has moved to N.
			//
			// INVARIANT: the snapshot travels through this same goroutine
			// and channel as ordinary entries. One sender means it can never
			// be overtaken by an older entry and then overwritten by it —
			// which is exactly what would happen if Raft restored the state
			// machine directly from another goroutine.
			pending = []ApplyMsg{{
				Index:    snapshot.LastIncludedIndex,
				Term:     snapshot.LastIncludedTerm,
				Snapshot: snapshot,
			}}
		} else if n.commitIndex > n.lastApplied {
			// lastApplied >= the snapshot boundary here, so the entries
			// after it are guaranteed to still be in the log.
			entries, _ := n.log.EntriesFrom(n.lastApplied + 1)
			for _, entry := range entries {
				if entry.Index > n.commitIndex {
					break
				}
				pending = append(pending, ApplyMsg{Index: entry.Index, Term: entry.Term, Command: entry.Command})
			}
		}
		n.mu.Unlock()

		// INVARIANT: entries are sent WITHOUT holding mu. Holding it here
		// would let a slow consumer block every RequestVote and
		// AppendEntries handler; the node would stop answering heartbeats
		// and trigger cluster-wide elections purely because the KV layer
		// was busy.
		//
		// That is only safe because the entries copied above cannot change
		// underneath us: everything at or below commitIndex is committed,
		// and a committed entry is never truncated — that is precisely
		// what "committed" means. Uncommitted entries were not copied.
		for _, msg := range pending {
			select {
			case n.applyCh <- msg:
			case <-n.stopCh:
				return
			}

			n.mu.Lock()
			n.lastApplied = msg.Index
			n.mu.Unlock()
		}

		// Delivering a snapshot leaves entries still to go, and new commits
		// may have arrived while we were sending. Re-arm rather than wait
		// for a wakeup that may never come.
		n.mu.Lock()
		if n.commitIndex > n.lastApplied || n.lastApplied < n.log.SnapshotIndex() {
			n.signalApplierLocked()
		}
		n.mu.Unlock()
	}
}

// TermAt returns the term of the log entry at index, and whether it exists.
// The KV layer uses it after an entry is applied to confirm the applied
// entry is the one it proposed: within a term there is one leader, which
// assigns each index exactly once, so (index, term) identifies an entry
// uniquely (the Log Matching Property). Stable for any applied index,
// because applied entries are committed and committed entries are never
// truncated.
func (n *Node) TermAt(index uint64) (term uint64, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log.TermAt(index)
}

// Snapshot folds the state machine's state through index into a snapshot
// and discards the log entries it covers. Called by the state machine, from
// its apply goroutine, once data captures the state after applying index.
//
// Holds mu while the snapshot is written: during that write this node
// answers no RPCs, and a leader sends no heartbeats. A snapshot slow enough
// to outlast an election timeout would cause an election. Acceptable at
// this project's scale; production systems persist snapshots in the
// background (see UNDERSTANDING.md, Milestone 8b open questions).
func (n *Node) Snapshot(index uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if index <= n.log.SnapshotIndex() {
		return nil // stale or repeated: an older snapshot can't replace a newer one
	}
	if index > n.commitIndex {
		return fmt.Errorf("raft: refusing to snapshot index %d beyond commitIndex %d — only applied state may be snapshotted", index, n.commitIndex)
	}
	term, ok := n.log.TermAt(index)
	if !ok {
		return fmt.Errorf("raft: cannot snapshot index %d: entry not in log", index)
	}
	return n.log.InstallSnapshot(&Snapshot{LastIncludedIndex: index, LastIncludedTerm: term, Data: data})
}

// LogSize returns the log file's current size in bytes — what the state
// machine compares against its snapshot threshold. Takes only the log's own
// lock, never mu.
func (n *Node) LogSize() int64 {
	return n.log.SizeBytes()
}

// LastApplied returns the highest index delivered to the state machine.
func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}
