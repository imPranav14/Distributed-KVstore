package raft

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
		if n.commitIndex > n.lastApplied {
			for _, entry := range n.log.EntriesFrom(n.lastApplied + 1) {
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

// LastApplied returns the highest index delivered to the state machine.
func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}
