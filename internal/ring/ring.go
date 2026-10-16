// Package ring decides which shard group owns each key, using a consistent
// hash ring with virtual nodes.
//
// Every node and every client builds its own Ring from the same list of
// groups and must arrive at exactly the same answer for every key — there is
// no coordination at lookup time. That is why everything here is a pure,
// deterministic function: no randomness, no per-process seeds, no state that
// changes after construction.
package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

// PointsPerGroup is how many positions ("virtual nodes") each group occupies
// on the ring. With one point per group the arcs between points have random
// lengths, and in the Milestone 9 simulation one group got 57% of keys and
// another 3%. With 100 points each, three groups split 100k keys
// 33.6/31.9/34.5%.
//
// Part of the ring's compatibility contract, like an on-disk format:
// changing it changes which group owns most keys, so a running cluster would
// look for existing data in the wrong group.
const PointsPerGroup = 100

// GroupID names one Raft group, which holds one shard of the keyspace.
type GroupID uint64

// point is one of a group's positions on the ring.
type point struct {
	position uint64
	group    GroupID
}

// Ring maps keys to groups.
//
// INVARIANT: a Ring is immutable once New returns. Nothing writes to points
// afterwards, so any number of goroutines may call Owner concurrently with
// no mutex (Rule 7: there is no mutable field, so there is nothing to lock).
type Ring struct {
	points []point // sorted by (position, group)
}

// New builds the ring for a set of groups.
func New(groups []GroupID) (*Ring, error) {
	if len(groups) == 0 {
		return nil, fmt.Errorf("ring: New: cannot build a ring with no groups")
	}

	seen := make(map[GroupID]bool, len(groups))
	points := make([]point, 0, len(groups)*PointsPerGroup)
	for _, group := range groups {
		// A duplicate would silently give that group twice the points, and
		// so roughly twice the keys.
		if seen[group] {
			return nil, fmt.Errorf("ring: New: group %d is listed more than once", group)
		}
		seen[group] = true

		for i := 0; i < PointsPerGroup; i++ {
			label := fmt.Sprintf("group-%d#%d", group, i)
			points = append(points, point{position: hash(label), group: group})
		}
	}

	sortPoints(points)
	return &Ring{points: points}, nil
}

// sortPoints orders points by position, then by group.
//
// INVARIANT: the ring is a pure function of the SET of groups. The same set
// produces an identical ring on every process, whatever order the groups
// were listed in. Sorting removes the input order; the group-ID tie-break
// makes the sort a total order, so even two points colliding on the same
// 64-bit position are ordered the same way everywhere. Without it, which of
// two colliding groups owns that arc would depend on the input order —
// different on a process that listed the groups differently.
func sortPoints(points []point) {
	sort.Slice(points, func(a, b int) bool {
		if points[a].position != points[b].position {
			return points[a].position < points[b].position
		}
		return points[a].group < points[b].group
	})
}

// Owner returns the group that owns key.
func (r *Ring) Owner(key string) GroupID {
	return r.ownerOfPosition(hash(key))
}

// ownerOfPosition returns the group of the first point at or after position,
// walking clockwise. Split out from Owner so tests can probe exact positions,
// including the wrap-around past the last point.
func (r *Ring) ownerOfPosition(position uint64) GroupID {
	// Binary search for the first point whose position is >= the key's.
	i := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].position >= position
	})
	// Past the last point: the ring is circular, so the owner is the first
	// point. Without this, i == len(points) would index out of range.
	if i == len(r.points) {
		i = 0
	}
	return r.points[i].group
}

// hash places a string on the ring: the first 8 bytes of its SHA-256,
// read big-endian.
//
// It must give the same answer in every process, on every machine, in every
// Go version — which rules out hash/maphash, whose seed is random per
// process, so every node would build a different ring. FNV-1a was rejected
// on measurement: our point labels differ only in their last bytes, and
// FNV barely moves the high bits that decide position (a 6.5/28.5/65 split
// at 100 points per group). SHA-256 costs ~300ns per lookup, which is
// noise next to the milliseconds a write spends in fsync.
func hash(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}
