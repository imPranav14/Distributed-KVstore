package ring

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

func mustNew(t *testing.T, groups ...GroupID) *Ring {
	t.Helper()
	r, err := New(groups)
	if err != nil {
		t.Fatalf("New(%v): %v", groups, err)
	}
	return r
}

// testKeys returns "user:0" .. "user:<n-1>" — the same keys the Python model
// in UNDERSTANDING.md used, so the numbers here can be compared with it.
func testKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("user:%d", i)
	}
	return keys
}

// This test checks the invariant that the ring is a pure function of the
// SET of groups. Every node and every client builds its own ring; if the
// order groups happen to be listed in changed ownership, two processes given
// the same configuration would route the same key to different groups.
func TestNew_OwnershipDependsOnlyOnTheSetOfGroups(t *testing.T) {
	inOrder := mustNew(t, 1, 2, 3)
	shuffled := mustNew(t, 3, 1, 2)

	for _, key := range testKeys(10_000) {
		if a, b := inOrder.Owner(key), shuffled.Owner(key); a != b {
			t.Fatalf("Owner(%q) = %d for groups [1 2 3] but %d for [3 1 2]", key, a, b)
		}
	}
}

// This test checks that a ring is never built from input that would make it
// wrong: no groups (Owner would have nothing to return), or a group listed
// twice (it would silently get double the points, and double the keys).
func TestNew_RejectsEmptyAndDuplicateGroups(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatalf("New(nil) succeeded, want an error")
	}
	if _, err := New([]GroupID{1, 2, 1}); err == nil {
		t.Fatalf("New([1 2 1]) succeeded, want an error for the duplicate group")
	}
}

// This test checks the ring is really circular, and pins the boundary rule:
// a position belongs to the first point AT OR AFTER it. A hash past the last
// point must wrap to the first point; without the wrap, every such key would
// index past the end of the slice.
func TestOwner_WrapsAroundAndOwnsItsOwnPosition(t *testing.T) {
	r := mustNew(t, 1, 2, 3)
	first, last := r.points[0], r.points[len(r.points)-1]

	if got := r.ownerOfPosition(math.MaxUint64); got != first.group {
		t.Fatalf("ownerOfPosition(max) = %d, want the first point's group %d (wrap-around)", got, first.group)
	}
	if got := r.ownerOfPosition(last.position + 1); got != first.group {
		t.Fatalf("one past the last point -> group %d, want %d (wrap-around)", got, first.group)
	}
	if got := r.ownerOfPosition(0); got != first.group {
		t.Fatalf("ownerOfPosition(0) = %d, want the first point's group %d", got, first.group)
	}

	// Probe a point whose NEXT point belongs to a different group. If the
	// two shared a group, "this point" and "the next point" would give the
	// same answer and the probe could not tell the boundary rules apart.
	for i := 0; i+1 < len(r.points); i++ {
		onPoint, next := r.points[i], r.points[i+1]
		if onPoint.group == next.group {
			continue
		}
		if got := r.ownerOfPosition(onPoint.position); got != onPoint.group {
			t.Fatalf("a position exactly on a point -> group %d, want that point's group %d (not the next point's)", got, onPoint.group)
		}
		return
	}
	t.Fatalf("setup: every point is followed by a point of the same group")
}

// This test checks the tie-break that makes the sort a total order. Two
// points colliding on the same position must end up in the same order no
// matter which order they arrived in — otherwise two processes listing the
// groups differently would disagree about who owns that arc. Real SHA-256
// positions never collide among a few hundred points, so the collision is
// built by hand.
func TestSortPoints_CollidingPositionsOrderedByGroup(t *testing.T) {
	forward := []point{{position: 5, group: 1}, {position: 5, group: 2}}
	backward := []point{{position: 5, group: 2}, {position: 5, group: 1}}
	sortPoints(forward)
	sortPoints(backward)

	if forward[0] != backward[0] || forward[0].group != 1 {
		t.Fatalf("colliding points sorted to %v from one input order and %v from the other, want group 1 first in both", forward, backward)
	}
}

// This test checks that virtual nodes spread keys evenly — within 5
// percentage points of a third each. With one point per group the same keys
// split 3/57/40 in the Step 2 simulation.
//
// It also cross-checks the implementation against the independent Python
// model: if the hash, the point labels, the byte order and the boundary rule
// all match, the counts are not just close but IDENTICAL.
func TestOwner_SpreadsKeysEvenly(t *testing.T) {
	r := mustNew(t, 1, 2, 3)
	counts := map[GroupID]int{}
	keys := testKeys(100_000)
	for _, key := range keys {
		counts[r.Owner(key)]++
	}

	for _, group := range []GroupID{1, 2, 3} {
		share := float64(counts[group]) / float64(len(keys))
		if share < 1.0/3-0.05 || share > 1.0/3+0.05 {
			t.Fatalf("group %d owns %.1f%% of keys, want 33.3%% ± 5", group, 100*share)
		}
	}

	pythonModel := map[GroupID]int{1: 33617, 2: 31887, 3: 34496}
	for group, want := range pythonModel {
		if counts[group] != want {
			t.Fatalf("group %d owns %d keys, the Python model says %d — the implementation differs from the model", group, counts[group], want)
		}
	}
}

// This test checks the property consistent hashing exists for: adding a
// group moves only the keys the new group takes over. Ideally 1/4 of them
// move, and NONE move between groups that already existed — so a resize
// would copy data to the new group only, never reshuffle the old ones.
func TestAddingAGroup_MovesKeysOnlyToTheNewGroup(t *testing.T) {
	before := mustNew(t, 1, 2, 3)
	after := mustNew(t, 1, 2, 3, 4)

	keys := testKeys(100_000)
	moved := 0
	for _, key := range keys {
		oldOwner, newOwner := before.Owner(key), after.Owner(key)
		if oldOwner == newOwner {
			continue
		}
		if newOwner != 4 {
			t.Fatalf("%q moved from group %d to group %d — between two OLD groups", key, oldOwner, newOwner)
		}
		moved++
	}

	fraction := float64(moved) / float64(len(keys))
	if fraction < 0.15 || fraction > 0.35 {
		t.Fatalf("%.1f%% of keys moved when adding a 4th group, want about 25%%", 100*fraction)
	}
	t.Logf("adding group 4 moved %.1f%% of keys, all of them to group 4", 100*fraction)
}

// This test checks the ALTERNATIVE we rejected, so the comparison in
// UNDERSTANDING.md stays reproducible: with plain modulo, going from 3 to 4
// groups moves about three quarters of all keys, and a large share of them
// move between groups that already existed.
func TestModuloBaseline_MovesMostKeysIncludingBetweenOldGroups(t *testing.T) {
	keys := testKeys(100_000)
	moved, betweenOldGroups := 0, 0
	for _, key := range keys {
		position := hash(key)
		oldOwner, newOwner := position%3, position%4
		if oldOwner == newOwner {
			continue
		}
		moved++
		if newOwner != 3 { // 3 is the new group's index under %4
			betweenOldGroups++
		}
	}

	if fraction := float64(moved) / float64(len(keys)); fraction < 0.6 {
		t.Fatalf("modulo moved only %.1f%% of keys, expected about 75%%", 100*fraction)
	}
	if fraction := float64(betweenOldGroups) / float64(len(keys)); fraction < 0.3 {
		t.Fatalf("modulo moved only %.1f%% of keys between old groups, expected about 50%%", 100*fraction)
	}
	t.Logf("modulo: %.1f%% of keys moved, %.1f%% between old groups",
		100*float64(moved)/float64(len(keys)), 100*float64(betweenOldGroups)/float64(len(keys)))
}

// This test checks the ring's compatibility contract. The expected values
// come from the independent Python model, not from this code's own output.
// Changing the hash, the point labels, the byte order or PointsPerGroup
// makes it fail — correctly, because in a running cluster any of those
// changes would send existing keys to a different group, and the data
// already stored would appear to have vanished.
func TestOwner_LayoutIsPinned(t *testing.T) {
	if got := hash("group-1#0"); got != 17076555393130616753 {
		t.Fatalf(`hash("group-1#0") = %d, want 17076555393130616753`, got)
	}

	r := mustNew(t, 1, 2, 3)
	pinned := map[string]GroupID{
		"user:1": 3, "user:2": 1, "user:3": 2, "user:42": 1, "user:1000": 2,
		"x": 3, "y": 3, "z": 1, "": 2, "alice": 3, "bob": 2, "order:2026-10-17": 1,
	}
	for key, want := range pinned {
		if got := r.Owner(key); got != want {
			t.Fatalf("Owner(%q) = %d, want %d — the ring layout changed", key, got, want)
		}
	}
}

// This test checks the INVARIANT that a Ring needs no mutex because nothing
// writes to it after New. Under -race it fails the moment someone adds
// hidden mutation — a lazily built index, a lookup cache — without a lock.
func TestOwner_SafeForConcurrentUse(t *testing.T) {
	r := mustNew(t, 1, 2, 3)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		// Exits after its fixed loop of lookups; nothing blocks (Rule 5).
		go func() {
			defer wg.Done()
			for _, key := range testKeys(1_000) {
				r.Owner(key)
			}
		}()
	}
	wg.Wait()
}
