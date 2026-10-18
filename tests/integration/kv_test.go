// Package integration runs the whole system end to end: three nodes built
// by server.StartNode — the same wiring main.go runs — each hosting three
// shard groups, talking over real gRPC on localhost, driven by the real
// client.
//
// The nodes share one test process, which is what lets a test fail a single
// shard group on a single node. Everything between them is real network
// traffic: Raft RPCs, group IDs, snapshots, client retries.
package integration

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/imPranav14/Distributed-KVstore/internal/client"
	"github.com/imPranav14/Distributed-KVstore/internal/config"
	"github.com/imPranav14/Distributed-KVstore/internal/raft"
	"github.com/imPranav14/Distributed-KVstore/internal/ring"
	"github.com/imPranav14/Distributed-KVstore/internal/server"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
)

const (
	nodeCount   = 3
	shardGroups = 3
)

// groupCut is one node's switch for failing individual shard groups: its
// gRPC interceptor refuses every request — Raft or KV — that names a cut
// group. Stopping a group's Raft node is not enough to kill a replica,
// because its RPC handlers keep answering; cutting its traffic is.
type groupCut struct {
	mu     sync.Mutex
	groups map[uint64]bool
}

func (c *groupCut) cut(group ring.GroupID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groups[uint64(group)] = true
}

func (c *groupCut) isCut(group uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.groups[group]
}

// intercept works for every request type because each generated request
// message has a GetGroupId method.
func (c *groupCut) intercept(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if named, ok := req.(interface{ GetGroupId() uint64 }); ok && c.isCut(named.GetGroupId()) {
		return nil, status.Errorf(codes.Unavailable, "test: group %d is down on this node", named.GetGroupId())
	}
	return handler(ctx, req)
}

type testCluster struct {
	t         *testing.T
	addresses []string // fixed for the cluster's lifetime, so a node restarts on its own address
	configs   []*config.Config
	cuts      []*groupCut

	mu    sync.Mutex
	nodes []*server.NodeRuntime // nil while a node is down

	keyRing *ring.Ring
	client  *client.Client

	done     chan struct{}  // closed by cleanup: tells every Fatal watcher to exit
	watchers sync.WaitGroup // the Fatal watchers, waited for before nodes stop
}

func startCluster(t *testing.T, snapshotThresholdBytes int64) *testCluster {
	t.Helper()
	cluster := &testCluster{t: t, nodes: make([]*server.NodeRuntime, nodeCount), done: make(chan struct{})}

	listeners := make([]net.Listener, nodeCount)
	for i := range listeners {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Listen: %v", err)
		}
		listeners[i] = listener
		cluster.addresses = append(cluster.addresses, listener.Addr().String())
	}

	for i := 0; i < nodeCount; i++ {
		peers := map[uint64]string{}
		for j := 0; j < nodeCount; j++ {
			if j != i {
				peers[uint64(j+1)] = cluster.addresses[j]
			}
		}
		cluster.configs = append(cluster.configs, &config.Config{
			NodeID:                 i + 1,
			PeerAddrs:              peers,
			DataDir:                t.TempDir(),
			ShardGroups:            shardGroups,
			SnapshotThresholdBytes: snapshotThresholdBytes,
		})
		cluster.cuts = append(cluster.cuts, &groupCut{groups: map[uint64]bool{}})
	}

	// Registered before any node starts, so it runs after every node's
	// own cleanup would — and it stops whatever is still running.
	t.Cleanup(func() {
		close(cluster.done)
		cluster.watchers.Wait()
		cluster.mu.Lock()
		defer cluster.mu.Unlock()
		for _, node := range cluster.nodes {
			if node != nil {
				node.Stop()
			}
		}
	})

	for i := 0; i < nodeCount; i++ {
		cluster.start(i, listeners[i])
	}

	keyRing, err := ring.New(cluster.configs[0].Groups())
	if err != nil {
		t.Fatalf("ring.New: %v", err)
	}
	cluster.keyRing = keyRing
	cluster.client, err = client.NewClient(keyRing, cluster.addresses...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return cluster
}

// start runs node i on listener and watches it for a failed state machine.
func (c *testCluster) start(i int, listener net.Listener) {
	c.t.Helper()
	node, err := server.StartNode(c.configs[i], listener, grpc.UnaryInterceptor(c.cuts[i].intercept))
	if err != nil {
		c.t.Fatalf("StartNode(node %d): %v", i+1, err)
	}
	c.mu.Lock()
	c.nodes[i] = node
	c.mu.Unlock()

	c.watchers.Add(1)
	// Exit (Rule 5): on a fatal error, or when cleanup closes done.
	go func() {
		defer c.watchers.Done()
		select {
		case err := <-node.Fatal():
			c.t.Errorf("node %d: %v", i+1, err)
		case <-c.done:
		}
	}()
}

// stopNode shuts node i down completely, as a crash or `docker stop` would.
func (c *testCluster) stopNode(i int) {
	c.mu.Lock()
	node := c.nodes[i]
	c.nodes[i] = nil
	c.mu.Unlock()
	node.Stop()
}

// restartNode starts node i again from its own data directory, on its own
// address — the address every other node still dials.
func (c *testCluster) restartNode(i int) {
	c.t.Helper()
	var listener net.Listener
	var err error
	deadline := time.Now().Add(2 * time.Second)
	for {
		listener, err = net.Listen("tcp", c.addresses[i])
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond) // the old listener's port is still being released
	}
	if err != nil {
		c.t.Fatalf("re-listening on %s: %v", c.addresses[i], err)
	}
	c.start(i, listener)
}

func (c *testCluster) replica(i int, group ring.GroupID) server.GroupReplica {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i].Groups[group]
}

func (c *testCluster) groupDir(i int, group ring.GroupID) string {
	return filepath.Join(c.configs[i].DataDir, fmt.Sprintf("group-%d", group))
}

// keyInGroup returns a key the ring assigns to group.
func (c *testCluster) keyInGroup(group ring.GroupID) string {
	c.t.Helper()
	for i := 0; i < 1000; i++ {
		if key := fmt.Sprintf("key-%d", i); c.keyRing.Owner(key) == group {
			return key
		}
	}
	c.t.Fatalf("no key in group %d among 1000 candidates", group)
	return ""
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitForValue waits until node i's replica of key's group has applied
// key=value.
func (c *testCluster) waitForValue(i int, key, value string, timeout time.Duration) {
	c.t.Helper()
	group := c.keyRing.Owner(key)
	waitFor(c.t, timeout, func() bool {
		got, found := c.replica(i, group).StateMachine.Get(key)
		return found && got == value
	}, fmt.Sprintf("node %d, group %d to apply %s=%s", i+1, group, key, value))
}

func (c *testCluster) mustPut(key, value string) {
	c.t.Helper()
	if err := c.client.Put(context.Background(), key, value); err != nil {
		c.t.Fatalf("Put(%s=%s): %v", key, value, err)
	}
}

func (c *testCluster) mustGet(key, want string) {
	c.t.Helper()
	got, found, err := c.client.Get(context.Background(), key)
	if err != nil || !found || got != want {
		c.t.Fatalf("Get(%s) = %q, %v, %v; want %q", key, got, found, err, want)
	}
}

// This test checks the core sharding invariant end to end: through the real
// client and real gRPC, every key lands on all three replicas of the group
// the ring assigns it — and on no replica of any other group.
func TestSharded_KeysSpreadAcrossGroupsAndStayInTheirOwn(t *testing.T) {
	cluster := startCluster(t, config.DefaultSnapshotThresholdBytes)

	keysPerGroup := map[ring.GroupID]int{}
	for i := 0; i < 60; i++ {
		key := fmt.Sprintf("user:%d", i)
		cluster.mustPut(key, key)
		keysPerGroup[cluster.keyRing.Owner(key)]++
	}
	if len(keysPerGroup) != shardGroups {
		t.Fatalf("setup: 60 keys covered only groups %v", keysPerGroup)
	}

	for i := 0; i < 60; i++ {
		key := fmt.Sprintf("user:%d", i)
		cluster.mustGet(key, key)
		owner := cluster.keyRing.Owner(key)
		for node := 0; node < nodeCount; node++ {
			cluster.waitForValue(node, key, key, 5*time.Second)
			for _, group := range cluster.configs[node].Groups() {
				if _, found := cluster.replica(node, group).StateMachine.Get(key); found && group != owner {
					t.Fatalf("%s belongs to group %d but is on node %d's replica of group %d", key, owner, node+1, group)
				}
			}
		}
	}
}

// This test checks that a cluster survives a whole node dying — every group
// keeps two of its three replicas — and that the node, restarted from its
// own data directory on its own address, catches up in EVERY group. The
// restart is also what would expose groups sharing one directory: their
// interleaved logs refuse to open.
func TestSharded_SurvivesANodeFailureAndCatchesUpAfterRestart(t *testing.T) {
	cluster := startCluster(t, config.DefaultSnapshotThresholdBytes)
	for i := 0; i < 30; i++ {
		cluster.mustPut(fmt.Sprintf("user:%d", i), "v1")
	}

	cluster.stopNode(0)
	for i := 0; i < 30; i++ {
		cluster.mustGet(fmt.Sprintf("user:%d", i), "v1")
	}
	for i := 0; i < 30; i++ {
		cluster.mustPut(fmt.Sprintf("user:%d", i), "v2")
	}

	cluster.restartNode(0)
	restarted := time.Now()
	for i := 0; i < 30; i++ {
		cluster.waitForValue(0, fmt.Sprintf("user:%d", i), "v2", 30*time.Second)
	}
	t.Logf("restarted node caught up in every group in %v", time.Since(restarted).Round(time.Millisecond))
}

// This test checks InstallSnapshot end to end, over real gRPC and per
// group. While node 1 is down, both other nodes compact every group past
// the last entry node 1 has — so ordinary log entries can no longer catch
// it up, anywhere. If it catches up anyway, it can only have been through
// a snapshot, carried with the right group ID.
func TestSharded_RestartedNodeCatchesUpThroughSnapshotsInEveryGroup(t *testing.T) {
	cluster := startCluster(t, 2<<10) // 2 KiB: compaction after a few dozen writes per group
	for i := 0; i < 30; i++ {
		cluster.mustPut(fmt.Sprintf("user:%d", i), "v0")
	}

	cluster.stopNode(0)
	lastIndexWhileDown := map[ring.GroupID]uint64{}
	for _, group := range cluster.configs[0].Groups() {
		dir := cluster.groupDir(0, group)
		log, err := raft.OpenLog(filepath.Join(dir, "raft-log.dat"), filepath.Join(dir, "snapshot.dat"))
		if err != nil {
			t.Fatalf("reading node 1's group %d log while it is down: %v", group, err)
		}
		lastIndexWhileDown[group] = log.LastIndex()
		_ = log.Close()
	}

	for round := 1; round <= 10; round++ {
		for i := 0; i < 30; i++ {
			cluster.mustPut(fmt.Sprintf("user:%d", i), fmt.Sprintf("v%d", round))
		}
	}

	for _, group := range cluster.configs[0].Groups() {
		for _, node := range []int{1, 2} {
			snapshot, err := raft.LoadSnapshot(filepath.Join(cluster.groupDir(node, group), "snapshot.dat"))
			if err != nil {
				t.Fatalf("LoadSnapshot: %v", err)
			}
			if snapshot.LastIncludedIndex <= lastIndexWhileDown[group] {
				t.Fatalf("setup: node %d compacted group %d only through %d, but node 1 has entries through %d — "+
					"it could still catch up from log entries", node+1, group, snapshot.LastIncludedIndex, lastIndexWhileDown[group])
			}
		}
	}

	cluster.restartNode(0)
	restarted := time.Now()
	for i := 0; i < 30; i++ {
		cluster.waitForValue(0, fmt.Sprintf("user:%d", i), "v10", 30*time.Second)
	}
	t.Logf("restarted node caught up through snapshots in %v", time.Since(restarted).Round(time.Millisecond))

	for _, group := range cluster.configs[0].Groups() {
		snapshot, err := raft.LoadSnapshot(filepath.Join(cluster.groupDir(0, group), "snapshot.dat"))
		if err != nil || snapshot.LastIncludedIndex <= lastIndexWhileDown[group] {
			t.Fatalf("node 1's group %d snapshot is at %d (err %v), want beyond its old last index %d",
				group, snapshot.LastIncludedIndex, err, lastIndexWhileDown[group])
		}
	}
}

// This test checks that shard groups are independent: one group losing its
// majority takes down only its own keys.
//
// Group 2's two followers are killed — Raft node stopped AND traffic cut —
// leaving its leader X alone. Groups 1 and 3 keep serving. Through the
// client, group 2's reads and writes both end with ErrNoLeaderReachable
// once the client's budget runs out.
//
// Asked DIRECTLY, without the client's retries, X shows the missing
// CheckQuorum: it rejects a read at once, because ReadIndex must confirm
// leadership with a majority — but it accepts a write and holds it until
// the caller gives up, because nothing ever tells X it is no longer leader.
func TestSharded_OneGroupLosingItsMajorityLeavesTheOthersServing(t *testing.T) {
	cluster := startCluster(t, config.DefaultSnapshotThresholdBytes)
	for _, group := range cluster.configs[0].Groups() {
		cluster.mustPut(cluster.keyInGroup(group), "before")
	}

	var group2Leader int
	waitFor(t, 5*time.Second, func() bool {
		for node := 0; node < nodeCount; node++ {
			if cluster.replica(node, 2).Raft.Role() == raft.Leader {
				group2Leader = node
				return true
			}
		}
		return false
	}, "group 2 to have a leader")

	for node := 0; node < nodeCount; node++ {
		if node != group2Leader {
			cluster.replica(node, 2).Raft.Stop()
			cluster.cuts[node].cut(2)
		}
	}

	for _, group := range []ring.GroupID{1, 3} {
		key := cluster.keyInGroup(group)
		cluster.mustPut(key, "after")
		cluster.mustGet(key, "after")
	}

	// Directly at X, no client retries.
	group2Key := cluster.keyInGroup(2)
	connection, err := grpc.NewClient(cluster.addresses[group2Leader], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing node %d: %v", group2Leader+1, err)
	}
	defer connection.Close()
	isolatedLeader := kvpb.NewKVStoreClient(connection)

	readStarted := time.Now()
	_, readErr := isolatedLeader.Get(context.Background(), &kvpb.GetRequest{Key: group2Key, GroupId: 2})
	readTook := time.Since(readStarted)
	if status.Code(readErr) != codes.Unavailable || readTook > 500*time.Millisecond {
		t.Fatalf("read at the isolated leader: %v after %v, want Unavailable within 500ms (ReadIndex cannot confirm a majority)", readErr, readTook)
	}

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelWrite()
	writeStarted := time.Now()
	_, writeErr := isolatedLeader.Put(writeCtx, &kvpb.PutRequest{Key: group2Key, Value: "after", ClientId: "direct", RequestId: 1, GroupId: 2})
	writeTook := time.Since(writeStarted)
	if status.Code(writeErr) != codes.DeadlineExceeded || writeTook < 3*time.Second {
		t.Fatalf("write at the isolated leader: %v after %v, want it held until the caller's 3s deadline (no CheckQuorum)", writeErr, writeTook)
	}
	t.Logf("isolated group 2 leader: rejected a read in %v, held a write for %v until the caller gave up",
		readTook.Round(time.Millisecond), writeTook.Round(time.Millisecond))

	// Through the client: both end with a clear error, within its budget.
	for _, operation := range []struct {
		name string
		run  func() error
	}{
		{"read", func() error { _, _, err := cluster.client.Get(context.Background(), group2Key); return err }},
		{"write", func() error { return cluster.client.Put(context.Background(), group2Key, "after") }},
	} {
		started := time.Now()
		err := operation.run()
		if !errors.Is(err, client.ErrNoLeaderReachable) {
			t.Fatalf("group 2 %s through the client: err = %v, want ErrNoLeaderReachable", operation.name, err)
		}
		if took := time.Since(started); took > 7*time.Second {
			t.Fatalf("group 2 %s through the client took %v — longer than its 5s budget allows", operation.name, took)
		}
	}

	for _, group := range []ring.GroupID{1, 3} {
		cluster.mustGet(cluster.keyInGroup(group), "after")
	}
}
