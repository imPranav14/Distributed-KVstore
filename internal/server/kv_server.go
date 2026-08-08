// Package server implements the gRPC-facing KVStore service, translating
// wire requests into calls against the local store.Store and dedup.Table.
//
// At this milestone there is no Raft yet: every node serves requests
// against its own local state only. No replication, no leader election, no
// durability beyond what's in memory. Milestone 7 is where writes get
// rerouted through a Raft log before being applied here.
package server

import (
	"context"

	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	kvpb "github.com/imPranav14/Distributed-KVstore/proto/kv"
)

// KVServer implements kvpb.KVStoreServer.
type KVServer struct {
	// UnimplementedKVStoreServer must be embedded (by value, per its own
	// doc comment) for forward compatibility: if a future version of
	// kv.proto adds a new RPC method, this embedded type supplies a default
	// (returns Unimplemented) so this file doesn't fail to compile the
	// moment the .proto grows — only intentionally-implemented methods
	// below override that default.
	kvpb.UnimplementedKVStoreServer

	store *store.Store
	dedup *dedup.Table
}

// NewKVServer wires a KVServer to an already-constructed store and dedup
// table. It does not create them itself — per Rule 2, wiring dependencies
// together is main.go's job, not this package's.
func NewKVServer(s *store.Store, d *dedup.Table) *KVServer {
	return &KVServer{store: s, dedup: d}
}

// Get reads a key directly from the local store. It does not go through
// dedup: Get is read-only and side-effect-free, so replaying it any number
// of times is always safe — there is nothing to protect against.
func (s *KVServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	value, found := s.store.Get(req.Key)
	return &kvpb.GetResponse{Value: value, Found: found}, nil
}

// Put applies a write through the dedup table, so a retried request is
// never reapplied. There is no error return here because neither
// dedup.Execute nor store.Put can fail — this will change once real
// failure modes exist (e.g. Milestone 5+ rejecting writes on a non-leader
// node).
func (s *KVServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	clientID := dedup.ClientID(req.ClientId)
	requestID := dedup.RequestID(req.RequestId)

	s.dedup.Execute(clientID, requestID, func() {
		s.store.Put(req.Key, req.Value)
	})

	return &kvpb.PutResponse{}, nil
}

// Delete removes a key through the dedup table, same reasoning as Put.
func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	clientID := dedup.ClientID(req.ClientId)
	requestID := dedup.RequestID(req.RequestId)

	s.dedup.Execute(clientID, requestID, func() {
		s.store.Delete(req.Key)
	})

	return &kvpb.DeleteResponse{}, nil
}
