// Package server implements the gRPC-facing KVStore service, translating
// wire requests into calls against the local store.Store, dedup.Table, and
// (since Milestone 4) wal.WAL.
//
// At this milestone there is no Raft yet: every node serves requests
// against its own local state only. No replication, no leader election.
// Milestone 7 is where writes get rerouted through a Raft log before being
// applied here.
package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/imPranav14/Distributed-KVstore/internal/dedup"
	"github.com/imPranav14/Distributed-KVstore/internal/store"
	"github.com/imPranav14/Distributed-KVstore/internal/wal"
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
	wal   *wal.WAL
}

// NewKVServer wires a KVServer to already-constructed dependencies. It does
// not create them itself — per Rule 2, wiring dependencies together is
// main.go's job, not this package's. w's WAL is expected to have already
// been replayed into s by the caller before this server starts accepting
// requests.
func NewKVServer(s *store.Store, d *dedup.Table, w *wal.WAL) *KVServer {
	return &KVServer{store: s, dedup: d, wal: w}
}

// Get reads a key directly from the local store. It does not go through
// dedup or the WAL: Get is read-only and side-effect-free, so replaying it
// any number of times is always safe — there is nothing to make durable.
func (s *KVServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	value, found := s.store.Get(req.Key)
	return &kvpb.GetResponse{Value: value, Found: found}, nil
}

// Put durably records the write to the WAL, THEN applies it to the store —
// write-ahead, not the other way around. If a crash happens between those
// two steps, the WAL entry is what survives; replay on restart is what
// rebuilds the in-memory store, so which one runs "first" here only matters
// for making sure the durable record exists before anything else depends
// on the write having happened.
//
// This is the first real failure mode this server has had: a WAL write can
// fail (disk full, I/O error). dedup.Execute's error return, added this
// milestone, is what makes that failure propagate correctly instead of
// being silently swallowed — and, just as importantly, ensures a failed
// write is NOT recorded as seen, so the client's retry can actually
// succeed once whatever was wrong with the disk is resolved.
func (s *KVServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	clientID := dedup.ClientID(req.ClientId)
	requestID := dedup.RequestID(req.RequestId)

	_, err := s.dedup.Execute(clientID, requestID, func() error {
		if err := s.wal.Append(&wal.Entry{Op: wal.OpPut, Key: req.Key, Value: req.Value}); err != nil {
			return fmt.Errorf("kv_server: failed to persist Put for key=%q: %w", req.Key, err)
		}
		s.store.Put(req.Key, req.Value)
		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "put failed: %v", err)
	}

	return &kvpb.PutResponse{}, nil
}

// Delete durably records the delete to the WAL, then applies it to the
// store — same write-ahead reasoning and failure handling as Put.
func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	clientID := dedup.ClientID(req.ClientId)
	requestID := dedup.RequestID(req.RequestId)

	_, err := s.dedup.Execute(clientID, requestID, func() error {
		if err := s.wal.Append(&wal.Entry{Op: wal.OpDelete, Key: req.Key}); err != nil {
			return fmt.Errorf("kv_server: failed to persist Delete for key=%q: %w", req.Key, err)
		}
		s.store.Delete(req.Key)
		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete failed: %v", err)
	}

	return &kvpb.DeleteResponse{}, nil
}
