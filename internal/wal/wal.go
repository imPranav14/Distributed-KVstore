// Package wal implements an append-only, fsync-durable write-ahead log.
// Every mutating store operation is recorded here, durably, before it is
// considered complete — so a crashed node can rebuild its exact in-memory
// state by replaying this file on restart.
package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// WAL wraps a single append-only log file.
type WAL struct {
	// mu serializes Append calls: kv_server.go's Put/Delete handlers can
	// call Append from multiple goroutines concurrently (concurrent gRPC
	// requests). Without this lock, two goroutines writing to the same
	// *os.File at once could interleave their byte writes and corrupt the
	// length-prefix framing itself — not just reorder entries, but corrupt
	// the file structure so that a later entry becomes unparseable.
	mu   sync.Mutex
	file *os.File
}

// Open opens (creating if necessary) the WAL file at path, ready to accept
// Append calls and, once, a ReadAll for startup replay.
func Open(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open %s: %w", path, err)
	}
	return &WAL{file: file}, nil
}

// Append durably records entry: writes [4-byte big-endian length][payload]
// and fsyncs before returning. A caller must treat the write as NOT durable
// until Append returns nil.
func (w *WAL) Append(entry *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	payload := entry.Encode()

	lengthPrefix := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthPrefix, uint32(len(payload)))

	if _, err := w.file.Write(lengthPrefix); err != nil {
		return fmt.Errorf("wal: failed to write length prefix for key=%q: %w", entry.Key, err)
	}
	if _, err := w.file.Write(payload); err != nil {
		return fmt.Errorf("wal: failed to write payload for key=%q: %w", entry.Key, err)
	}

	// The fsync: forces the OS to flush its page cache to physical storage
	// before we return. Without this, Write() returning successfully only
	// means the OS accepted the bytes into its buffer — a real crash
	// (power loss, kernel panic), as opposed to just this process dying,
	// could lose data that was "written" but never actually reached disk.
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: failed to fsync entry for key=%q: %w", entry.Key, err)
	}

	return nil
}

// ReadAll reads every complete entry from the start of the file, in order,
// for replay at startup.
//
// PRECONDITION: ReadAll must only be called before any concurrent Append
// begins (i.e. during startup replay, before the server starts accepting
// requests). It is deliberately not protected by mu — that lock exists to
// serialize concurrent Appends against each other, a scenario ReadAll is
// never exercised under by construction, not by accident.
func (w *WAL) ReadAll() ([]*Entry, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("wal: failed to seek to start for replay: %w", err)
	}

	var entries []*Entry
	lengthBuf := make([]byte, 4)

	for {
		if _, err := io.ReadFull(w.file, lengthBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Safe: either a clean end of the log (io.EOF, nothing more
				// was ever written here) or a crash that happened while
				// writing the length prefix itself (io.ErrUnexpectedEOF,
				// fewer than 4 bytes present). Either way, replay stops
				// here and everything read so far is still valid.
				break
			}
			return nil, fmt.Errorf("wal: failed to read length prefix during replay: %w", err)
		}
		length := binary.BigEndian.Uint32(lengthBuf)

		payload := make([]byte, length)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Safe: a crash happened after the length prefix was
				// flushed but before the payload finished writing — the
				// exact scenario CLAUDE.md's Milestone 4 break-it protocol
				// targets (a process killed mid-write).
				break
			}
			return nil, fmt.Errorf("wal: failed to read payload during replay: %w", err)
		}

		entry, err := DecodeEntry(payload)
		if err != nil {
			// NOT safe to ignore: the length-prefix framing succeeded (we
			// read exactly as many bytes as promised), so this isn't an
			// ordinary crash-truncation — the content itself is
			// inconsistent, which means real corruption (a bad disk
			// sector, a bug), not an expected partial write.
			return nil, fmt.Errorf("wal: corrupt entry during replay (not a safe truncation): %w", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: failed to close: %w", err)
	}
	return nil
}
