package wal

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// This test checks the invariant that entries appended to a WAL, after
// closing and reopening the file, are read back in the same order with
// identical content — the basic durability-and-replay contract this whole
// package exists to provide.
func TestAppendReadAll_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	want := []*Entry{
		{Op: OpPut, Key: "a", Value: "1"},
		{Op: OpPut, Key: "b", Value: "2"},
		{Op: OpDelete, Key: "a"},
	}
	for _, e := range want {
		if err := w.Append(e); err != nil {
			t.Fatalf("Append(%+v): %v", e, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	got, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("ReadAll returned %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Op != want[i].Op || got[i].Key != want[i].Key || got[i].Value != want[i].Value {
			t.Fatalf("entry %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// This test checks the invariant that a freshly created, empty WAL file
// replays cleanly to zero entries with no error — the first-ever startup
// of a brand-new node.
func TestReadAll_EmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll on empty file: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("ReadAll on empty file returned %d entries, want 0", len(entries))
	}
}

// This test checks the invariant that a torn write from a simulated
// mid-payload crash is silently and safely truncated during replay — every
// COMPLETE entry before the tear is still returned, and no error occurs.
// This directly exercises CLAUDE.md's Milestone 4 break-it scenario ("a
// process killed mid-write") without needing to actually kill a process.
func TestReadAll_TruncatedFinalEntry_IsSafelyIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Append(&Entry{Op: OpPut, Key: "a", Value: "1"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-write of a second entry: a length prefix
	// claiming 100 bytes follow, but only 3 garbage bytes actually do —
	// exactly what a torn write looks like on disk.
	rawFile, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file for raw append: %v", err)
	}
	if _, err := rawFile.Write([]byte{0, 0, 0, 100, 0xAA, 0xBB, 0xCC}); err != nil {
		t.Fatalf("failed to write torn entry: %v", err)
	}
	if err := rawFile.Close(); err != nil {
		t.Fatalf("failed to close raw file: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	entries, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll with a torn trailing write: %v, want no error (safe truncation)", err)
	}
	if len(entries) != 1 {
		t.Fatalf("ReadAll with a torn trailing write returned %d entries, want 1 (only the complete one)", len(entries))
	}
	if entries[0].Key != "a" || entries[0].Value != "1" {
		t.Fatalf("surviving entry = %+v, want Key=a Value=1", entries[0])
	}
}

// This test checks the invariant that genuine corruption — where the
// OUTER length-prefix framing is internally consistent (exactly as many
// bytes are present as promised) but the INNER content doesn't decode — is
// reported as an error, not silently ignored like a safe truncation would
// be. This is the "not a safe truncation" branch in ReadAll.
func TestReadAll_CorruptEntry_ReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	// Payload: op=Put, keyLen=100 (claims 100 bytes), but only 2 bytes
	// actually follow within the payload. The OUTER length prefix (7)
	// correctly matches this payload's real byte count, so ReadAll's
	// io.ReadFull for the payload succeeds fully — the corruption is only
	// visible once DecodeEntry inspects the content.
	malformedPayload := []byte{byte(OpPut), 0, 0, 0, 100, 'x', 'y'}

	rawFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to create raw file: %v", err)
	}
	lengthPrefix := []byte{0, 0, 0, byte(len(malformedPayload))}
	if _, err := rawFile.Write(lengthPrefix); err != nil {
		t.Fatalf("failed to write length prefix: %v", err)
	}
	if _, err := rawFile.Write(malformedPayload); err != nil {
		t.Fatalf("failed to write malformed payload: %v", err)
	}
	if err := rawFile.Close(); err != nil {
		t.Fatalf("failed to close raw file: %v", err)
	}

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	_, err = w.ReadAll()
	if err == nil {
		t.Fatalf("ReadAll with a self-inconsistent (corrupt) entry: err = nil, want an error")
	}
}

// This test checks the invariant that mu actually protects concurrent
// Append calls: many goroutines appending at once must never corrupt the
// file's framing. It's the same style of proof as store_test.go's
// TestConcurrentAccess_NoRace, aimed at the same class of risk Rule 7 exists
// to guard against.
func TestAppend_ConcurrentCalls_NoRace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			_ = w.Append(&Entry{Op: OpPut, Key: "k", Value: "v"})
		}(i)
	}
	wg.Wait()

	entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after concurrent Appends: %v — framing was corrupted", err)
	}
	if len(entries) != goroutines {
		t.Fatalf("ReadAll after %d concurrent Appends returned %d entries, want %d", goroutines, len(entries), goroutines)
	}
}
