package wal

import "testing"

// This test checks the invariant that a PUT entry survives an
// encode/decode round trip unchanged — the basic correctness property
// everything else in this package depends on.
func TestEncodeDecode_RoundTrip_Put(t *testing.T) {
	original := &Entry{Op: OpPut, Key: "user:123", Value: "alice"}

	decoded, err := DecodeEntry(original.Encode())
	if err != nil {
		t.Fatalf("DecodeEntry: %v", err)
	}
	if decoded.Op != original.Op || decoded.Key != original.Key || decoded.Value != original.Value {
		t.Fatalf("round trip mismatch: got %+v, want %+v", decoded, original)
	}
}

// This test checks the same round-trip invariant for DELETE, whose Value
// is always empty.
func TestEncodeDecode_RoundTrip_Delete(t *testing.T) {
	original := &Entry{Op: OpDelete, Key: "user:123", Value: ""}

	decoded, err := DecodeEntry(original.Encode())
	if err != nil {
		t.Fatalf("DecodeEntry: %v", err)
	}
	if decoded.Op != OpDelete || decoded.Key != "user:123" || decoded.Value != "" {
		t.Fatalf("round trip mismatch: got %+v, want Op=OpDelete Key=user:123 Value=\"\"", decoded)
	}
}

// This test checks the invariant that length-prefixing (not a delimiter)
// correctly handles zero-length strings — an empty key or value must round
// trip cleanly, not be confused with "absent."
func TestEncodeDecode_RoundTrip_EmptyKeyAndValue(t *testing.T) {
	original := &Entry{Op: OpPut, Key: "", Value: ""}

	decoded, err := DecodeEntry(original.Encode())
	if err != nil {
		t.Fatalf("DecodeEntry: %v", err)
	}
	if decoded.Key != "" || decoded.Value != "" {
		t.Fatalf("round trip mismatch: got Key=%q Value=%q, want both empty", decoded.Key, decoded.Value)
	}
}

// This test checks the invariant that a payload too short to even contain
// an op byte and key-length field is rejected with an error, not a panic —
// DecodeEntry must never slice out of bounds on adversarial input.
func TestDecodeEntry_TooShortForHeader(t *testing.T) {
	_, err := DecodeEntry([]byte{0, 0, 0})
	if err == nil {
		t.Fatalf("DecodeEntry on a too-short payload: err = nil, want an error")
	}
}

// This test checks the invariant that a declared key length exceeding the
// actual remaining bytes is rejected with an error rather than panicking
// on an out-of-range slice.
func TestDecodeEntry_TruncatedKey(t *testing.T) {
	payload := []byte{byte(OpPut), 0, 0, 0, 100} // claims a 100-byte key, has 0

	_, err := DecodeEntry(payload)
	if err == nil {
		t.Fatalf("DecodeEntry with a truncated key: err = nil, want an error")
	}
}

// This test checks the same bounds-safety invariant for a truncated value.
func TestDecodeEntry_TruncatedValue(t *testing.T) {
	// op=Put, keyLen=1, key="x", then a valueLen claiming 50 bytes with none present.
	payload := []byte{byte(OpPut), 0, 0, 0, 1, 'x', 0, 0, 0, 50}

	_, err := DecodeEntry(payload)
	if err == nil {
		t.Fatalf("DecodeEntry with a truncated value: err = nil, want an error")
	}
}

// This test checks the invariant that unexplained trailing bytes after a
// fully-decoded entry are treated as an error — a well-formed Encode()
// output should never have leftover bytes, so trailing data signals
// corruption rather than something safe to ignore.
func TestDecodeEntry_TrailingGarbage(t *testing.T) {
	valid := (&Entry{Op: OpPut, Key: "x", Value: "1"}).Encode()
	withGarbage := append(valid, 0xFF, 0xFF, 0xFF)

	_, err := DecodeEntry(withGarbage)
	if err == nil {
		t.Fatalf("DecodeEntry with trailing garbage: err = nil, want an error")
	}
}

// This test checks the invariant that an unrecognized op byte is rejected
// rather than silently decoded as some arbitrary OpType value.
func TestDecodeEntry_UnknownOp(t *testing.T) {
	payload := []byte{99, 0, 0, 0, 0, 0, 0, 0, 0} // op=99 is not OpPut or OpDelete

	_, err := DecodeEntry(payload)
	if err == nil {
		t.Fatalf("DecodeEntry with an unknown op byte: err = nil, want an error")
	}
}
