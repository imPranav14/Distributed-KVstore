package wal

import (
	"encoding/binary"
	"fmt"
)

// OpType identifies which store operation a WAL entry represents.
type OpType byte

const (
	OpPut OpType = iota
	OpDelete
)

// Entry is one write-ahead log record: enough information to replay a
// single store.Put or store.Delete call after a crash.
type Entry struct {
	Op    OpType
	Key   string
	Value string // unused (empty) for OpDelete
}

// Encode serializes e as: [1-byte op][4-byte keyLen][key][4-byte valueLen][value].
//
// Length-prefixed rather than delimiter-based (e.g. null-terminated)
// because keys and values are arbitrary strings that could contain any
// byte, including whatever a delimiter would be — length-prefixing has no
// such ambiguity.
func (e *Entry) Encode() []byte {
	keyLen := len(e.Key)
	valueLen := len(e.Value)

	buf := make([]byte, 1+4+keyLen+4+valueLen)
	buf[0] = byte(e.Op)
	binary.BigEndian.PutUint32(buf[1:5], uint32(keyLen))
	copy(buf[5:5+keyLen], e.Key)
	binary.BigEndian.PutUint32(buf[5+keyLen:5+keyLen+4], uint32(valueLen))
	copy(buf[5+keyLen+4:], e.Value)

	return buf
}

// DecodeEntry parses the layout written by Encode back into an Entry.
//
// Every length is bounds-checked against what's actually left in payload
// before any slicing happens — payload may come from a corrupted or
// maliciously truncated file, so an out-of-range slice here must become an
// error, never a panic.
func DecodeEntry(payload []byte) (*Entry, error) {
	if len(payload) < 5 {
		return nil, fmt.Errorf("wal: payload too short (%d bytes) to contain op+keyLen", len(payload))
	}

	op := OpType(payload[0])
	if op != OpPut && op != OpDelete {
		return nil, fmt.Errorf("wal: unknown op byte %d", payload[0])
	}

	keyLen := binary.BigEndian.Uint32(payload[1:5])
	offset := 5
	if uint32(len(payload)-offset) < keyLen {
		return nil, fmt.Errorf("wal: payload declares key length %d but only %d bytes remain", keyLen, len(payload)-offset)
	}
	key := string(payload[offset : offset+int(keyLen)])
	offset += int(keyLen)

	if len(payload)-offset < 4 {
		return nil, fmt.Errorf("wal: payload too short (%d bytes remaining) to contain valueLen", len(payload)-offset)
	}
	valueLen := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4

	if uint32(len(payload)-offset) < valueLen {
		return nil, fmt.Errorf("wal: payload declares value length %d but only %d bytes remain", valueLen, len(payload)-offset)
	}
	value := string(payload[offset : offset+int(valueLen)])
	offset += int(valueLen)

	if offset != len(payload) {
		return nil, fmt.Errorf("wal: payload has %d unexplained trailing bytes after decoding", len(payload)-offset)
	}

	return &Entry{Op: op, Key: key, Value: value}, nil
}
