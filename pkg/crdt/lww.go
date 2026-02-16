package crdt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
)

// LWWRegister implements a Last-Write-Wins register.
type LWWRegister struct {
	mu        sync.RWMutex
	value     []byte
	timestamp int64
}

// NewLWWRegister creates a new LWWRegister.
func NewLWWRegister(val []byte, ts int64) *LWWRegister {
	return &LWWRegister{
		value:     cloneBytes(val),
		timestamp: ts,
	}
}

func (r *LWWRegister) Type() Type {
	return TypeLWW
}

func (r *LWWRegister) Value() any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.value
}

// OpLWWSet sets the value for LWWRegister.
type OpLWWSet struct {
	Value     []byte
	Timestamp int64
}

func (op OpLWWSet) Type() Type {
	return TypeLWW
}

func (r *LWWRegister) Apply(op Op) error {
	setOp, ok := op.(OpLWWSet)
	if !ok {
		return ErrInvalidOp
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Deterministic winner: timestamp first, then value bytes as tie-breaker.
	if compareLWWState(setOp.Timestamp, setOp.Value, r.timestamp, r.value) > 0 {
		r.value = cloneBytes(setOp.Value)
		r.timestamp = setOp.Timestamp
	}
	return nil
}

func (r *LWWRegister) Merge(other CRDT) error {
	o, ok := other.(*LWWRegister)
	if !ok {
		return fmt.Errorf("cannot merge %T into LWWRegister", other)
	}

	o.mu.RLock()
	defer o.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	if compareLWWState(o.timestamp, o.value, r.timestamp, r.value) > 0 {
		r.value = cloneBytes(o.value)
		r.timestamp = o.timestamp
	}
	return nil
}

func compareLWWState(tsA int64, valueA []byte, tsB int64, valueB []byte) int {
	if tsA > tsB {
		return 1
	}
	if tsA < tsB {
		return -1
	}
	// For equal timestamps, use a stable deterministic order on value bytes.
	// We pick the lexicographically smaller value as winner so every replica
	// resolves ties in the same direction.
	return bytes.Compare(valueB, valueA)
}

func cloneBytes(v []byte) []byte {
	if v == nil {
		return nil
	}
	c := make([]byte, len(v))
	copy(c, v)
	return c
}

func (r *LWWRegister) GC(safeTimestamp int64) int {
	return 0
}

// Bytes serializes LWWRegister.
// Format: timestamp (8 bytes) + value bytes.
func (r *LWWRegister) Bytes() ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	buf := make([]byte, 8+len(r.value))
	binary.BigEndian.PutUint64(buf[0:8], uint64(r.timestamp))
	copy(buf[8:], r.value)
	return buf, nil
}

// FromBytesLWW deserializes LWWRegister.
func FromBytesLWW(data []byte) (*LWWRegister, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("invalid data length for LWWRegister")
	}
	ts := int64(binary.BigEndian.Uint64(data[0:8]))
	val := make([]byte, len(data)-8)
	copy(val, data[8:])

	return &LWWRegister{
		value:     val,
		timestamp: ts,
	}, nil
}
