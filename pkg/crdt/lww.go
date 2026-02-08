package crdt

import (
	"encoding/binary"
	"fmt"
)

// LWWRegister 实现最后写入胜出 (Last-Write-Wins) 寄存器。
type LWWRegister struct {
	value     []byte
	timestamp int64
}

// NewLWWRegister 创建一个新的 LWWRegister。
func NewLWWRegister(val []byte, ts int64) *LWWRegister {
	return &LWWRegister{
		value:     val,
		timestamp: ts,
	}
}

func (r *LWWRegister) Type() Type {
	return TypeLWW
}

func (r *LWWRegister) Value() any {
	return r.value
}

// OpLWWSet 是在 LWWRegister 中设置值的操作。
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

	// LWW 逻辑：如果时间戳更大则更新
	if setOp.Timestamp > r.timestamp {
		r.value = setOp.Value
		r.timestamp = setOp.Timestamp
	}
	return nil
}

func (r *LWWRegister) Merge(other CRDT) error {
	o, ok := other.(*LWWRegister)
	if !ok {
		return fmt.Errorf("cannot merge %T into LWWRegister", other)
	}

	if o.timestamp > r.timestamp {
		r.value = o.value
		r.timestamp = o.timestamp
	}
	return nil
}

func (r *LWWRegister) GC(safeTimestamp int64) int {
	return 0
}

// Bytes 序列化 LWWRegister。
// 格式：时间戳 (8 字节) + 值
func (r *LWWRegister) Bytes() ([]byte, error) {
	buf := make([]byte, 8+len(r.value))
	binary.BigEndian.PutUint64(buf[0:8], uint64(r.timestamp))
	copy(buf[8:], r.value)
	return buf, nil
}

// FromBytesLWW 反序列化 LWWRegister。
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
