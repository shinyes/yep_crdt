package crdt

import (
	"encoding/json"
	"fmt"
)

// PNCounter 实现正负计数器。
type PNCounter struct {
	ID  string           // 本地节点 (副本) 的 ID
	Inc map[string]int64 // 每个节点的增量映射
	Dec map[string]int64 // 每个节点的减量映射
}

// NewPNCounter 创建一个新的 PNCounter。
func NewPNCounter(id string) *PNCounter {
	return &PNCounter{
		ID:  id,
		Inc: make(map[string]int64),
		Dec: make(map[string]int64),
	}
}

func (c *PNCounter) Type() Type {
	return TypePNCounter
}

func (c *PNCounter) Value() interface{} {
	var total int64
	for _, v := range c.Inc {
		total += v
	}
	for _, v := range c.Dec {
		total -= v
	}
	return total
}

// OpPNCounterInc 增加计数器。
type OpPNCounterInc struct {
	Val int64
}

func (op OpPNCounterInc) Type() Type { return TypePNCounter }

func (c *PNCounter) Apply(op Op) error {
	switch o := op.(type) {
	case OpPNCounterInc:
		if o.Val >= 0 {
			c.Inc[c.ID] += o.Val
		} else {
			c.Dec[c.ID] += (-o.Val)
		}
	default:
		return ErrInvalidOp
	}
	return nil
}

func (c *PNCounter) Merge(other CRDT) error {
	o, ok := other.(*PNCounter)
	if !ok {
		return fmt.Errorf("cannot merge %T into PNCounter", other)
	}

	mergeMap(c.Inc, o.Inc)
	mergeMap(c.Dec, o.Dec)
	return nil
}

func mergeMap(dest, src map[string]int64) {
	for k, v := range src {
		if dest[k] < v {
			dest[k] = v
		}
	}
}

func (c *PNCounter) Bytes() ([]byte, error) {
	return json.Marshal(c)
}

func (c *PNCounter) GC(safeTimestamp int64) int {
	return 0
}

func FromBytesPNCounter(data []byte) (*PNCounter, error) {
	// 注意：我们通常不知道字节中的 ID，除非已存储。
	// 对于反序列化，ID 可能为空，或者我们需要传递它。
	// 对于简单的状态恢复，ID=空 是可以的，直到我们要写入。
	c := NewPNCounter("")
	if err := json.Unmarshal(data, c); err != nil {
		return nil, err
	}
	return c, nil
}
