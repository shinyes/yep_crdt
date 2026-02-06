package crdt

import (
	"errors"
	"sync"
)

// LWWRegister (最后写入胜出寄存器)
type LWWRegister struct {
	value     interface{}
	timestamp int64
	mu        sync.RWMutex
}

func NewLWWRegister(initialVal interface{}, ts int64) *LWWRegister {
	return &LWWRegister{
		value:     initialVal,
		timestamp: ts,
	}
}

type LWWOp struct {
	OriginID string
	Value    interface{}
	Ts       int64
}

func (op LWWOp) Origin() string   { return op.OriginID }
func (op LWWOp) Type() Type       { return TypeRegister }
func (op LWWOp) Timestamp() int64 { return op.Ts }

func (r *LWWRegister) Apply(op Op) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	lwwOp, ok := op.(LWWOp)
	if !ok {
		return errors.New("LWWRegister 的操作无效")
	}

	// 最后写入胜出逻辑：较高的时间戳胜出。
	// 如果时间戳相等，可以使用特定于 OriginID 的规则（例如较大的 ID 胜出）来保证收敛。
	if lwwOp.Ts > r.timestamp {
		r.value = lwwOp.Value
		r.timestamp = lwwOp.Ts
	} else if lwwOp.Ts == r.timestamp {
		// 冲突解决（简单处理）
		// 理想情况下，我们也需要在寄存器中存储 OriginID 以进行比较。
		// 目前，我们只是简单地覆盖或保持。确定性需要稳定的逻辑。
		// 假设如果相等则接受它。
		r.value = lwwOp.Value
	}

	return nil
}

func (r *LWWRegister) Value() interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.value
}

func (r *LWWRegister) Type() Type { return TypeRegister }
