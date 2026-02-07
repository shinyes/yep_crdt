package crdt

import (
	"encoding/json"
	"errors"
	"sync"
)

// PNCounter (正负计数器)
// 同时支持增加和减少操作，是唯一的计数器类型。
type PNCounter struct {
	ID string           `json:"id"` // 计数器标识
	P  map[string]int64 `json:"p"`  // 正向计数（每个节点的增加量）
	N  map[string]int64 `json:"n"`  // 负向计数（每个节点的减少量）
	mu sync.RWMutex
}

// NewPNCounter 创建一个新的 PNCounter 实例。
func NewPNCounter(id string) *PNCounter {
	return &PNCounter{
		ID: id,
		P:  make(map[string]int64),
		N:  make(map[string]int64),
	}
}

// PNCounterOp 表示对 PNCounter 的操作。
type PNCounterOp struct {
	OriginID string `json:"origin_id"` // 操作来源节点 ID
	Amount   int64  `json:"amount"`    // 变化量（正数增加，负数减少）
	Ts       int64  `json:"ts"`        // 时间戳
}

func (op PNCounterOp) Origin() string   { return op.OriginID }
func (op PNCounterOp) Type() Type       { return TypeCounter }
func (op PNCounterOp) Timestamp() int64 { return op.Ts }

// Apply 应用操作到 PNCounter。
func (pn *PNCounter) Apply(op Op) error {
	pnOp, ok := op.(PNCounterOp)
	if !ok {
		return errors.New("PNCounter 的操作类型无效")
	}

	pn.mu.Lock()
	defer pn.mu.Unlock()

	originID := pnOp.OriginID
	if originID == "" {
		originID = pn.ID
	}

	if pnOp.Amount >= 0 {
		// 正向操作：增加 P 计数
		pn.P[originID] += pnOp.Amount
	} else {
		// 负向操作：增加 N 计数（存储绝对值）
		pn.N[originID] += -pnOp.Amount
	}

	return nil
}

// Value 返回当前计数器的值（所有 P 之和减去所有 N 之和）。
func (pn *PNCounter) Value() interface{} {
	pn.mu.RLock()
	defer pn.mu.RUnlock()

	var total int64
	for _, v := range pn.P {
		total += v
	}
	for _, v := range pn.N {
		total -= v
	}
	return total
}

// Type 返回 CRDT 类型。
func (pn *PNCounter) Type() Type { return TypeCounter }

// Bytes 返回序列化后的状态。
func (pn *PNCounter) Bytes() []byte {
	pn.mu.RLock()
	defer pn.mu.RUnlock()
	b, _ := json.Marshal(pn)
	return b
}

// PNCounterState 表示 PNCounter 的完整状态。
type PNCounterState struct {
	P map[string]int64 `json:"p"`
	N map[string]int64 `json:"n"`
}

// State 返回 PNCounter 的当前状态副本。
func (pn *PNCounter) State() PNCounterState {
	pn.mu.RLock()
	defer pn.mu.RUnlock()

	pCopy := make(map[string]int64, len(pn.P))
	for k, v := range pn.P {
		pCopy[k] = v
	}

	nCopy := make(map[string]int64, len(pn.N))
	for k, v := range pn.N {
		nCopy[k] = v
	}

	return PNCounterState{P: pCopy, N: nCopy}
}

// Merge 将另一个 PNCounter 的状态合并到当前实例。
// 使用 max 策略合并每个节点的计数。
func (pn *PNCounter) Merge(other PNCounterState) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	// 合并 P（正向计数）
	for k, v := range other.P {
		if v > pn.P[k] {
			pn.P[k] = v
		}
	}

	// 合并 N（负向计数）
	for k, v := range other.N {
		if v > pn.N[k] {
			pn.N[k] = v
		}
	}
}
