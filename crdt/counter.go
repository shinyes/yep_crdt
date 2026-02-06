package crdt

import (
	"encoding/json"
	"errors"
	"sync"
)

// GCounter (只增计数器)
type GCounter struct {
	ID     string           `json:"id"`
	Counts map[string]int64 `json:"counts"`
	mu     sync.RWMutex
}

func NewGCounter(id string) *GCounter {
	return &GCounter{
		ID:     id,
		Counts: make(map[string]int64),
	}
}

type GCounterOp struct {
	OriginID string
	Amount   int64
	Ts       int64
}

func (op GCounterOp) Origin() string   { return op.OriginID }
func (op GCounterOp) Type() Type       { return TypeCounter }
func (op GCounterOp) Timestamp() int64 { return op.Ts }

func (g *GCounter) Apply(op Op) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	gOp, ok := op.(GCounterOp)
	if !ok {
		return errors.New("GCounter 的操作类型无效")
	}

	if gOp.Amount < 0 {
		return errors.New("无法减少 GCounter")
	}

	// current := g.counts[gOp.OriginID] // 此逻辑中未直接使用
	// G-Counter 逻辑：我们添加来自 Op 的增量。
	// 但在基于状态的合并中，我们取最大值。
	// 在基于操作的合并中，我们通常只是相加。
	// 假设使用基于操作的传输，但使用状态持久化。
	// 如果操作是 "增加"，我们增加本地副本的计数。
	// 如果操作来自另一个副本，它必须告诉我们该副本的新总量或增量。
	// 标准的基于状态的 G-Counter：状态是 map[string]int。合并是 max(local[k], remote[k])。

	// 通常 Payload 使用基于状态的逻辑，但这里的 Apply 接受一个 Op。
	// 如果 Op 是一个 "命令"（例如 Inc），我们更新状态。
	// 如果 Op 是来自另一个节点的 "下游操作"，它依赖于同步机制。
	// 为简单起见，在本库中，我们将 Apply 视为应用来自本地或远程的变更。
	// 但是等等，通常在 CRDT 中：
	// 1. 准备/生成阶段：创建一个 Op。
	// 2. 效果/应用阶段：更新状态。

	// 在这里，我们假设 GCounterOp 携带了 "增加" 的意图。
	// 但通常 G-Counter 状态合并是幂等的。操作并不总是幂等的。
	// 如果我们使用基于状态的同步 (Deltas)，Apply(Op) 用于本地变更。
	// 我们需要一个用于同步的 Merge(State) 方法。稍后会显式或隐式添加到接口中。

	// 目前，将 Op 视为 "增加的负载"
	if gOp.OriginID == "" {
		gOp.OriginID = g.ID // 如果为空，默认为自身
	}
	g.Counts[gOp.OriginID] += gOp.Amount
	return nil
}

func (g *GCounter) Value() interface{} {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var total int64
	for _, v := range g.Counts {
		total += v
	}
	return total
}

func (g *GCounter) Type() Type { return TypeCounter }

func (g *GCounter) State() map[string]int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	copy := make(map[string]int64, len(g.Counts))
	for k, v := range g.Counts {
		copy[k] = v
	}
	return copy
}

func (g *GCounter) Bytes() []byte {
	g.mu.RLock()
	defer g.mu.RUnlock()
	// 使用 JSON 序列化导出的字段
	b, _ := json.Marshal(g)
	return b
}

func (g *GCounter) Merge(other map[string]int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k, v := range other {
		if v > g.Counts[k] {
			g.Counts[k] = v
		}
	}
}

// PNCounter (正负计数器)
type PNCounter struct {
	P *GCounter
	N *GCounter
}

func NewPNCounter(id string) *PNCounter {
	return &PNCounter{
		P: NewGCounter(id),
		N: NewGCounter(id),
	}
}

type PNCounterOp struct {
	OriginID string
	Amount   int64
	Ts       int64
}

func (op PNCounterOp) Origin() string   { return op.OriginID }
func (op PNCounterOp) Type() Type       { return TypePNCounter }
func (op PNCounterOp) Timestamp() int64 { return op.Ts }

func (pn *PNCounter) Apply(op Op) error {
	pnOp, ok := op.(PNCounterOp)
	if !ok {
		return errors.New("PNCounter 的操作类型无效")
	}

	if pnOp.OriginID == "" {
		// 这里我们没有存储 PNCounter 自己的 ID，通常 PNCounter 的 ID 与其内部 GCounter 的 ID 一致
		// 但我们最好确保 internal GCounter 有 ID。
		// 由于 pn.P 和 pn.N 已经初始化，我们假设它们有 ID。
		// 如果 Op 的 OriginID 为空，我们该用什么？
		// 应该使用 Op 传递进来的，或者如果 Op 是本地生成的。
		// Apply 逻辑中，我们只是传递给子 GCounter。
		pnOp.OriginID = pn.P.ID // Use P's ID as default
	}

	if pnOp.Amount >= 0 {
		return pn.P.Apply(GCounterOp{OriginID: pnOp.OriginID, Amount: pnOp.Amount, Ts: pnOp.Ts})
	} else {
		return pn.N.Apply(GCounterOp{OriginID: pnOp.OriginID, Amount: -pnOp.Amount, Ts: pnOp.Ts})
	}
}

func (pn *PNCounter) Value() interface{} {
	return pn.P.Value().(int64) - pn.N.Value().(int64)
}

func (pn *PNCounter) Type() Type { return TypePNCounter }

func (pn *PNCounter) Bytes() []byte {
	b, _ := json.Marshal(pn)
	return b
}

// PNCounterState 表示 PNCounter 的完整状态
type PNCounterState struct {
	P map[string]int64 `json:"p"`
	N map[string]int64 `json:"n"`
}

// State 返回 PNCounter 的当前状态副本
func (pn *PNCounter) State() PNCounterState {
	return PNCounterState{
		P: pn.P.State(),
		N: pn.N.State(),
	}
}

// Merge 将另一个 PNCounter 的状态合并到当前实例
func (pn *PNCounter) Merge(other PNCounterState) {
	pn.P.Merge(other.P)
	pn.N.Merge(other.N)
}
