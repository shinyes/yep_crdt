package crdt

// Type 表示 CRDT 的类型。
type Type int

const (
	TypeCounter Type = iota
	TypeSet
	TypeRegister
	TypeSequence
	TypeMap
	TypeImmutableFile
	TypePNCounter
)

// Op 表示对 CRDT 的操作。
type Op interface {
	// Origin 返回生成此操作的副本 ID。
	Origin() string
	// Type 返回此操作适用的 CRDT 类型。
	Type() Type
	// Timestamp 返回逻辑执行时间（可选，如果不使用则为 0）
	Timestamp() int64
}

// State 表示 CRDT 的完整状态（用于基于状态的 CRDT 或快照）。
type State interface {
	// Bytes 返回序列化后的状态。
	Bytes() []byte
}

// CRDT 是此库中所有 CRDT 实现的通用接口。
type CRDT interface {
	// Apply 将下游操作应用于 CRDT 状态。
	Apply(op Op) error
	// Value 返回 CRDT 的当前值。
	Value() interface{}
	// Type 返回此 CRDT 的类型。
	Type() Type
}

// DeltaCRDT 扩展了 CRDT 以支持基于 Delta 的同步。
type DeltaCRDT interface {
	CRDT
	// Delta 返回自上次版本以来的状态变化（隐含）。
	// 在实际实现中，这可能接受一个版本向量作为输入。
	Delta() State
	// MergeDelta 将 Delta 合并到当前状态中。
	MergeDelta(delta State) error
}
