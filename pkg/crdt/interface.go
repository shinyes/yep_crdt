package crdt

import "errors"

// Type 标识 CRDT 的类型。
type Type byte

const (
	TypeLWW       Type = 0x01
	TypeORSet     Type = 0x02
	TypePNCounter Type = 0x03
	TypeRGA       Type = 0x04
	TypeMap       Type = 0x05
)

var (
	ErrInvalidOp = errors.New("此 CRDT 类型的操作无效")
)

// CRDT 是所有 CRDT 实现的通用接口。
type CRDT interface {
	// Type 返回 CRDT 的类型。
	Type() Type

	// Value 返回 CRDT 的面向用户的值。
	Value() interface{}

	// Apply 将本地生成的操作应用于 CRDT。
	// 它应该更新状态并返回增量（如果适用）或错误。
	Apply(op Op) error

	// Merge 将另一个 CRDT 状态合并到此状态中。
	// 另一个状态通常是从字节反序列化的。
	Merge(other CRDT) error

	// GC 执行垃圾回收。
	// safeTimestamp 是指所有节点都已确认看到的最小时间戳（因果稳定时间）。
	// 返回被移除的（物理删除的）元素数量。
	GC(safeTimestamp int64) int

	// Bytes 将 CRDT 状态序列化为字节。
	Bytes() ([]byte, error)
}

// Op 代表对 CRDT 的通用操作。
// 理想情况下，这应该是特定于类型的结构或接口。
// 为了简单起见，我们暂时使用通用结构，或者我们可以使用特定的 Op 类型。
// 为了保持接口简单，我们将使用 interface{} 或特定的 Op 接口。
type Op interface {
	Type() Type
}
