package crdt

import (
	"errors"
	"fmt"
)

// Type 标识 CRDT 的类型。
type Type byte

const (
	TypeLWW       Type = 0x01
	TypeORSet     Type = 0x02
	TypePNCounter Type = 0x03
	TypeRGA       Type = 0x04
	TypeMap       Type = 0x05
	TypeLocalFile Type = 0x06
)

// 错误定义
var (
	ErrInvalidOp     = errors.New("此 CRDT 类型的操作无效")
	ErrInvalidData   = errors.New("无效的 CRDT 数据")
	ErrKeyNotFound   = errors.New("键不存在")
	ErrTypeMismatch  = errors.New("类型不匹配")
	ErrNilCRDT       = errors.New("CRDT 对象为 nil")
	ErrSerialization = errors.New("序列化失败")
	ErrDeserialization = errors.New("反序列化失败")
)

// InvalidOpError 表示操作类型不匹配的错误
type InvalidOpError struct {
	ExpectedOp string
	GotOp      string
	CRDTType   Type
}

func (e *InvalidOpError) Error() string {
	return fmt.Sprintf("操作类型不匹配: CRDT 类型 %d 期望 %s, 得到 %s", e.CRDTType, e.ExpectedOp, e.GotOp)
}

func (e *InvalidOpError) Unwrap() error {
	return ErrInvalidOp
}

// InvalidDataError 表示数据无效的错误
type InvalidDataError struct {
	CRDTType   Type
	Reason     string
	DataLength int
}

func (e *InvalidDataError) Error() string {
	msg := fmt.Sprintf("无效的 CRDT 数据: 类型 %d", e.CRDTType)
	if e.Reason != "" {
		msg += ", 原因: " + e.Reason
	}
	if e.DataLength >= 0 {
		msg += fmt.Sprintf(", 数据长度: %d", e.DataLength)
	}
	return msg
}

func (e *InvalidDataError) Unwrap() error {
	return ErrInvalidData
}

func NewInvalidDataError(t Type, reason string) *InvalidDataError {
	return &InvalidDataError{
		CRDTType:   t,
		Reason:     reason,
		DataLength: -1,
	}
}

// KeyErrorKeyNotFound 表示键不存在的错误
type KeyNotFoundError struct {
	Key string
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("键 '%s' 不存在", e.Key)
}

func (e *KeyNotFoundError) Unwrap() error {
	return ErrKeyNotFound
}

// TypeMismatchError 表示类型不匹配的错误
type TypeMismatchError struct {
	Key         string
	ExpectedType Type
	GotType     Type
}

func (e *TypeMismatchError) Error() string {
	return fmt.Sprintf("类型不匹配: 键 '%s' 期望类型 %d, 得到类型 %d", e.Key, e.ExpectedType, e.GotType)
}

func (e *TypeMismatchError) Unwrap() error {
	return ErrTypeMismatch
}

// CRDT 是所有 CRDT 实现的通用接口。
type CRDT interface {
	// Type 返回 CRDT 的类型。
	Type() Type

	// Value 返回 CRDT 的面向用户的值。
	Value() any

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
// 为了保持接口简单，我们将使用 any 或特定的 Op 接口。
type Op interface {
	Type() Type
}

// ReadOnlyRGA 定义了 RGA 的只读接口。
type ReadOnlyRGA[T any] interface {
	Value() any
	Iterator() func() (T, bool)
}

// ReadOnlySet 定义了 ORSet 的只读接口。
type ReadOnlySet[T comparable] interface {
	Value() any
	Contains(element T) bool
	Elements() []T
}

// ReadOnlyMap 定义了 MapCRDT 的只读接口。
type ReadOnlyMap interface {
	Value() any
	Get(key string) (any, bool)
	Has(key string) bool

	// 类型安全访问器
	GetString(key string) (string, bool)
	GetInt(key string) (int, bool)
	GetRGA(key string) (ReadOnlyRGA[any], error)
	GetRGAString(key string) (ReadOnlyRGA[string], error)
	GetRGABytes(key string) (ReadOnlyRGA[[]byte], error)
	GetSetString(key string) (ReadOnlySet[string], error)
	GetSetInt(key string) (ReadOnlySet[int], error)
	GetLocalFile(key string) (ReadOnlyLocalFile, error)
}

// ReadOnlyLocalFile 定义了 LocalFileCRDT 的只读接口。
type ReadOnlyLocalFile interface {
	Value() any
	ReadAll() ([]byte, error)
	ReadAt(offset int64, length int) ([]byte, error)
}
