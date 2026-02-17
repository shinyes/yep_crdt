package crdt

import (
	"sync"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/hlc"
)

// RGA 实现复制可增长数组 (Replicated Growable Array)。
type RGA[T any] struct {
	mu       sync.RWMutex
	Vertices map[string]*RGAVertex[T]
	Head     string     // 虚拟头节点的 ID
	Clock    *hlc.Clock // 混合逻辑时钟

	// internal cache for tree structure: Origin -> List of Children
	edges map[string][]*RGAVertex[T]
}

type RGAVertex[T any] struct {
	ID        string
	Value     T
	Origin    string // ID of the node this was inserted after (Origin)
	Next      string // ID of the next vertex (derived/cached)
	Timestamp int64  // For conflict resolution
	Deleted   bool
	DeletedAt int64 // Time of deletion for GC
}

func NewRGA[T any](clock *hlc.Clock) *RGA[T] {
	// Head has zero value of T
	var zero T
	head := &RGAVertex[T]{
		ID:        uuid.NewString(),
		Value:     zero,
		Next:      "",
		Timestamp: 0,
		Deleted:   true,
	}
	return &RGA[T]{
		Vertices: map[string]*RGAVertex[T]{head.ID: head},
		Head:     head.ID,
		Clock:    clock,
		edges:    make(map[string][]*RGAVertex[T]),
	}
}

func (r *RGA[T]) Type() Type { return TypeRGA }

// deepCopyValue 尝试对值进行深拷贝，主要处理 []byte 类型
func deepCopyValue[T any](value T) T {
	// 尝试处理 []byte 类型
	if bytesVal, ok := any(value).([]byte); ok {
		copied := make([]byte, len(bytesVal))
		copy(copied, bytesVal)
		return any(copied).(T)
	}
	// 其他类型假设是不可变的或可以浅拷贝的
	return value
}

// Value 按顺序返回值的列表。
// 注意：对于大数据量，建议使用 Iterator() 以避免切片分配。
func (r *RGA[T]) Value() any {
	r.mu.RLock()
	defer r.mu.RUnlock()

	capHint := 0
	if len(r.Vertices) > 1 {
		capHint = len(r.Vertices) - 1
	}
	res := make([]T, 0, capHint)
	curr := r.Head
	for curr != "" {
		v := r.Vertices[curr]
		if !v.Deleted && v.ID != r.Head {
			res = append(res, v.Value)
		}
		curr = v.Next
	}
	return res
}

// Iterator 返回一个迭代器函数。
// 每次调用该函数，返回 (下一个值, true)。
// 如果遍历结束，返回 (零值, false)。
// 这种模式避免了在这里分配整个切片。
// 注意：迭代器创建时会创建快照，迭代期间不需要持有锁。
func (r *RGA[T]) Iterator() func() (T, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	capHint := 0
	if len(r.Vertices) > 1 {
		capHint = len(r.Vertices) - 1
	}

	// 创建顶点指针快照：相比复制 T 值，通常能减少分配和拷贝开销。
	currID := r.Head
	snapshot := make([]*RGAVertex[T], 0, capHint)
	for currID != "" {
		v := r.Vertices[currID]
		currID = v.Next
		if !v.Deleted && v.ID != r.Head {
			snapshot = append(snapshot, v)
		}
	}

	index := 0
	return func() (T, bool) {
		if index < len(snapshot) {
			val := snapshot[index].Value
			index++
			return val, true
		}
		var zero T
		return zero, false
	}
}
