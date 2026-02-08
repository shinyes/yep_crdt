package crdt

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/hlc"
)

// ORSet 实现观察-移除 (Observed-Remove) 集合。
// 它使用每个元素的活动 ID 集合和已删除 ID 的墓碑集合。
// T 必须是 comparable，以便用作 map 的键。
type ORSet[T comparable] struct {
	AddSet     map[T]map[string]struct{} // 元素 -> ID 集合
	Tombstones map[string]int64          // 已删除 ID -> 删除时间戳
	Clock      *hlc.Clock                // 混合逻辑时钟 (可选，用于 Apply)
}

// NewORSet 创建一个新的 ORSet。
func NewORSet[T comparable]() *ORSet[T] {
	return &ORSet[T]{
		AddSet:     make(map[T]map[string]struct{}),
		Tombstones: make(map[string]int64),
		Clock:      hlc.New(), // 默认自带时钟，也可外部注入
	}
}

func (s *ORSet[T]) Type() Type {
	return TypeORSet
}

func (s *ORSet[T]) Value() interface{} {
	elements := make([]T, 0, len(s.AddSet))
	for e, ids := range s.AddSet {
		// 过滤掉在 Tombstones 中的 ID
		active := false
		for id := range ids {
			if _, deleted := s.Tombstones[id]; !deleted {
				active = true
				break
			}
		}
		if active {
			elements = append(elements, e)
		}
	}
	return elements
}

// OpORSetAdd 添加一个元素。
type OpORSetAdd[T comparable] struct {
	Element T
}

func (op OpORSetAdd[T]) Type() Type { return TypeORSet }

// OpORSetRemove 移除一个元素。
type OpORSetRemove[T comparable] struct {
	Element T
}

func (op OpORSetRemove[T]) Type() Type { return TypeORSet }

func (s *ORSet[T]) Apply(op Op) error {
	switch o := op.(type) {
	case OpORSetAdd[T]:
		// 生成唯一 ID
		id := uuid.NewString() // 在生产环境中，依赖混合逻辑时钟或类似机制
		if s.AddSet[o.Element] == nil {
			s.AddSet[o.Element] = make(map[string]struct{})
		}
		s.AddSet[o.Element][id] = struct{}{}

	case OpORSetRemove[T]:
		ids, ok := s.AddSet[o.Element]
		if !ok {
			return nil // 无需移除
		}
		ts := int64(0)
		if s.Clock != nil {
			ts = s.Clock.Now()
		}
		for id := range ids {
			s.Tombstones[id] = ts
		}
		// 立即从 AddSet 中删除，以节省空间和后续遍历时间
		delete(s.AddSet, o.Element)

	default:
		return ErrInvalidOp
	}
	return nil
}

func (s *ORSet[T]) Merge(other CRDT) error {
	o, ok := other.(*ORSet[T])
	if !ok {
		return fmt.Errorf("cannot merge %T into ORSet", other)
	}

	// 1. 合并 Tombstones
	for id, ts := range o.Tombstones {
		if localTs, exists := s.Tombstones[id]; !exists || ts > localTs {
			s.Tombstones[id] = ts
		}
	}

	// 2. 合并 AddSets
	for elem, ids := range o.AddSet {
		if s.AddSet[elem] == nil {
			s.AddSet[elem] = make(map[string]struct{})
		}
		for id := range ids {
			s.AddSet[elem][id] = struct{}{}
		}
	}

	// 3. 优化：清理 AddSet 中已经在 Tombstones 中的元素
	for elem, ids := range s.AddSet {
		for id := range ids {
			if _, deleted := s.Tombstones[id]; deleted {
				delete(s.AddSet[elem], id)
			}
		}
		if len(s.AddSet[elem]) == 0 {
			delete(s.AddSet, elem)
		}
	}
	return nil
}

func (s *ORSet[T]) GC(safeTimestamp int64) int {
	count := 0
	for id, ts := range s.Tombstones {
		if ts < safeTimestamp {
			delete(s.Tombstones, id)
			count++
		}
	}
	return count
}

func (s *ORSet[T]) Bytes() ([]byte, error) {
	return json.Marshal(s)
}

// FromBytesORSet 反序列化 ORSet。需要指定 T。
func FromBytesORSet[T comparable](data []byte) (*ORSet[T], error) {
	s := NewORSet[T]()
	if err := json.Unmarshal(data, s); err != nil {
		return nil, err
	}
	return s, nil
}
