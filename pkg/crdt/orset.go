package crdt

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/hlc"
)

// ORSet 实现观察-移除 (Observed-Remove) 集合。
// 它使用每个元素的活动 ID 集合和已删除 ID 的墓碑集合。
// T 必须是 comparable，以便用作 map 的键。
type ORSet[T comparable] struct {
	mu         sync.RWMutex
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

// Elements 返回集合中的所有元素。
func (s *ORSet[T]) Elements() []T {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

func (s *ORSet[T]) Value() any {
	return s.Elements()
}

// Contains 检查集合中是否包含某个元素。
func (s *ORSet[T]) Contains(element T) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids, ok := s.AddSet[element]
	if !ok {
		return false
	}
	for id := range ids {
		if _, deleted := s.Tombstones[id]; !deleted {
			return true
		}
	}
	return false
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
	s.mu.Lock()
	defer s.mu.Unlock()

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
		// 延迟删除策略：只标记为已删除，不从 AddSet 中移除
		// 在 GC 时才真正清理 AddSet 中的已删除 ID
		for id := range ids {
			s.Tombstones[id] = ts
		}

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

	o.mu.RLock()
	defer o.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. 合并 Tombstones (深度拷贝)
	for id, ts := range o.Tombstones {
		if localTs, exists := s.Tombstones[id]; !exists || ts > localTs {
			s.Tombstones[id] = ts
		}
	}

	// 2. 合并 AddSets (深度拷贝)
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
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	
	// 1. 清理过期的 Tombstones 和对应的 AddSet 元素
	for id, ts := range s.Tombstones {
		if ts < safeTimestamp {
			// 找到并删除 AddSet 中对应的元素
			for elem, ids := range s.AddSet {
				if _, exists := ids[id]; exists {
					delete(s.AddSet[elem], id)
					count++
					// 如果元素的所有 ID 都被删除了，从 AddSet 中移除该元素
					if len(s.AddSet[elem]) == 0 {
						delete(s.AddSet, elem)
					}
					break // 每个 ID 只在一个元素中
				}
			}
			// 删除 Tombstone
			delete(s.Tombstones, id)
		}
	}
	
	return count
}

func (s *ORSet[T]) Bytes() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 需要创建深拷贝以避免锁被 JSON 序列化期间持有
	tempSet := &ORSet[T]{
		AddSet:     make(map[T]map[string]struct{}, len(s.AddSet)),
		Tombstones: make(map[string]int64, len(s.Tombstones)),
	}
	for k, v := range s.AddSet {
		tempSet.AddSet[k] = make(map[string]struct{}, len(v))
		for id := range v {
			tempSet.AddSet[k][id] = struct{}{}
		}
	}
	for k, v := range s.Tombstones {
		tempSet.Tombstones[k] = v
	}
	return json.Marshal(tempSet)
}

// FromBytesORSet 反序列化 ORSet。需要指定 T。
func FromBytesORSet[T comparable](data []byte) (*ORSet[T], error) {
	s := NewORSet[T]()
	if err := json.Unmarshal(data, s); err != nil {
		return nil, err
	}
	return s, nil
}
