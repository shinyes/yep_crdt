package crdt

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// ORSet 实现观察-移除 (Observed-Remove) 集合。
// 它使用每个元素的活动 ID 集合和已删除 ID 的墓碑集合。
type ORSet struct {
	AddSet     map[string]map[string]struct{} // 元素 -> ID 集合
	Tombstones map[string]struct{}            // 已删除 ID 的集合
}

// NewORSet 创建一个新的 ORSet。
func NewORSet() *ORSet {
	return &ORSet{
		AddSet:     make(map[string]map[string]struct{}),
		Tombstones: make(map[string]struct{}),
	}
}

func (s *ORSet) Type() Type {
	return TypeORSet
}

func (s *ORSet) Value() interface{} {
	elements := make([]string, 0, len(s.AddSet))
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
type OpORSetAdd struct {
	Element string
}

func (op OpORSetAdd) Type() Type { return TypeORSet }

// OpORSetRemove 移除一个元素。
type OpORSetRemove struct {
	Element string
}

func (op OpORSetRemove) Type() Type { return TypeORSet }

func (s *ORSet) Apply(op Op) error {
	switch o := op.(type) {
	case OpORSetAdd:
		// 生成唯一 ID
		id := uuid.NewString() // 在生产环境中，依赖混合逻辑时钟或类似机制
		if s.AddSet[o.Element] == nil {
			s.AddSet[o.Element] = make(map[string]struct{})
		}
		s.AddSet[o.Element][id] = struct{}{}

	case OpORSetRemove:
		ids, ok := s.AddSet[o.Element]
		if !ok {
			return nil // 无需移除
		}
		for id := range ids {
			s.Tombstones[id] = struct{}{}
		}
		// 立即从 AddSet 中删除，以节省空间和后续遍历时间
		delete(s.AddSet, o.Element)

	default:
		return ErrInvalidOp
	}
	return nil
}

func (s *ORSet) Merge(other CRDT) error {
	o, ok := other.(*ORSet)
	if !ok {
		return fmt.Errorf("cannot merge %T into ORSet", other)
	}

	// 1. 合并 Tombstones
	for id := range o.Tombstones {
		s.Tombstones[id] = struct{}{}
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

func (s *ORSet) Bytes() ([]byte, error) {
	return json.Marshal(s)
}

func FromBytesORSet(data []byte) (*ORSet, error) {
	s := NewORSet()
	if err := json.Unmarshal(data, s); err != nil {
		return nil, err
	}
	return s, nil
}
