package crdt

import (
	"encoding/json"
	"errors"
	"sync"
)

// GSet (只增集合)
type GSet struct {
	elems map[interface{}]struct{}
	mu    sync.RWMutex
}

func NewGSet() *GSet {
	return &GSet{
		elems: make(map[interface{}]struct{}),
	}
}

type SetOp struct {
	OriginID string
	Val      interface{}
	Add      bool // 对于 OR-Set，true 表示增加，false 表示移除
	Ts       int64
}

func (op SetOp) Origin() string   { return op.OriginID }
func (op SetOp) Type() Type       { return TypeSet }
func (op SetOp) Timestamp() int64 { return op.Ts }

func (s *GSet) Apply(op Op) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sOp, ok := op.(SetOp)
	if !ok {
		return errors.New("GSet 的操作无效")
	}
	if !sOp.Add {
		return errors.New("无法从 GSet 中移除元素")
	}
	s.elems[sOp.Val] = struct{}{}
	return nil
}

func (s *GSet) Value() interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make([]interface{}, 0, len(s.elems))
	for k := range s.elems {
		res = append(res, k)
	}
	return res
}

func (s *GSet) Type() Type { return TypeSet }

// ORSet (Observed-Remove Set，可观察移除集合)
// 通常使用 Add-Wins（增加胜出）策略，或基于 Tag 的策略。
// 最简单的 OR-Set: Map[Element] -> Set[Tag/UUID]
// Add(E): 创建唯一标签，添加到 E 的标签集中。
// Remove(E): 清除 E 的标签集。
// Merge: 标签集的并集。
type ORSet struct {
	// elems 将元素值映射到一组唯一标签（观察到的添加操作）
	elems map[interface{}]map[string]struct{}
	mu    sync.RWMutex
}

func NewORSet() *ORSet {
	return &ORSet{
		elems: make(map[interface{}]map[string]struct{}),
	}
}

type ORSetOp struct {
	OriginID string
	Val      interface{}
	Add      bool
	Tag      string   // 此添加操作的唯一标签
	RemTags  []string // 要移除的标签（在准备阶段预先计算）
	Ts       int64
}

func (op ORSetOp) Origin() string   { return op.OriginID }
func (op ORSetOp) Type() Type       { return TypeSet }
func (op ORSetOp) Timestamp() int64 { return op.Ts }

func (s *ORSet) Apply(op Op) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sOp, ok := op.(ORSetOp)
	if !ok {
		return errors.New("ORSet 的操作无效")
	}

	if sOp.Add {
		if s.elems[sOp.Val] == nil {
			s.elems[sOp.Val] = make(map[string]struct{})
		}
		s.elems[sOp.Val][sOp.Tag] = struct{}{}
	} else {
		// 移除在生成时观察到的特定标签
		if tags, exists := s.elems[sOp.Val]; exists {
			for _, t := range sOp.RemTags {
				delete(tags, t)
			}
			if len(tags) == 0 {
				delete(s.elems, sOp.Val)
			}
		}
	}
	return nil
}

func (s *ORSet) Value() interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make([]interface{}, 0, len(s.elems))
	for k := range s.elems {
		res = append(res, k)
	}
	return res
}

func (s *ORSet) Type() Type { return TypeSet }

// GetTags 返回指定元素的所有标签
func (s *ORSet) GetTags(val interface{}) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tags, exists := s.elems[val]
	if !exists {
		return nil
	}

	result := make([]string, 0, len(tags))
	for tag := range tags {
		result = append(result, tag)
	}
	return result
}

// Bytes 返回 ORSet 的序列化状态
func (s *ORSet) Bytes() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 将 map[interface{}]map[string]struct{} 转换为可序列化的格式
	type elemState struct {
		Val  interface{} `json:"val"`
		Tags []string    `json:"tags"`
	}

	elems := make([]elemState, 0, len(s.elems))
	for val, tags := range s.elems {
		tagList := make([]string, 0, len(tags))
		for tag := range tags {
			tagList = append(tagList, tag)
		}
		elems = append(elems, elemState{Val: val, Tags: tagList})
	}

	b, _ := json.Marshal(elems)
	return b
}
