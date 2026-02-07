package crdt

import (
	"encoding/json"
	"errors"
	"fmt"
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

// ORSetElement 表示 ORSet 中的一个元素
type ORSetElement struct {
	ID        string              // 元素唯一 ID
	Child     CRDT                // 嵌套的 CRDT
	ChildType Type                // 子 CRDT 类型
	Tags      map[string]struct{} // 活跃标签（用于 Add-Wins 冲突解决）
}

// ORSet (Observed-Remove Set，可观察移除集合)
// 支持嵌套 CRDT，每个元素持有一个 CRDT 实例
type ORSet struct {
	elems map[string]*ORSetElement // ElemID -> Element
	mu    sync.RWMutex
}

func NewORSet() *ORSet {
	return &ORSet{
		elems: make(map[string]*ORSetElement),
	}
}

// ORSetOp 表示 ORSet 的操作
type ORSetOp struct {
	OriginID string
	TypeCode int // 0 = 添加, 1 = 移除, 2 = 子操作转发

	// 添加参数
	ElemID   string      // 元素唯一 ID
	InitType Type        // CRDT 类型
	InitVal  interface{} // 初始值（可选）
	Tag      string      // 此添加操作的唯一标签
	Ts       int64

	// 移除参数
	RemoveID string   // 要移除的元素 ID
	RemTags  []string // 要移除的标签

	// 子操作转发参数（TypeCode=2）
	TargetID string // 目标元素 ID
	ChildOp  Op     // 要转发的子操作
}

func (op ORSetOp) Origin() string   { return op.OriginID }
func (op ORSetOp) Type() Type       { return TypeSet }
func (op ORSetOp) Timestamp() int64 { return op.Ts }

// MarshalJSON 自定义序列化以支持 Op 接口
func (op ORSetOp) MarshalJSON() ([]byte, error) {
	type Alias ORSetOp
	aux := &struct {
		ChildOp *json.RawMessage `json:"ChildOp,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(&op),
	}

	if op.ChildOp != nil {
		data, err := json.Marshal(op.ChildOp)
		if err != nil {
			return nil, err
		}
		wrapper := TypedOpWrapper{
			Type: op.ChildOp.Type(),
			Data: data,
		}
		b, err := json.Marshal(wrapper)
		if err != nil {
			return nil, err
		}
		raw := json.RawMessage(b)
		aux.ChildOp = &raw
	}
	return json.Marshal(aux)
}

// UnmarshalJSON 自定义反序列化以支持 Op 接口
func (op *ORSetOp) UnmarshalJSON(data []byte) error {
	type Alias ORSetOp
	aux := &struct {
		ChildOp *json.RawMessage `json:"ChildOp,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(op),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if aux.ChildOp != nil {
		var wrapper TypedOpWrapper
		if err := json.Unmarshal(*aux.ChildOp, &wrapper); err != nil {
			return err
		}
		child, err := OpReg.UnmarshalOp(wrapper.Type, wrapper.Data)
		if err != nil {
			return err
		}
		op.ChildOp = child
	}
	return nil
}

func (s *ORSet) Apply(op Op) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sOp, ok := op.(ORSetOp)
	if !ok {
		return errors.New("ORSet 的操作无效")
	}

	if sOp.TypeCode == 0 { // 添加
		elem, exists := s.elems[sOp.ElemID]
		if !exists {
			// 创建新元素
			child, err := Factory.NewCRDT(sOp.OriginID, sOp.InitType)
			if err != nil {
				return fmt.Errorf("创建嵌套 CRDT 失败: %w", err)
			}

			// 如果有初始值且是 Register 类型，应用初始值
			if sOp.InitVal != nil && sOp.InitType == TypeRegister {
				initOp := LWWOp{
					OriginID: sOp.OriginID,
					Value:    sOp.InitVal,
					Ts:       sOp.Ts,
				}
				if err := child.Apply(initOp); err != nil {
					return fmt.Errorf("应用初始值失败: %w", err)
				}
			}

			elem = &ORSetElement{
				ID:        sOp.ElemID,
				Child:     child,
				ChildType: sOp.InitType,
				Tags:      make(map[string]struct{}),
			}
			s.elems[sOp.ElemID] = elem
		}
		// 添加标签
		if sOp.Tag != "" {
			elem.Tags[sOp.Tag] = struct{}{}
		}

	} else if sOp.TypeCode == 1 { // 移除
		elem, exists := s.elems[sOp.RemoveID]
		if exists {
			for _, t := range sOp.RemTags {
				delete(elem.Tags, t)
			}
			// 如果没有活跃标签，删除元素
			if len(elem.Tags) == 0 {
				delete(s.elems, sOp.RemoveID)
			}
		}

	} else if sOp.TypeCode == 2 { // 子操作转发
		elem, exists := s.elems[sOp.TargetID]
		if !exists {
			return fmt.Errorf("未找到目标元素 %s", sOp.TargetID)
		}
		if len(elem.Tags) == 0 {
			return fmt.Errorf("目标元素 %s 已被删除", sOp.TargetID)
		}
		if elem.Child == nil {
			return fmt.Errorf("目标元素 %s 没有子 CRDT", sOp.TargetID)
		}
		if sOp.ChildOp == nil {
			return errors.New("子操作为空")
		}
		return elem.Child.Apply(sOp.ChildOp)
	}

	return nil
}

func (s *ORSet) Value() interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make([]interface{}, 0, len(s.elems))
	for _, elem := range s.elems {
		if len(elem.Tags) > 0 && elem.Child != nil {
			res = append(res, elem.Child.Value())
		}
	}
	return res
}

func (s *ORSet) Type() Type { return TypeSet }

// GetElement 获取指定 ID 的元素的 CRDT 实例
func (s *ORSet) GetElement(id string) CRDT {
	s.mu.RLock()
	defer s.mu.RUnlock()

	elem, exists := s.elems[id]
	if !exists || len(elem.Tags) == 0 {
		return nil
	}
	return elem.Child
}

// GetTags 返回指定元素的所有标签
func (s *ORSet) GetTagsByID(id string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	elem, exists := s.elems[id]
	if !exists {
		return nil
	}

	result := make([]string, 0, len(elem.Tags))
	for tag := range elem.Tags {
		result = append(result, tag)
	}
	return result
}

// Elements 返回集合中所有元素
func (s *ORSet) Elements() []ORSetElement {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]ORSetElement, 0, len(s.elems))
	for _, elem := range s.elems {
		if len(elem.Tags) > 0 {
			res = append(res, *elem)
		}
	}
	return res
}

// Bytes 返回 ORSet 的序列化状态
func (s *ORSet) Bytes() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	type elemState struct {
		ID        string      `json:"id"`
		ChildType Type        `json:"child_type"`
		Value     interface{} `json:"value"`
		Tags      []string    `json:"tags"`
	}

	elems := make([]elemState, 0, len(s.elems))
	for _, elem := range s.elems {
		if len(elem.Tags) > 0 && elem.Child != nil {
			tagList := make([]string, 0, len(elem.Tags))
			for tag := range elem.Tags {
				tagList = append(tagList, tag)
			}
			elems = append(elems, elemState{
				ID:        elem.ID,
				ChildType: elem.ChildType,
				Value:     elem.Child.Value(),
				Tags:      tagList,
			})
		}
	}

	b, _ := json.Marshal(elems)
	return b
}

// Len 返回集合中活跃元素的数量
func (s *ORSet) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, elem := range s.elems {
		if len(elem.Tags) > 0 {
			count++
		}
	}
	return count
}
