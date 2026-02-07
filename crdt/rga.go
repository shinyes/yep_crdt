package crdt

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// RGA (可复制的可扩展数组)
type RGA struct {
	// 我们需要一种方式来存储列表。
	// 一种简单的方法是节点切片，但插入需要查找位置。
	// 或者使用链表。
	vertices map[string]*RGANode // ID -> 节点的映射，用于快速查找
	head     *RGANode            // 虚拟头节点
	mu       sync.RWMutex
}

// RGANode 表示 RGA 中的一个节点
type RGANode struct {
	ID        string
	Child     CRDT // 存储嵌套的 CRDT（替代原来的 Value interface{}）
	ChildType Type // 子 CRDT 类型（序列化需要）
	Next      *RGANode
	Timestamp int64
	Tombstone bool
}

func NewRGA() *RGA {
	head := &RGANode{ID: "start", Timestamp: 0}
	r := &RGA{
		vertices: make(map[string]*RGANode),
		head:     head,
	}
	r.vertices[head.ID] = head
	return r
}

// RGAOp 表示 RGA 的操作
type RGAOp struct {
	OriginID string
	TypeCode int // 0 = 插入, 1 = 移除, 2 = 子操作转发

	// 插入参数
	PrevID   string
	ElemID   string
	InitType Type        // 新元素的 CRDT 类型
	InitVal  interface{} // 初始值（可选，用于 Register 类型）
	Ts       int64

	// 移除参数
	RemoveID string

	// 子操作转发参数（TypeCode=2）
	TargetID string // 目标元素 ID
	ChildOp  Op     // 要转发的子操作
}

func (op RGAOp) Origin() string   { return op.OriginID }
func (op RGAOp) Type() Type       { return TypeSequence }
func (op RGAOp) Timestamp() int64 { return op.Ts }

// MarshalJSON 自定义序列化以支持 Op 接口
func (op RGAOp) MarshalJSON() ([]byte, error) {
	type Alias RGAOp
	aux := &struct {
		ChildOp *json.RawMessage `json:"ChildOp,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(&op),
	}

	if op.ChildOp != nil {
		// 先序列化 ChildOp 数据
		data, err := json.Marshal(op.ChildOp)
		if err != nil {
			return nil, err
		}
		// 包装类型
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
func (op *RGAOp) UnmarshalJSON(data []byte) error {
	type Alias RGAOp
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

		// 使用 Op 注册表反序列化
		child, err := OpReg.UnmarshalOp(wrapper.Type, wrapper.Data)
		if err != nil {
			return err
		}
		op.ChildOp = child
	}
	return nil
}

func (r *RGA) Apply(op Op) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	rOp, ok := op.(RGAOp)
	if !ok {
		return errors.New("RGA 的操作无效")
	}

	if rOp.TypeCode == 0 { // 插入
		// 1. 查找前一个节点
		prev, ok := r.vertices[rOp.PrevID]
		if !ok {
			// 如果没找到前一个节点，我们可能需要缓存或报错。
			// 为简单起见，报错或忽略（因果性违反）。
			return fmt.Errorf("未找到前置节点 %s", rOp.PrevID)
		}

		if _, exists := r.vertices[rOp.ElemID]; exists {
			// 已经应用过（幂等性）
			return nil
		}

		// 2. 确定位置
		// 我们需要在 prev 之后插入。但如果 prev.Next 存在，我们可能需要跳过
		// 具有更高时间戳（或相同时间戳且具有更高 ID）的节点。
		curr := prev

		for curr.Next != nil {
			next := curr.Next
			// 跳过条件：下一个节点比新节点更新
			if next.Timestamp > rOp.Ts || (next.Timestamp == rOp.Ts && next.ID > rOp.ElemID) {
				curr = next
			} else {
				break
			}
		}

		// 3. 创建嵌套 CRDT 实例
		child, err := Factory.NewCRDT(rOp.OriginID, rOp.InitType)
		if err != nil {
			return fmt.Errorf("创建嵌套 CRDT 失败: %w", err)
		}

		// 如果有初始值且是 Register 类型，应用初始值
		if rOp.InitVal != nil && rOp.InitType == TypeRegister {
			initOp := LWWOp{
				OriginID: rOp.OriginID,
				Value:    rOp.InitVal,
				Ts:       rOp.Ts,
			}
			if err := child.Apply(initOp); err != nil {
				return fmt.Errorf("应用初始值失败: %w", err)
			}
		}

		// 4. 插入节点
		newNode := &RGANode{
			ID:        rOp.ElemID,
			Child:     child,
			ChildType: rOp.InitType,
			Timestamp: rOp.Ts,
			Next:      curr.Next,
			Tombstone: false,
		}
		curr.Next = newNode
		r.vertices[newNode.ID] = newNode

	} else if rOp.TypeCode == 1 { // 移除
		node, ok := r.vertices[rOp.RemoveID]
		if ok {
			node.Tombstone = true
		}

	} else if rOp.TypeCode == 2 { // 子操作转发
		node, ok := r.vertices[rOp.TargetID]
		if !ok {
			return fmt.Errorf("未找到目标元素 %s", rOp.TargetID)
		}
		if node.Tombstone {
			return fmt.Errorf("目标元素 %s 已被删除", rOp.TargetID)
		}
		if node.Child == nil {
			return fmt.Errorf("目标元素 %s 没有子 CRDT", rOp.TargetID)
		}
		if rOp.ChildOp == nil {
			return errors.New("子操作为空")
		}
		return node.Child.Apply(rOp.ChildOp)
	}

	return nil
}

func (r *RGA) Value() interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var res []interface{}
	curr := r.head.Next
	for curr != nil {
		if !curr.Tombstone && curr.Child != nil {
			res = append(res, curr.Child.Value())
		}
		curr = curr.Next
	}
	return res
}

func (r *RGA) Type() Type { return TypeSequence }

// LastID 返回序列中最后一个元素的 ID（用于 Append 操作）。
// 如果序列为空，返回 "start"（虚拟头节点的 ID）。
func (r *RGA) LastID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	lastID := "start"
	curr := r.head.Next
	for curr != nil {
		if !curr.Tombstone {
			lastID = curr.ID
		}
		curr = curr.Next
	}
	return lastID
}

// RGAElement 表示序列中的一个元素
type RGAElement struct {
	ID        string      `json:"id"`
	Value     interface{} `json:"value"`
	ChildType Type        `json:"child_type"`
}

// Elements 返回序列内容（包含元素 ID）。
func (r *RGA) Elements() []RGAElement {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var res []RGAElement
	curr := r.head.Next
	for curr != nil {
		if !curr.Tombstone && curr.Child != nil {
			res = append(res, RGAElement{
				ID:        curr.ID,
				Value:     curr.Child.Value(),
				ChildType: curr.ChildType,
			})
		}
		curr = curr.Next
	}
	return res
}

// GetElement 获取指定 ID 的元素的 CRDT 实例
func (r *RGA) GetElement(id string) CRDT {
	r.mu.RLock()
	defer r.mu.RUnlock()

	node, ok := r.vertices[id]
	if !ok || node.Tombstone {
		return nil
	}
	return node.Child
}

// Bytes 返回 RGA 的序列化状态。
func (r *RGA) Bytes() []byte {
	r.mu.RLock()
	defer r.mu.RUnlock()

	elements := r.Elements()
	b, _ := json.Marshal(elements)
	return b
}

// Len 返回序列中非墓碑元素的数量。
func (r *RGA) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	curr := r.head.Next
	for curr != nil {
		if !curr.Tombstone {
			count++
		}
		curr = curr.Next
	}
	return count
}
