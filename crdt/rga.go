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

type RGANode struct {
	ID        string
	Value     interface{}
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

type RGAOp struct {
	OriginID string
	TypeCode int // 0 = 插入, 1 = 移除

	// 插入参数
	PrevID string
	ElemID string
	Value  interface{}
	Ts     int64

	// 移除参数
	RemoveID string
}

func (op RGAOp) Origin() string   { return op.OriginID }
func (op RGAOp) Type() Type       { return TypeSequence }
func (op RGAOp) Timestamp() int64 { return op.Ts }

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

		// 3. 插入
		newNode := &RGANode{
			ID:        rOp.ElemID,
			Value:     rOp.Value,
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
	}

	return nil
}

func (r *RGA) Value() interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var res []interface{}
	curr := r.head.Next
	for curr != nil {
		if !curr.Tombstone {
			res = append(res, curr.Value)
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
	ID    string      `json:"id"`
	Value interface{} `json:"value"`
}

// Elements 返回序列内容（包含元素 ID）。
func (r *RGA) Elements() []RGAElement {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var res []RGAElement
	curr := r.head.Next
	for curr != nil {
		if !curr.Tombstone {
			res = append(res, RGAElement{
				ID:    curr.ID,
				Value: curr.Value,
			})
		}
		curr = curr.Next
	}
	return res
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
