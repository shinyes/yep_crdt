package crdt

import (
	"encoding/json"
	"errors"
	"sync"
)

// MapCRDT 支持嵌套其他 CRDT。
// 键（Keys）映射到一个 CRDT 实例。
// 结构：Key -> CRDT
// 操作：
// - SetKey(key, crdtType, initialVal) -> 通常由 Manager 处理？
// - ApplyOp(key, op) -> 将操作路由到子节点
// - RemoveKey(key) -> 墓碑化或移除
type MapCRDT struct {
	data map[string]CRDT
	mu   sync.RWMutex
}

func NewMapCRDT() *MapCRDT {
	return &MapCRDT{
		data: make(map[string]CRDT),
	}
}

// MapOp 可以是：
// 1. 修改结构（添加/移除键）- 等等，添加键通常意味着在该位置初始化一个 CRDT。
// 2. 将操作路由到子 CRDT。
type MapOp struct {
	OriginID string
	Key      string

	// 如果转发给子节点：
	ChildOp Op

	// 如果结构发生变化（例如在键处初始化一个新的 CRDT）：
	IsInit   bool
	InitType Type
	// 移除：
	IsRemove bool

	Ts int64
}

func (op MapOp) Origin() string   { return op.OriginID }
func (op MapOp) Type() Type       { return TypeMap }
func (op MapOp) Timestamp() int64 { return op.Ts }

// TypedOpWrapper 用于序列化接口 Op
type TypedOpWrapper struct {
	Type Type            `json:"type"`
	Data json.RawMessage `json:"data"`
}

func (op MapOp) MarshalJSON() ([]byte, error) {
	type Alias MapOp
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

func (op *MapOp) UnmarshalJSON(data []byte) error {
	type Alias MapOp
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

func (m *MapCRDT) Apply(op Op) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	mOp, ok := op.(MapOp)
	if !ok {
		return errors.New("MapCRDT 的操作无效")
	}

	if mOp.IsRemove {
		delete(m.data, mOp.Key)
		return nil
	}

	if mOp.IsInit {
		// 如果不存在则初始化新的 CRDT
		if _, exists := m.data[mOp.Key]; !exists {
			// 使用工厂创建 CRDT 实例
			newC, err := Factory.NewCRDT(mOp.OriginID, mOp.InitType)
			if err == nil && newC != nil {
				m.data[mOp.Key] = newC
			}
		}
	}

	// 转发给子节点
	if child, exists := m.data[mOp.Key]; exists && mOp.ChildOp != nil {
		return child.Apply(mOp.ChildOp)
	}

	return nil
}

func (m *MapCRDT) Value() interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[string]interface{})
	for k, v := range m.data {
		res[k] = v.Value()
	}
	return res
}

func (m *MapCRDT) Type() Type { return TypeMap }

// AddChild 允许在本地创建子 CRDT
func (m *MapCRDT) AddChild(key string, c CRDT) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = c
}

func (m *MapCRDT) GetChild(key string) CRDT {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data[key]
}

// MapCRDTState 用于序列化 MapCRDT
type MapCRDTState struct {
	Children map[string]MapChildState `json:"children"`
}

// MapChildState 表示 MapCRDT 中某个子节点的状态
type MapChildState struct {
	Type Type            `json:"type"`
	Data json.RawMessage `json:"data"`
}

// Bytes 返回 MapCRDT 的序列化状态
func (m *MapCRDT) Bytes() []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state := MapCRDTState{
		Children: make(map[string]MapChildState),
	}

	for key, child := range m.data {
		var data []byte
		// 检查子节点是否实现了 State 接口
		if stateC, ok := child.(State); ok {
			data = stateC.Bytes()
		} else {
			// 否则尝试 JSON 序列化
			data, _ = json.Marshal(child)
		}

		state.Children[key] = MapChildState{
			Type: child.Type(),
			Data: data,
		}
	}

	b, _ := json.Marshal(state)
	return b
}

// Keys 返回 MapCRDT 中所有的键
func (m *MapCRDT) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

// Len 返回 MapCRDT 中子节点数量
func (m *MapCRDT) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}
