package manager

import (
	"encoding/json"
	"fmt"

	"github.com/shinyes/yep_crdt/crdt"
)

// 持久化助手

// SaveOp 将操作追加到日志中。
// 键：ops/<root_id>/<timestamp>_<origin>
func (m *Manager) SaveOp(rootID string, op crdt.Op) error {
	// 使用 PackOp 序列化操作以保留类型信息
	data, err := PackOp(op)
	if err != nil {
		return err
	}

	// 使用操作的时间戳和 Origin 作为键，确保唯一性和排序
	ts := fmt.Sprintf("%020d", op.Timestamp()) // 填充零以确保字符串字典序正确
	key := []byte(fmt.Sprintf("ops/%s/%s_%s", rootID, ts, op.Origin()))
	return m.store.Set(key, data)
}

// SaveSnapshot 保存根节点的完整状态。
func (m *Manager) SaveSnapshot(rootID string, c crdt.CRDT) error {
	var data []byte
	var err error

	// Root 需要支持序列化。
	// 我们定义了带有 Bytes() 方法的 State 接口。
	stateC, ok := c.(crdt.State)
	if !ok {
		// 如果未实现 crdt.State，尝试 JSON 序列化
		data, err = json.Marshal(c)
		if err != nil {
			return err
		}
	} else {
		data = stateC.Bytes()
	}

	key := []byte(fmt.Sprintf("snap/%s/latest", rootID))
	return m.store.Set(key, data)
}

// RootMetadata 存储根节点的基本信息
type RootMetadata struct {
	ID        string    `json:"id"`
	Type      crdt.Type `json:"type"`
	CreatedAt int64     `json:"created_at"`
}

func (m *Manager) SaveRootMetadata(meta *RootMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("meta/%s", meta.ID))
	return m.store.Set(key, data)
}

func (m *Manager) LoadRootMetadata(id string) (*RootMetadata, error) {
	key := []byte(fmt.Sprintf("meta/%s", id))
	data, err := m.store.Get(key)
	if err != nil {
		return nil, err
	}
	var meta RootMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// LoadSnapshot 加载最新的快照。需要传入类型以进行初始化。
func (m *Manager) LoadSnapshot(rootID string, typ crdt.Type) (crdt.CRDT, error) {
	key := []byte(fmt.Sprintf("snap/%s/latest", rootID))
	data, err := m.store.Get(key)
	if err != nil {
		return nil, err // 可能没有快照
	}

	// 使用工厂创建 CRDT 实例
	c, err := crdt.Factory.NewCRDT(rootID, typ)
	if err != nil {
		return nil, err
	}

	// 尝试反序列化快照数据
	if len(data) > 0 {
		if err := json.Unmarshal(data, c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// LoadOps 加载指定根节点的所有操作日志
// fromTs: 可选，只加载时间戳 > fromTs 的操作
func (m *Manager) LoadOps(rootID string, fromTs int64) ([]crdt.Op, error) {
	ops := []crdt.Op{}
	prefix := []byte(fmt.Sprintf("ops/%s/", rootID))

	err := m.store.Scan(prefix, func(k, v []byte) error {
		// key format: ops/<root_id>/<timestamp>_<origin>
		// 可以在这里解析 key 检查时间戳以优化性能 (避免 UnpackOp)

		op, err := UnpackOp(v)
		if err != nil {
			return nil // 忽略坏的操作
		}

		if op.Timestamp() > fromTs {
			ops = append(ops, op)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return ops, nil
}
