package sync

import (
	"encoding/json"
)

// VectorClock 表示一个版本向量。
// 映射 NodeID -> 计数器
type VectorClock map[string]uint64

// NewVectorClock 创建一个新的空版本向量。
func NewVectorClock() VectorClock {
	return make(VectorClock)
}

// Clone 返回版本向量的深拷贝。
func (vc VectorClock) Clone() VectorClock {
	clone := make(VectorClock, len(vc))
	for id, counter := range vc {
		clone[id] = counter
	}
	return clone
}

// Get 安全获取指定节点的计数器值。
// 如果节点不存在，返回 0。
func (vc VectorClock) Get(nodeID string) uint64 {
	return vc[nodeID]
}

// Set 设置指定节点的计数器值。
func (vc VectorClock) Set(nodeID string, value uint64) {
	vc[nodeID] = value
}

// Increment 递增指定节点的计数器。
func (vc VectorClock) Increment(nodeID string) {
	vc[nodeID]++
}

// Merge 合并另一个版本向量，取每个节点的最大值。
func (vc VectorClock) Merge(other VectorClock) {
	for id, counter := range other {
		if counter > vc[id] {
			vc[id] = counter
		}
	}
}

// Descends 检查当前版本向量是否涵盖（>=）另一个版本向量。
// 返回 true 表示 vc 已经看过 other 包含的所有操作。
func (vc VectorClock) Descends(other VectorClock) bool {
	for id, otherCtr := range other {
		if vc[id] < otherCtr {
			return false
		}
	}
	return true
}

// Compare 比较两个版本向量的关系。
// 返回值：
//   - -1: vc 严格落后于 other（vc < other）
//   - 0: 相等或并发关系
//   - 1: vc 严格领先于 other（vc > other）
func (vc VectorClock) Compare(other VectorClock) int {
	vcDescends := vc.Descends(other)
	otherDescends := other.Descends(vc)

	if vcDescends && otherDescends {
		// 两者相互涵盖，说明相等
		return 0
	}
	if vcDescends && !otherDescends {
		// vc 涵盖 other，但 other 不涵盖 vc
		return 1
	}
	if !vcDescends && otherDescends {
		// other 涵盖 vc，但 vc 不涵盖 other
		return -1
	}
	// 两者都不涵盖对方，是并发关系
	return 0
}

// IsEmpty 检查版本向量是否为空。
func (vc VectorClock) IsEmpty() bool {
	return len(vc) == 0
}

// Bytes 返回版本向量的 JSON 序列化字节。
func (vc VectorClock) Bytes() []byte {
	data, err := json.Marshal(vc)
	if err != nil {
		return []byte("{}")
	}
	return data
}

// FromBytes 从 JSON 字节反序列化版本向量。
func FromBytes(data []byte) (VectorClock, error) {
	vc := NewVectorClock()
	if len(data) == 0 {
		return vc, nil
	}
	if err := json.Unmarshal(data, &vc); err != nil {
		return nil, err
	}
	return vc, nil
}

// RecordOp 记录一个操作到版本向量中。
// 使用操作的时间戳作为该来源的计数器值（如果更大）。
func (vc VectorClock) RecordOp(origin string, timestamp uint64) {
	if timestamp > vc[origin] {
		vc[origin] = timestamp
	}
}

