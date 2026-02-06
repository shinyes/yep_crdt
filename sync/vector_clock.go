package sync

import (
	"fmt"
)

// VectorClock 表示一个版本向量。
// 映射 NodeID -> 计数器
type VectorClock map[string]uint64

func NewVectorClock() VectorClock {
	return make(VectorClock)
}

func (vc VectorClock) Increment(nodeID string) {
	vc[nodeID]++
}

func (vc VectorClock) Merge(other VectorClock) {
	for id, counter := range other {
		if counter > vc[id] {
			vc[id] = counter
		}
	}
}

// Compare 返回：
// -1 如果 vc < other
//
//	0 如果 vc == other 或并发
//	1 如果 vc > other
//
// 注意：偏序使得简单的比较变得复杂（存在并发情况）。
// 通常我们允许检查 "Descends" (<=)。
func (vc VectorClock) Descends(other VectorClock) bool {
	for id, otherCtr := range other {
		if vc[id] < otherCtr {
			return false
		}
	}
	return true
}

func (vc VectorClock) Bytes() []byte {
	// 序列化为字节（例如 JSON 或专用编码）
	// 占位符
	return []byte(fmt.Sprintf("%v", vc))
}
