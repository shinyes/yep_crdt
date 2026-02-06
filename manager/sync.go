package manager

import (
	"encoding/json"
	"fmt"

	"github.com/shinyes/yep_crdt/crdt"
	"github.com/shinyes/yep_crdt/sync"
)

// SyncManager 处理同步逻辑。
type SyncManager struct {
	m *Manager
}

func NewSyncManager(m *Manager) *SyncManager {
	return &SyncManager{m: m}
}

// GetVersionVector 返回根节点的当前版本向量。
// 在实际系统中，这通常是从 Ops 聚合或显式存储的。
func (sm *SyncManager) GetVersionVector(rootID string) (sync.VectorClock, error) {
	// 占位符：是否遍历 ops 来构建？或者直接存储它。
	// 目前返回空。
	return sync.NewVectorClock(), nil
}

// GenerateDelta 返回在给定版本向量之后发生的操作。
func (sm *SyncManager) GenerateDelta(rootID string, remoteVC sync.VectorClock) ([]crdt.Op, error) {
	// 从存储层扫描操作。
	// 键：ops/<root_id>/<timestamp>
	// 我们需要过滤出远程尚未看到的操作。
	// 由于我们的键只是时间戳，扫描所有并过滤？这效率低下。
	// 更好做法：从 VC 暗示的最小时间戳开始扫描？
	// 但 VC 是多维的。
	// 最简单做法：扫描该根节点的所有操作，过滤掉 VC[origin] < op.Timestamp 的操作。
	// 注意：我们使用键 `ops/<root_id>/<timestamp>`。
	// badger.Scan 前缀。

	ops := []crdt.Op{}
	prefix := []byte(fmt.Sprintf("ops/%s/", rootID))

	err := sm.m.store.Scan(prefix, func(k, v []byte) error {
		op, err := UnpackOp(v)
		if err != nil {
			return nil // 跳过无效操作
		}

		// 按版本向量过滤
		// 如果 remoteHas >= ts，说明远程已经有了该操作。
		origin := op.Origin()
		ts := uint64(op.Timestamp()) // 将 int64 转换为 uint64 以用于 VC

		// 如果 VC 已经看过该来源的这个时间戳，跳过
		if remoteHas, ok := remoteVC[origin]; ok && remoteHas >= ts {
			return nil
		}

		ops = append(ops, op)
		return nil
	})

	return ops, err
}

// ApplyDelta 应用操作列表。
func (sm *SyncManager) ApplyDelta(rootID string, ops []crdt.Op) error {
	root, err := sm.m.GetRoot(rootID)
	if err != nil {
		return err // 或者在缺失时创建？
	}

	for _, op := range ops {
		if err := root.Apply(op); err != nil {
			return err
		}
		if err := sm.m.SaveOp(rootID, op); err != nil {
			return err
		}
	}
	return nil
}

// 用于持久化的 OpWrapper，以处理多态性
type OpWrapper struct {
	Type crdt.Type       `json:"type"`
	Data json.RawMessage `json:"data"`
}

func PackOp(op crdt.Op) ([]byte, error) {
	data, err := json.Marshal(op)
	if err != nil {
		return nil, err
	}
	wrapper := OpWrapper{
		Type: op.Type(),
		Data: data,
	}
	return json.Marshal(wrapper)
}

func UnpackOp(data []byte) (crdt.Op, error) {
	var w OpWrapper
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, err
	}

	return crdt.OpReg.UnmarshalOp(w.Type, w.Data)
}
