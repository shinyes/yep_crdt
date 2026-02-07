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
// 从持久化存储加载版本向量。
func (sm *SyncManager) GetVersionVector(rootID string) (sync.VectorClock, error) {
	return sm.m.LoadVersionVector(rootID)
}

// GenerateDelta 返回在给定版本向量之后发生的操作。
// 这用于发送给其他节点以进行同步。
func (sm *SyncManager) GenerateDelta(rootID string, remoteVC sync.VectorClock) ([]crdt.Op, error) {
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
		ts := uint64(op.Timestamp())

		// 如果远程 VC 已经看过该来源的这个时间戳，跳过
		if remoteVC.Get(origin) >= ts {
			return nil
		}

		ops = append(ops, op)
		return nil
	})

	return ops, err
}

// ApplyDelta 应用从远程节点接收的操作列表。
// 它会应用每个操作，保存到存储，并更新本地版本向量。
func (sm *SyncManager) ApplyDelta(rootID string, ops []crdt.Op) error {
	root, err := sm.m.GetRoot(rootID)
	if err != nil {
		return err
	}

	// 加载当前版本向量
	localVC, err := sm.m.LoadVersionVector(rootID)
	if err != nil {
		return err
	}

	for _, op := range ops {
		origin := op.Origin()
		ts := uint64(op.Timestamp())

		// 检查是否已经应用过此操作（去重）
		if localVC.Get(origin) >= ts {
			continue // 已存在，跳过
		}

		// 应用操作
		if err := root.Apply(op); err != nil {
			// 对于某些 CRDT（如 RGA），重复操作可能返回错误
			// 这里可以选择忽略或记录
			continue
		}

		// 保存操作到存储
		if err := sm.m.SaveOp(rootID, op); err != nil {
			return err
		}

		// 更新版本向量
		localVC.RecordOp(origin, ts)
	}

	// 保存更新后的版本向量
	return sm.m.SaveVersionVector(rootID, localVC)
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
