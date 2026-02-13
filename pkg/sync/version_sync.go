package sync

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
)

// VersionSync 版本沟通组件。
// 当新节点连接时，交换版本摘要，发现差异后补发数据。
type VersionSync struct {
	db      *db.DB
	nodeMgr *NodeManager
}

// NewVersionSync 创建版本沟通组件
func NewVersionSync(database *db.DB, nodeMgr *NodeManager) *VersionSync {
	return &VersionSync{
		db:      database,
		nodeMgr: nodeMgr,
	}
}

// OnPeerConnected 新节点连接时触发版本沟通。
// 构建本地版本摘要并发送给对方。
func (vs *VersionSync) OnPeerConnected(peerID string) {
	digest := vs.BuildDigest()
	if digest == nil {
		return
	}

	// 序列化摘要
	digestBytes, err := json.Marshal(digest)
	if err != nil {
		log.Printf("[VersionSync] 序列化摘要失败: %v", err)
		return
	}

	// 发送版本摘要消息
	msg := &NetworkMessage{
		Type:    MsgTypeVersionDigest,
		NodeID:  vs.nodeMgr.localNodeID,
		RawData: digestBytes,
	}

	if err := vs.nodeMgr.network.SendRawData(peerID, "_version_digest", "", digestBytes, vs.db.Clock().Now()); err != nil {
		log.Printf("[VersionSync] 发送版本摘要失败: %v", err)
		return
	}

	log.Printf("[VersionSync] 已发送版本摘要到 %s, tables=%d", peerID[:8], len(digest.Tables))
	_ = msg // msg 变量仅用于文档说明
}

// BuildDigest 构建本地所有表的版本摘要
func (vs *VersionSync) BuildDigest() *VersionDigest {
	tableNames := vs.db.TableNames()
	if len(tableNames) == 0 {
		return nil
	}

	digest := &VersionDigest{
		NodeID: vs.nodeMgr.localNodeID,
		Tables: make([]TableDigest, 0, len(tableNames)),
	}

	for _, tableName := range tableNames {
		table := vs.db.Table(tableName)
		if table == nil {
			continue
		}

		// 扫描表的行摘要
		var rowDigests []db.RowDigest
		vs.db.View(func(tx *db.Tx) error {
			t := tx.Table(tableName)
			if t != nil {
				rowDigests, _ = t.ScanRowDigest()
			}
			return nil
		})

		rowKeys := make(map[string]uint32, len(rowDigests))
		for _, rd := range rowDigests {
			rowKeys[rd.Key.String()] = rd.Hash
		}

		digest.Tables = append(digest.Tables, TableDigest{
			TableName: tableName,
			RowKeys:   rowKeys,
		})
	}

	return digest
}

// OnReceiveDigest 处理收到的远程版本摘要。
// 比较本地和远程数据，将本地有但远端缺少或不同的行发送过去。
func (vs *VersionSync) OnReceiveDigest(peerID string, msg *NetworkMessage) {
	// 解析版本摘要
	var remoteDigest VersionDigest
	if err := json.Unmarshal(msg.RawData, &remoteDigest); err != nil {
		log.Printf("[VersionSync] 解析远程摘要失败: %v", err)
		return
	}

	log.Printf("[VersionSync] 收到 %s 的版本摘要, tables=%d", peerID[:8], len(remoteDigest.Tables))

	// 构建远程摘要的索引: tableName -> { key -> hash }
	remoteIndex := make(map[string]map[string]uint32)
	for _, td := range remoteDigest.Tables {
		remoteIndex[td.TableName] = td.RowKeys
	}

	// 比较本地数据与远程摘要，找出差异
	var diffCount int
	tableNames := vs.db.TableNames()

	for _, tableName := range tableNames {
		table := vs.db.Table(tableName)
		if table == nil {
			continue
		}

		// 获取本地摘要
		var localDigests []db.RowDigest
		vs.db.View(func(tx *db.Tx) error {
			t := tx.Table(tableName)
			if t != nil {
				localDigests, _ = t.ScanRowDigest()
			}
			return nil
		})

		remoteRows := remoteIndex[tableName]

		for _, ld := range localDigests {
			keyStr := ld.Key.String()
			remoteHash, existsInRemote := uint32(0), false
			if remoteRows != nil {
				remoteHash, existsInRemote = remoteRows[keyStr]
			}

			// 远端缺少该行，或哈希不同 → 发送过去
			if !existsInRemote || remoteHash != ld.Hash {
				vs.sendRow(peerID, tableName, ld.Key)
				diffCount++
			}
		}
	}

	log.Printf("[VersionSync] 向 %s 补发了 %d 行差异数据", peerID[:8], diffCount)

	// 同时发送自己的版本摘要让对方也比较（双向同步）
	// 只在对方先发送摘要时才回发，避免无限循环
	if msg.NodeID != vs.nodeMgr.localNodeID {
		go vs.OnPeerConnected(peerID) // 发送自己的摘要
	}
}

// sendRow 发送单行数据到指定节点
func (vs *VersionSync) sendRow(peerID string, tableName string, key uuid.UUID) {
	var rawData []byte
	vs.db.View(func(tx *db.Tx) error {
		table := tx.Table(tableName)
		if table != nil {
			rawData, _ = table.GetRawRow(key)
		}
		return nil
	})

	if rawData == nil {
		return
	}

	timestamp := vs.db.Clock().Now()
	if err := vs.nodeMgr.network.SendRawData(peerID, tableName, key.String(), rawData, timestamp); err != nil {
		log.Printf("[VersionSync] 发送行数据失败: table=%s, key=%s, err=%v",
			tableName, key.String()[:8], err)
	}
}

// CompareAndSync 主动与指定节点进行版本比较并同步差异。
// 这是手动触发的版本，OnPeerConnected 会自动调用。
func (vs *VersionSync) CompareAndSync(peerID string) error {
	if vs.nodeMgr.network == nil {
		return fmt.Errorf("网络未注册")
	}

	vs.OnPeerConnected(peerID)
	return nil
}
