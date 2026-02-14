package sync

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
)

// Engine 同步引擎，统一管理数据同步的所有组件。
// 通过 db.EnableSync() 创建并启动。
// 实现 db.SyncEngine 接口。
type Engine struct {
	mu      sync.RWMutex
	db      *db.DB
	config  *TenetConfig
	network *TenantNetwork
	nodeMgr *NodeManager
	vs      *VersionSync
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewEngine 创建同步引擎
func NewEngine(database *db.DB, config db.SyncConfig) (*Engine, error) {
	if config.Password == "" {
		return nil, fmt.Errorf("同步密码不能为空")
	}

	tenetConfig := &TenetConfig{
		Password:    config.Password,
		ListenPort:  config.ListenPort,
		EnableDebug: config.Debug,
	}

	// 使用数据库 ID 作为 tenet 频道 ID（自动实现租户隔离）
	tenantID := database.DatabaseID

	// 创建租户网络
	network, err := NewTenantNetwork(tenantID, tenetConfig)
	if err != nil {
		return nil, fmt.Errorf("创建网络失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	e := &Engine{
		db:      database,
		config:  tenetConfig,
		network: network,
		ctx:     ctx,
		cancel:  cancel,
	}

	return e, nil
}

// Start 启动同步引擎
func (e *Engine) Start(ctx context.Context) error {
	// 启动网络
	if err := e.network.Start(); err != nil {
		return fmt.Errorf("启动网络失败: %w", err)
	}

	// 创建节点管理器
	e.nodeMgr = NewNodeManager(e.db, e.network.LocalID())
	e.nodeMgr.RegisterNetwork(e.network)

	// 创建版本沟通组件
	e.vs = NewVersionSync(e.db, e.nodeMgr)

	// 设置消息处理
	e.network.SetBroadcastHandler(PeerMessageHandler{
		OnReceive: func(peerID string, msg *NetworkMessage) {
			e.handleMessage(peerID, msg)
		},
	})

	// 通过 TenantNetwork 注册回调，避免覆盖内部回调。
	e.network.AddPeerConnectedHandler(func(peerID string) {
		log.Printf("[Engine:%s] 节点连接: %s，开始版本沟通", e.db.DatabaseID, peerID[:8])
		e.nodeMgr.OnHeartbeat(peerID, 0) // 注册节点
		go e.vs.OnPeerConnected(peerID)  // 异步发送版本摘要
	})

	e.network.AddPeerDisconnectedHandler(func(peerID string) {
		log.Printf("[Engine:%s] 节点断开: %s", e.db.DatabaseID, peerID[:8])
	})

	// 注册 DB 变更回调（自动广播）
	e.db.OnChange(func(tableName string, key uuid.UUID) {
		go e.OnDataChanged(tableName, key)
	})

	// 启动节点管理器
	e.nodeMgr.Start(ctx)

	// 在 DB 上注册引擎引用
	e.db.SetSyncEngine(e)

	log.Printf("[Engine:%s] 同步引擎已启动, 节点=%s, 地址=%s",
		e.db.DatabaseID, e.network.LocalID()[:8], e.network.LocalAddr())

	return nil
}

// Stop 停止同步引擎
func (e *Engine) Stop() {
	log.Printf("[Engine:%s] 停止同步引擎", e.db.DatabaseID)
	e.cancel()

	if e.nodeMgr != nil {
		e.nodeMgr.Stop()
	}
	if e.network != nil {
		e.network.Stop()
	}
}

// Connect 连接到其他节点
func (e *Engine) Connect(addr string) error {
	return e.network.Connect(addr)
}

// Peers 获取在线节点列表
func (e *Engine) Peers() []string {
	return e.network.Peers()
}

// LocalAddr 获取本地监听地址
func (e *Engine) LocalAddr() string {
	return e.network.LocalAddr()
}

// LocalID 获取本地节点 ID
func (e *Engine) LocalID() string {
	return e.network.LocalID()
}

// OnDataChanged 处理数据变更（DB 回调入口）
// 自动将变更行的 CRDT 字节广播到所有节点。
func (e *Engine) OnDataChanged(tableName string, key uuid.UUID) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.nodeMgr == nil || e.nodeMgr.dataSync == nil {
		return
	}

	// 广播变更行
	if err := e.nodeMgr.dataSync.BroadcastRow(tableName, key); err != nil {
		log.Printf("[Engine:%s] 广播失败: table=%s, key=%s, err=%v",
			e.db.DatabaseID, tableName, key.String()[:8], err)
	}
}

// handleMessage 处理接收到的网络消息
func (e *Engine) handleMessage(peerID string, msg *NetworkMessage) {
	switch msg.Type {
	case MsgTypeHeartbeat:
		e.nodeMgr.OnHeartbeat(peerID, msg.Clock)
		if msg.Clock > 0 {
			e.nodeMgr.UpdateLocalClock(msg.Clock)
		}

	case MsgTypeRawData:
		// 处理增量同步：接收原始 CRDT 字节并 Merge
		if msg.Table != "" && msg.Key != "" && msg.RawData != nil {
			log.Printf("[Engine:%s] 收到增量数据: table=%s, key=%s, from=%s",
				e.db.DatabaseID, msg.Table, msg.Key[:8], peerID[:8])
			err := e.nodeMgr.OnReceiveMerge(msg.Table, msg.Key, msg.RawData, msg.Timestamp)
			if err != nil {
				log.Printf("[Engine:%s] Merge 失败: %v", e.db.DatabaseID, err)
			}
		}

	case MsgTypeFetchRawRequest:
		// 处理全量同步请求
		e.handleFetchRawRequest(peerID, msg)

	case MsgTypeFetchRawResponse:
		// 响应已由 TenantNetwork 的 SendWithResponse 处理

	case MsgTypeVersionDigest:
		// 处理版本摘要
		e.vs.OnReceiveDigest(peerID, msg)
	}
}

// handleFetchRawRequest 处理原始数据获取请求
func (e *Engine) handleFetchRawRequest(peerID string, msg *NetworkMessage) {
	if msg.Table == "" {
		return
	}

	rawRows, err := e.nodeMgr.dataSync.ExportTableRawData(msg.Table)
	if err != nil {
		log.Printf("[Engine:%s] 导出表数据失败: %v", e.db.DatabaseID, err)
		return
	}

	for _, row := range rawRows {
		responseMsg := &NetworkMessage{
			Type:      MsgTypeFetchRawResponse,
			RequestID: msg.RequestID,
			Table:     msg.Table,
			Key:       row.Key,
			RawData:   row.Data,
		}

		if err := e.network.Send(peerID, responseMsg); err != nil {
			log.Printf("[Engine:%s] 发送行数据失败: %v", e.db.DatabaseID, err)
		}
	}
}

// EnableSync 在数据库上启用同步（便捷入口函数）。
// 使用数据库 ID 作为网络频道 ID，实现租户隔离。
func EnableSync(database *db.DB, config db.SyncConfig) (*Engine, error) {
	engine, err := NewEngine(database, config)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		return nil, err
	}

	// 如果指定了连接地址，自动连接
	if config.ConnectTo != "" {
		if err := engine.Connect(config.ConnectTo); err != nil {
			log.Printf("[Engine] 连接到 %s 失败: %v", config.ConnectTo, err)
		}
	}

	return engine, nil
}
