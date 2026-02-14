package sync

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
)

// Engine orchestrates network/node/version sync components.
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

// NewEngine creates a sync engine.
func NewEngine(database *db.DB, config db.SyncConfig) (*Engine, error) {
	if config.Password == "" {
		return nil, fmt.Errorf("同步密码不能为空")
	}

	tenetConfig := &TenetConfig{
		Password:    config.Password,
		ListenPort:  config.ListenPort,
		EnableDebug: config.Debug,
	}

	tenantID := database.DatabaseID
	network, err := NewTenantNetwork(tenantID, tenetConfig)
	if err != nil {
		return nil, fmt.Errorf("创建网络失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		db:      database,
		config:  tenetConfig,
		network: network,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// Start starts network and node managers.
func (e *Engine) Start(ctx context.Context) error {
	if err := e.network.Start(); err != nil {
		return fmt.Errorf("启动网络失败: %w", err)
	}

	e.nodeMgr = NewNodeManager(e.db, e.network.LocalID())
	e.nodeMgr.RegisterNetwork(e.network)
	e.vs = NewVersionSync(e.db, e.nodeMgr)

	e.network.SetBroadcastHandler(PeerMessageHandler{
		OnReceive: func(peerID string, msg *NetworkMessage) {
			e.handleMessage(peerID, msg)
		},
	})

	e.network.AddPeerConnectedHandler(func(peerID string) {
		log.Printf("[Engine:%s] peer connected: %s", e.db.DatabaseID, shortPeerID(peerID))
		e.nodeMgr.OnHeartbeat(peerID, 0)
		go e.vs.OnPeerConnected(peerID)
	})

	e.network.AddPeerDisconnectedHandler(func(peerID string) {
		log.Printf("[Engine:%s] peer disconnected: %s", e.db.DatabaseID, shortPeerID(peerID))
	})

	e.db.OnChangeDetailed(func(event db.ChangeEvent) {
		go e.OnDataChangedDetailed(event.TableName, event.Key, event.Columns)
	})

	e.nodeMgr.Start(ctx)
	e.db.SetSyncEngine(e)

	log.Printf("[Engine:%s] started: node=%s, addr=%s", e.db.DatabaseID, shortPeerID(e.network.LocalID()), e.network.LocalAddr())
	return nil
}

// Stop stops all components.
func (e *Engine) Stop() {
	log.Printf("[Engine:%s] stopping", e.db.DatabaseID)
	e.cancel()

	if e.nodeMgr != nil {
		e.nodeMgr.Stop()
	}
	if e.network != nil {
		e.network.Stop()
	}
}

// Connect connects to another node.
func (e *Engine) Connect(addr string) error {
	return e.network.Connect(addr)
}

// Peers returns connected peers.
func (e *Engine) Peers() []string {
	return e.network.Peers()
}

// LocalAddr returns local listen address.
func (e *Engine) LocalAddr() string {
	return e.network.LocalAddr()
}

// LocalID returns local node ID.
func (e *Engine) LocalID() string {
	return e.network.LocalID()
}

// OnDataChanged keeps SyncEngine compatibility and does full-row broadcast.
func (e *Engine) OnDataChanged(tableName string, key uuid.UUID) {
	e.OnDataChangedDetailed(tableName, key, nil)
}

// OnDataChangedDetailed broadcasts row changes with optional column granularity.
func (e *Engine) OnDataChangedDetailed(tableName string, key uuid.UUID, columns []string) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.nodeMgr == nil || e.nodeMgr.dataSync == nil {
		return
	}

	if len(columns) > 0 {
		if err := e.nodeMgr.dataSync.BroadcastRowDelta(tableName, key, columns); err != nil {
			log.Printf("[Engine:%s] delta broadcast failed, fallback full row: table=%s, key=%s, cols=%v, err=%v",
				e.db.DatabaseID, tableName, shortPeerID(key.String()), columns, err)
			if fullErr := e.nodeMgr.dataSync.BroadcastRow(tableName, key); fullErr != nil {
				log.Printf("[Engine:%s] fallback full broadcast failed: table=%s, key=%s, err=%v",
					e.db.DatabaseID, tableName, shortPeerID(key.String()), fullErr)
			}
		}
		return
	}

	if err := e.nodeMgr.dataSync.BroadcastRow(tableName, key); err != nil {
		log.Printf("[Engine:%s] full broadcast failed: table=%s, key=%s, err=%v",
			e.db.DatabaseID, tableName, shortPeerID(key.String()), err)
	}
}

// handleMessage handles incoming network messages.
func (e *Engine) handleMessage(peerID string, msg *NetworkMessage) {
	switch msg.Type {
	case MsgTypeHeartbeat:
		e.nodeMgr.OnHeartbeat(peerID, msg.Clock)
		if msg.Clock > 0 {
			e.nodeMgr.UpdateLocalClock(msg.Clock)
		}

	case MsgTypeRawData:
		if msg.Table != "" && msg.Key != "" && msg.RawData != nil {
			log.Printf("[Engine:%s] received full row: table=%s, key=%s, from=%s",
				e.db.DatabaseID, msg.Table, shortPeerID(msg.Key), shortPeerID(peerID))
			if err := e.nodeMgr.OnReceiveMerge(msg.Table, msg.Key, msg.RawData, msg.Timestamp); err != nil {
				log.Printf("[Engine:%s] merge failed: %v", e.db.DatabaseID, err)
			}
		}

	case MsgTypeRawDelta:
		if msg.Table != "" && msg.Key != "" && msg.RawData != nil {
			log.Printf("[Engine:%s] received row delta: table=%s, key=%s, cols=%v, from=%s",
				e.db.DatabaseID, msg.Table, shortPeerID(msg.Key), msg.Columns, shortPeerID(peerID))
			if err := e.nodeMgr.OnReceiveDelta(msg.Table, msg.Key, msg.Columns, msg.RawData, msg.Timestamp); err != nil {
				log.Printf("[Engine:%s] delta merge failed: %v", e.db.DatabaseID, err)
			}
		}

	case MsgTypeFetchRawRequest:
		e.handleFetchRawRequest(peerID, msg)

	case MsgTypeFetchRawResponse:
		// handled by TenantNetwork SendWithResponse

	case MsgTypeVersionDigest:
		e.vs.OnReceiveDigest(peerID, msg)
	}
}

// handleFetchRawRequest replies all rows for the requested table.
func (e *Engine) handleFetchRawRequest(peerID string, msg *NetworkMessage) {
	if msg.Table == "" {
		return
	}

	rawRows, err := e.nodeMgr.dataSync.ExportTableRawData(msg.Table)
	if err != nil {
		log.Printf("[Engine:%s] export raw table failed: %v", e.db.DatabaseID, err)
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
			log.Printf("[Engine:%s] send row failed: %v", e.db.DatabaseID, err)
		}
	}
}

// EnableSync enables sync for one database.
func EnableSync(database *db.DB, config db.SyncConfig) (*Engine, error) {
	engine, err := NewEngine(database, config)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		return nil, err
	}

	if config.ConnectTo != "" {
		if err := engine.Connect(config.ConnectTo); err != nil {
			log.Printf("[Engine] connect %s failed: %v", config.ConnectTo, err)
		}
	}

	return engine, nil
}
