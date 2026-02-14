package sync

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
)

// Engine orchestrates network/node/version sync components.
type Engine struct {
	mu       sync.RWMutex
	db       *db.DB
	config   *TenetConfig
	network  *TenantNetwork
	nodeMgr  *NodeManager
	vs       *VersionSync
	ctx      context.Context
	cancel   context.CancelFunc
	changeQ  chan db.ChangeEvent
	workerWg sync.WaitGroup
	stats    engineStats
}

const engineChangeQueueSize = 1024

type engineStats struct {
	changeEnqueued     uint64
	changeProcessed    uint64
	changeBackpressure uint64
}

// EngineStats is a snapshot of sync-engine runtime counters.
type EngineStats struct {
	ChangeEnqueued     uint64
	ChangeProcessed    uint64
	ChangeBackpressure uint64
	ChangeQueueDepth   int
	Network            TenantNetworkStats
}

// NewEngine creates a sync engine.
func NewEngine(database *db.DB, config db.SyncConfig) (*Engine, error) {
	if config.Password == "" {
		return nil, fmt.Errorf("同步密码不能为空")
	}

	tenetConfig := &TenetConfig{
		Password:     config.Password,
		ListenPort:   config.ListenPort,
		EnableDebug:  config.Debug,
		IdentityPath: config.IdentityPath,
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
		OnReceive: func(peerID string, msg NetworkMessage) {
			e.handleMessage(peerID, msg)
		},
	})

	e.network.AddPeerConnectedHandler(func(peerID string) {
		log.Printf("[Engine:%s] peer connected: %s", e.db.DatabaseID, shortPeerID(peerID))
		e.nodeMgr.OnPeerConnected(peerID)
		go e.vs.OnPeerConnected(peerID)
	})

	e.network.AddPeerDisconnectedHandler(func(peerID string) {
		log.Printf("[Engine:%s] peer disconnected: %s", e.db.DatabaseID, shortPeerID(peerID))
		e.nodeMgr.OnPeerDisconnected(peerID)
	})

	e.changeQ = make(chan db.ChangeEvent, engineChangeQueueSize)
	e.workerWg.Add(1)
	go e.runChangeWorker()

	e.db.OnChangeDetailed(func(event db.ChangeEvent) {
		if e.ctx.Err() != nil {
			return
		}

		select {
		case e.changeQ <- event:
			atomic.AddUint64(&e.stats.changeEnqueued, 1)
		default:
			// Apply backpressure on the writer path when queue is saturated.
			atomic.AddUint64(&e.stats.changeBackpressure, 1)
			log.Printf("[Engine:%s] change queue saturated, applying backpressure: table=%s, key=%s",
				e.db.DatabaseID, event.TableName, shortPeerID(event.Key.String()))
			e.OnDataChangedDetailed(event.TableName, event.Key, event.Columns)
			atomic.AddUint64(&e.stats.changeProcessed, 1)
		}
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
	e.workerWg.Wait()

	if e.nodeMgr != nil {
		e.nodeMgr.Stop()
	}
	if e.network != nil {
		e.network.Stop()
	}
}

func (e *Engine) runChangeWorker() {
	defer e.workerWg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return
		case event := <-e.changeQ:
			e.OnDataChangedDetailed(event.TableName, event.Key, event.Columns)
			atomic.AddUint64(&e.stats.changeProcessed, 1)
		}
	}
}

// Stats returns runtime metrics for the sync engine.
func (e *Engine) Stats() EngineStats {
	s := EngineStats{
		ChangeEnqueued:     atomic.LoadUint64(&e.stats.changeEnqueued),
		ChangeProcessed:    atomic.LoadUint64(&e.stats.changeProcessed),
		ChangeBackpressure: atomic.LoadUint64(&e.stats.changeBackpressure),
	}
	if e.changeQ != nil {
		s.ChangeQueueDepth = len(e.changeQ)
	}
	if e.network != nil {
		s.Network = e.network.Stats()
	}
	return s
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
func (e *Engine) handleMessage(peerID string, msg NetworkMessage) {
	if msg.Type != MsgTypeHeartbeat {
		e.nodeMgr.MarkPeerSeen(peerID)
	}

	switch msg.Type {
	case MsgTypeHeartbeat:
		e.nodeMgr.OnHeartbeat(peerID, msg.Clock)

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
		// handled by TenantNetwork request waiter

	case MsgTypeVersionDigest:
		e.vs.OnReceiveDigest(peerID, &msg)
	}
}

// handleFetchRawRequest replies all rows for the requested table.
func (e *Engine) handleFetchRawRequest(peerID string, msg NetworkMessage) {
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

	doneMsg := &NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: msg.RequestID,
		Table:     msg.Table,
		Key:       fetchRawResponseDoneKey,
	}
	if err := e.network.Send(peerID, doneMsg); err != nil {
		log.Printf("[Engine:%s] send fetch done marker failed: %v", e.db.DatabaseID, err)
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
