package sync

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
)

// MultiEngine orchestrates sync for multiple tenant databases on one shared tunnel.
type MultiEngine struct {
	mu      sync.RWMutex
	network *TenantNetwork
	tenants map[string]*tenantRuntime
}

const engineChangeQueueSize = 1024

type engineStats struct {
	changeEnqueued     uint64
	changeProcessed    uint64
	changeBackpressure uint64
}

// EngineStats is a snapshot of sync runtime counters.
type EngineStats struct {
	ChangeEnqueued     uint64
	ChangeProcessed    uint64
	ChangeBackpressure uint64
	ChangeQueueDepth   int
	Network            TenantNetworkStats
}

type tenantRuntime struct {
	tenantID string
	db       *db.DB
	network  NetworkInterface
	nodeMgr  *NodeManager
	vs       *VersionSync

	ctx      context.Context
	cancel   context.CancelFunc
	changeQ  chan db.ChangeEvent
	workerWg sync.WaitGroup
	stats    engineStats
}

type tenantScopedNetwork struct {
	tenantID string
	network  *TenantNetwork
}

// EnableMultiTenantSync enables sync for multiple tenant databases on a single transport.
func EnableMultiTenantSync(databases []*db.DB, config db.SyncConfig, nodeOpts ...Option) (*MultiEngine, error) {
	if config.Password == "" {
		return nil, fmt.Errorf("sync password cannot be empty")
	}
	if len(databases) == 0 {
		return nil, fmt.Errorf("no tenant databases provided")
	}

	tenantDBs := make(map[string]*db.DB, len(databases))
	tenantIDs := make([]string, 0, len(databases))
	for _, database := range databases {
		if database == nil {
			return nil, fmt.Errorf("database is nil")
		}
		tenantID := database.DatabaseID
		if tenantID == "" {
			return nil, fmt.Errorf("database has empty tenant id")
		}
		if _, exists := tenantDBs[tenantID]; exists {
			return nil, fmt.Errorf("duplicate tenant id: %s", tenantID)
		}
		tenantDBs[tenantID] = database
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)

	tenetConfig := &TenetConfig{
		Password:     config.Password,
		ListenPort:   config.ListenPort,
		EnableDebug:  config.Debug,
		IdentityPath: config.IdentityPath,
		ChannelIDs:   tenantIDs,
	}

	network, err := NewTenantNetwork(tenantIDs[0], tenetConfig)
	if err != nil {
		return nil, fmt.Errorf("create shared network failed: %w", err)
	}
	if err := network.Start(); err != nil {
		return nil, fmt.Errorf("start shared network failed: %w", err)
	}

	engine := &MultiEngine{
		network: network,
		tenants: make(map[string]*tenantRuntime, len(tenantIDs)),
	}

	for _, tenantID := range tenantIDs {
		database := tenantDBs[tenantID]
		tenantCtx, tenantCancel := context.WithCancel(context.Background())
		scopedNetwork := &tenantScopedNetwork{
			tenantID: tenantID,
			network:  network,
		}
		nodeMgr := NewNodeManager(database, network.LocalID(), nodeOpts...)
		nodeMgr.RegisterNetwork(scopedNetwork)

		rt := &tenantRuntime{
			tenantID: tenantID,
			db:       database,
			network:  scopedNetwork,
			nodeMgr:  nodeMgr,
			ctx:      tenantCtx,
			cancel:   tenantCancel,
			changeQ:  make(chan db.ChangeEvent, engineChangeQueueSize),
		}
		rt.vs = NewVersionSync(database, nodeMgr)
		engine.tenants[tenantID] = rt

		runtime := rt
		network.SetTenantBroadcastHandler(tenantID, PeerMessageHandler{
			OnReceive: func(peerID string, msg NetworkMessage) {
				runtime.handleMessage(peerID, msg)
			},
		})

		database.OnChangeDetailed(func(event db.ChangeEvent) {
			if runtime.ctx.Err() != nil {
				return
			}

			select {
			case runtime.changeQ <- event:
				atomic.AddUint64(&runtime.stats.changeEnqueued, 1)
			default:
				// Apply backpressure on local writes when queue is saturated.
				atomic.AddUint64(&runtime.stats.changeBackpressure, 1)
				log.Printf("[MultiEngine:%s] change queue saturated, applying backpressure: table=%s, key=%s",
					runtime.tenantID, event.TableName, shortPeerID(event.Key.String()))
				runtime.onDataChangedDetailed(event.TableName, event.Key, event.Columns)
				atomic.AddUint64(&runtime.stats.changeProcessed, 1)
			}
		})

		runtime.workerWg.Add(1)
		go runtime.runChangeWorker()
		runtime.nodeMgr.Start(runtime.ctx)
	}

	network.AddPeerConnectedHandler(engine.onPeerConnected)
	network.AddPeerDisconnectedHandler(engine.onPeerDisconnected)

	if config.ConnectTo != "" {
		if err := network.Connect(config.ConnectTo); err != nil {
			log.Printf("[MultiEngine] connect %s failed: %v", config.ConnectTo, err)
		}
	}

	log.Printf("[MultiEngine] started: node=%s, addr=%s, tenants=%v",
		shortPeerID(network.LocalID()), network.LocalAddr(), tenantIDs)
	return engine, nil
}

func (m *MultiEngine) onPeerConnected(peerID string) {
	runtimes := m.snapshotRuntimes()
	for _, rt := range runtimes {
		log.Printf("[MultiEngine:%s] peer connected: %s", rt.tenantID, shortPeerID(peerID))
		rt.nodeMgr.OnPeerConnected(peerID)
		go rt.vs.OnPeerConnected(peerID)
	}
}

func (m *MultiEngine) onPeerDisconnected(peerID string) {
	runtimes := m.snapshotRuntimes()
	for _, rt := range runtimes {
		log.Printf("[MultiEngine:%s] peer disconnected: %s", rt.tenantID, shortPeerID(peerID))
		rt.nodeMgr.OnPeerDisconnected(peerID)
	}
}

func (m *MultiEngine) snapshotRuntimes() []*tenantRuntime {
	m.mu.RLock()
	defer m.mu.RUnlock()

	runtimes := make([]*tenantRuntime, 0, len(m.tenants))
	for _, rt := range m.tenants {
		runtimes = append(runtimes, rt)
	}
	return runtimes
}

// Stop stops all tenant runtimes and the shared transport.
func (m *MultiEngine) Stop() {
	runtimes := m.snapshotRuntimes()
	for _, rt := range runtimes {
		rt.cancel()
	}
	for _, rt := range runtimes {
		rt.workerWg.Wait()
		rt.nodeMgr.Stop()
	}
	if m.network != nil {
		m.network.Stop()
	}
}

// Connect connects shared transport to a remote node.
func (m *MultiEngine) Connect(addr string) error {
	if m.network == nil {
		return fmt.Errorf("network not started")
	}
	return m.network.Connect(addr)
}

// Peers returns connected peers on shared transport.
func (m *MultiEngine) Peers() []string {
	if m.network == nil {
		return nil
	}
	return m.network.Peers()
}

// LocalAddr returns shared local listen address.
func (m *MultiEngine) LocalAddr() string {
	if m.network == nil {
		return ""
	}
	return m.network.LocalAddr()
}

// LocalID returns shared local node ID.
func (m *MultiEngine) LocalID() string {
	if m.network == nil {
		return ""
	}
	return m.network.LocalID()
}

// TenantIDs returns all started tenant IDs.
func (m *MultiEngine) TenantIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tenantIDs := make([]string, 0, len(m.tenants))
	for tenantID := range m.tenants {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)
	return tenantIDs
}

// TenantDatabase returns one tenant database if started.
func (m *MultiEngine) TenantDatabase(tenantID string) (*db.DB, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rt, exists := m.tenants[tenantID]
	if !exists || rt == nil || rt.db == nil {
		return nil, false
	}
	return rt.db, true
}

// TenantStats returns one tenant's runtime metrics.
func (m *MultiEngine) TenantStats(tenantID string) (EngineStats, bool) {
	m.mu.RLock()
	rt, exists := m.tenants[tenantID]
	network := m.network
	m.mu.RUnlock()
	if !exists {
		return EngineStats{}, false
	}

	stats := EngineStats{
		ChangeEnqueued:     atomic.LoadUint64(&rt.stats.changeEnqueued),
		ChangeProcessed:    atomic.LoadUint64(&rt.stats.changeProcessed),
		ChangeBackpressure: atomic.LoadUint64(&rt.stats.changeBackpressure),
	}
	if rt.changeQ != nil {
		stats.ChangeQueueDepth = len(rt.changeQ)
	}
	if network != nil {
		stats.Network = network.Stats()
	}
	return stats, true
}

func (rt *tenantRuntime) runChangeWorker() {
	defer rt.workerWg.Done()

	for {
		select {
		case <-rt.ctx.Done():
			return
		case event := <-rt.changeQ:
			rt.onDataChangedDetailed(event.TableName, event.Key, event.Columns)
			atomic.AddUint64(&rt.stats.changeProcessed, 1)
		}
	}
}

func (rt *tenantRuntime) onDataChangedDetailed(tableName string, key uuid.UUID, columns []string) {
	if rt.nodeMgr == nil || rt.nodeMgr.dataSync == nil {
		return
	}

	peers := rt.nodeMgr.GetOnlineNodes()
	if len(peers) == 0 {
		return
	}

	for _, peerID := range peers {
		if peerID == "" || peerID == rt.nodeMgr.GetLocalNodeID() {
			continue
		}
		if !rt.nodeMgr.CanUseIncrementalWithPeer(peerID) {
			log.Printf("[MultiEngine:%s] skip incremental broadcast to blocked peer: peer=%s, table=%s, key=%s",
				rt.tenantID, shortPeerID(peerID), tableName, shortPeerID(key.String()))
			continue
		}

		if len(columns) > 0 {
			if err := rt.nodeMgr.dataSync.SendRowDeltaToPeer(peerID, tableName, key, columns); err != nil {
				log.Printf("[MultiEngine:%s] delta send failed, fallback full row: peer=%s, table=%s, key=%s, cols=%v, err=%v",
					rt.tenantID, shortPeerID(peerID), tableName, shortPeerID(key.String()), columns, err)
				if fullErr := rt.nodeMgr.dataSync.SendRowToPeer(peerID, tableName, key); fullErr != nil {
					log.Printf("[MultiEngine:%s] fallback full send failed: peer=%s, table=%s, key=%s, err=%v",
						rt.tenantID, shortPeerID(peerID), tableName, shortPeerID(key.String()), fullErr)
				}
			}
			continue
		}

		if err := rt.nodeMgr.dataSync.SendRowToPeer(peerID, tableName, key); err != nil {
			log.Printf("[MultiEngine:%s] full row send failed: peer=%s, table=%s, key=%s, err=%v",
				rt.tenantID, shortPeerID(peerID), tableName, shortPeerID(key.String()), err)
		}
	}
}

func (rt *tenantRuntime) handleMessage(peerID string, msg NetworkMessage) {
	if msg.Type != MsgTypeHeartbeat {
		rt.nodeMgr.MarkPeerSeen(peerID)
		if msg.GCFloor > 0 {
			rt.nodeMgr.ObservePeerGCFloor(peerID, msg.GCFloor)
		}
	}

	switch msg.Type {
	case MsgTypeHeartbeat:
		rt.nodeMgr.OnHeartbeat(peerID, msg.Clock, msg.GCFloor)

	case MsgTypeRawData:
		if !rt.nodeMgr.CanUseIncrementalWithPeer(peerID) {
			log.Printf("[MultiEngine:%s] skip raw row from blocked incremental peer: from=%s",
				rt.tenantID, shortPeerID(peerID))
			return
		}
		if msg.Table != "" && msg.Key != "" && msg.RawData != nil {
			log.Printf("[MultiEngine:%s] received full row: table=%s, key=%s, from=%s",
				rt.tenantID, msg.Table, shortPeerID(msg.Key), shortPeerID(peerID))
			if err := rt.nodeMgr.OnReceiveMerge(msg.Table, msg.Key, msg.RawData, msg.Timestamp); err != nil {
				log.Printf("[MultiEngine:%s] merge failed: %v", rt.tenantID, err)
			}
		}

	case MsgTypeRawDelta:
		if !rt.nodeMgr.CanUseIncrementalWithPeer(peerID) {
			log.Printf("[MultiEngine:%s] skip raw delta from blocked incremental peer: from=%s",
				rt.tenantID, shortPeerID(peerID))
			return
		}
		if msg.Table != "" && msg.Key != "" && msg.RawData != nil {
			log.Printf("[MultiEngine:%s] received row delta: table=%s, key=%s, cols=%v, from=%s",
				rt.tenantID, msg.Table, shortPeerID(msg.Key), msg.Columns, shortPeerID(peerID))
			if err := rt.nodeMgr.OnReceiveDelta(msg.Table, msg.Key, msg.Columns, msg.RawData, msg.Timestamp); err != nil {
				log.Printf("[MultiEngine:%s] delta merge failed: %v", rt.tenantID, err)
			}
		}

	case MsgTypeFetchRawRequest:
		rt.handleFetchRawRequest(peerID, msg)

	case MsgTypeFetchRawResponse:
		// handled by TenantNetwork request waiter

	case MsgTypeVersionDigest:
		rt.vs.OnReceiveDigest(peerID, &msg)

	case MsgTypeGCPrepare:
		rt.nodeMgr.HandleManualGCPrepare(peerID, msg)

	case MsgTypeGCCommit:
		rt.nodeMgr.HandleManualGCCommit(peerID, msg)

	case MsgTypeGCExecute:
		rt.nodeMgr.HandleManualGCExecute(peerID, msg)

	case MsgTypeGCAbort:
		rt.nodeMgr.HandleManualGCAbort(peerID, msg)
	}
}

func (rt *tenantRuntime) handleFetchRawRequest(peerID string, msg NetworkMessage) {
	if msg.Table == "" {
		return
	}

	rawRows, err := rt.nodeMgr.dataSync.ExportTableRawData(msg.Table)
	if err != nil {
		log.Printf("[MultiEngine:%s] export raw table failed: %v", rt.tenantID, err)
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
		if err := rt.network.SendMessage(peerID, responseMsg); err != nil {
			log.Printf("[MultiEngine:%s] send row failed: %v", rt.tenantID, err)
		}
	}

	doneMsg := &NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: msg.RequestID,
		Table:     msg.Table,
		Key:       fetchRawResponseDoneKey,
	}
	if err := rt.network.SendMessage(peerID, doneMsg); err != nil {
		log.Printf("[MultiEngine:%s] send fetch done marker failed: %v", rt.tenantID, err)
	}
}

func (n *tenantScopedNetwork) SendHeartbeat(targetNodeID string, clock int64) error {
	return n.SendMessage(targetNodeID, &NetworkMessage{
		Type:  MsgTypeHeartbeat,
		Clock: clock,
	})
}

func (n *tenantScopedNetwork) BroadcastHeartbeat(clock int64) error {
	_, err := n.broadcastValue(NetworkMessage{
		Type:      MsgTypeHeartbeat,
		Clock:     clock,
		Timestamp: clock,
	})
	return err
}

func (n *tenantScopedNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	return n.SendMessage(targetNodeID, &NetworkMessage{
		Type:      MsgTypeRawData,
		Table:     table,
		Key:       key,
		RawData:   rawData,
		Timestamp: timestamp,
	})
}

func (n *tenantScopedNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	_, err := n.broadcastValue(NetworkMessage{
		Type:      MsgTypeRawData,
		Table:     table,
		Key:       key,
		RawData:   rawData,
		Timestamp: timestamp,
	})
	return err
}

func (n *tenantScopedNetwork) SendRawDelta(targetNodeID string, table string, key string, columns []string, rawData []byte, timestamp int64) error {
	return n.SendMessage(targetNodeID, &NetworkMessage{
		Type:      MsgTypeRawDelta,
		Table:     table,
		Key:       key,
		Columns:   append([]string(nil), columns...),
		RawData:   rawData,
		Timestamp: timestamp,
	})
}

func (n *tenantScopedNetwork) BroadcastRawDelta(table string, key string, columns []string, rawData []byte, timestamp int64) error {
	_, err := n.broadcastValue(NetworkMessage{
		Type:      MsgTypeRawDelta,
		Table:     table,
		Key:       key,
		Columns:   append([]string(nil), columns...),
		RawData:   rawData,
		Timestamp: timestamp,
	})
	return err
}

func (n *tenantScopedNetwork) SendMessage(targetNodeID string, msg *NetworkMessage) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	cloned := *msg
	cloned.TenantID = n.tenantID
	return n.network.Send(targetNodeID, &cloned)
}

func (n *tenantScopedNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	return n.network.fetchRawTableDataWithTenant(sourceNodeID, tableName, n.tenantID, 30*time.Second)
}

func (n *tenantScopedNetwork) broadcastValue(msg NetworkMessage) (int, error) {
	msg.TenantID = n.tenantID
	return n.network.Broadcast(&msg)
}

var _ NetworkInterface = (*tenantScopedNetwork)(nil)
