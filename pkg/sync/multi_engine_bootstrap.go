package sync

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync/atomic"

	"github.com/shinyes/yep_crdt/pkg/db"
)

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
			chunks:   newLocalFileChunkReceiver(),
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
