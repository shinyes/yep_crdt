package sync

import (
	"context"
	"sync"

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
	chunks   *localFileChunkReceiver

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
