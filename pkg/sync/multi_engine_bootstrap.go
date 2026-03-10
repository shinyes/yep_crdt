package sync

import (
	"fmt"
	"log"
	"sort"

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
		Password:            config.Password,
		ListenPort:          config.ListenPort,
		RelayNodes:          append([]string(nil), config.RelayNodes...),
		EnableHolePunch:     config.EnableHolePunch,
		EnableRelay:         config.EnableRelay,
		EnableReconnect:     config.EnableReconnect,
		ReconnectMaxRetries: config.ReconnectMaxRetries,
		DialTimeout:         config.DialTimeout,
		EnableDebug:         config.Debug,
		IdentityPath:        config.IdentityPath,
		ChannelIDs:          tenantIDs,
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
		nodeOpts: append([]Option(nil), nodeOpts...),
	}

	for _, tenantID := range tenantIDs {
		database := tenantDBs[tenantID]
		rt, err := engine.addTenantRuntime(database, false)
		if err != nil {
			engine.Stop()
			return nil, err
		}
		engine.startTenantRuntime(rt, false)
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
