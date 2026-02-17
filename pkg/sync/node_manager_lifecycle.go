package sync

import (
	"context"
	"log"
)

// Start starts background components.
func (nm *NodeManager) Start(ctx context.Context) {
	log.Printf("node manager starting: local=%s", nm.localNodeID)
	nm.heartbeat.Start(ctx)
	log.Printf("node manager started: local=%s", nm.localNodeID)
}

// Stop stops background components.
func (nm *NodeManager) Stop() {
	log.Printf("node manager stopping: local=%s", nm.localNodeID)
	nm.heartbeat.Stop()
}

// RegisterNetwork binds transport implementation.
func (nm *NodeManager) RegisterNetwork(network NetworkInterface) {
	nm.network = network
	nm.dataSync.SetNetwork(network)
	log.Println("network interface registered")
}

// GetLocalNodeID returns local node ID.
func (nm *NodeManager) GetLocalNodeID() string {
	return nm.localNodeID
}
