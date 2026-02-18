package sync

func (nm *NodeManager) getNetwork() NetworkInterface {
	nm.networkMu.RLock()
	defer nm.networkMu.RUnlock()
	return nm.network
}

func (nm *NodeManager) setNetwork(network NetworkInterface) {
	nm.networkMu.Lock()
	defer nm.networkMu.Unlock()
	nm.network = network
}
