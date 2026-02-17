package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/shinyes/tenet/api"
	tenetlog "github.com/shinyes/tenet/log"
)

// NewTenantNetwork creates a tenet network for one tenant.
func NewTenantNetwork(tenantID string, config *TenetConfig) (*TenantNetwork, error) {
	if config == nil || config.Password == "" {
		return nil, fmt.Errorf("password is required")
	}

	cfg := normalizeTenetConfig(config)

	opts := []api.Option{
		api.WithPassword(cfg.Password),
		api.WithListenPort(cfg.ListenPort),
	}
	for _, channelID := range uniqueChannelIDs(tenantID, cfg.ChannelIDs) {
		opts = append(opts, api.WithChannelID(channelID))
	}

	if cfg.IdentityPath != "" {
		identityJSON, err := os.ReadFile(cfg.IdentityPath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("read identity file failed: %w", err)
		}
		if len(identityJSON) > 0 {
			opts = append(opts, api.WithIdentityJSON(identityJSON))
		}
	}

	if len(cfg.RelayNodes) > 0 {
		opts = append(opts, api.WithRelayNodes(cfg.RelayNodes))
	}

	if cfg.EnableDebug {
		logger := tenetlog.NewStdLogger(
			tenetlog.WithLevel(tenetlog.LevelDebug),
			tenetlog.WithPrefix(fmt.Sprintf("[tenet:%s]", tenantID)),
		)
		opts = append(opts, api.WithLogger(logger))
	} else {
		opts = append(opts, api.WithLogger(tenetlog.Nop()))
	}

	tunnel, err := api.NewTunnel(opts...)
	if err != nil {
		return nil, fmt.Errorf("create tunnel failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	tn := &TenantNetwork{
		tenantID:         tenantID,
		config:           &cfg,
		tunnel:           tunnel,
		ctx:              ctx,
		cancel:           cancel,
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		tenantHandlers:   make(map[string]PeerMessageHandler),
		responseChannels: make(map[string]pendingResponse),
	}
	if localID := tunnel.LocalID(); localID != "" {
		tn.localNodeID.Store(localID)
	}

	if err := persistIdentityJSON(cfg.IdentityPath, tunnel); err != nil {
		return nil, err
	}

	tn.setupCallbacks()
	return tn, nil
}

func uniqueChannelIDs(primary string, channels []string) []string {
	seen := make(map[string]struct{}, len(channels)+1)
	result := make([]string, 0, len(channels)+1)

	add := func(ch string) {
		if ch == "" {
			return
		}
		if _, exists := seen[ch]; exists {
			return
		}
		seen[ch] = struct{}{}
		result = append(result, ch)
	}

	add(primary)
	for _, ch := range channels {
		add(ch)
	}
	return result
}

func persistIdentityJSON(path string, tunnel *api.Tunnel) error {
	if path == "" || tunnel == nil {
		return nil
	}

	identityJSON, err := tunnel.GetIdentityJSON()
	if err != nil {
		return fmt.Errorf("export identity failed: %w", err)
	}
	if len(identityJSON) == 0 {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create identity dir failed: %w", err)
	}
	if err := os.WriteFile(path, identityJSON, 0o600); err != nil {
		return fmt.Errorf("write identity file failed: %w", err)
	}
	return nil
}
