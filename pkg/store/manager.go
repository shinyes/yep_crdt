package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// MultiStore 管理多个 Store 实例，每个租户一个。
type MultiStore struct {
	rootPath string
	mu       sync.RWMutex
	stores   map[string]*BadgerStore
}

// NewMultiStore 创建一个新的 MultiStore 管理器。
// rootPath 是存储所有租户数据库的目录。
func NewMultiStore(rootPath string) *MultiStore {
	return &MultiStore{
		rootPath: rootPath,
		stores:   make(map[string]*BadgerStore),
	}
}

// Get 返回给定租户的 Store。
// 如果存储尚未打开，它将打开存储。
func (m *MultiStore) Get(tenantID string) (Store, error) {
	m.mu.RLock()
	s, ok := m.stores[tenantID]
	m.mu.RUnlock()
	if ok {
		return s, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 双重检查
	if s, ok := m.stores[tenantID]; ok {
		return s, nil
	}

	// 为租户创建目录
	dbPath := filepath.Join(m.rootPath, tenantID)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create tenant directory: %w", err)
	}

	// 打开存储
	store, err := NewBadgerStore(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open tenant store: %w", err)
	}

	m.stores[tenantID] = store
	return store, nil
}

// Close 关闭给定租户的存储。
func (m *MultiStore) Close(tenantID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.stores[tenantID]
	if !ok {
		return nil
	}

	delete(m.stores, tenantID)
	return s.Close()
}

// CloseAll 关闭所有打开的存储。
func (m *MultiStore) CloseAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for id, s := range m.stores {
		if err := s.Close(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
		delete(m.stores, id)
	}
	return firstErr
}
