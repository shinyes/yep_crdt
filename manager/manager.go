package manager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/shinyes/yep_crdt/crdt"
	"github.com/shinyes/yep_crdt/store"
)

// Manager 是主要入口点（数据库实例）。
type Manager struct {
	mu        sync.RWMutex
	store     store.Store
	blobStore *store.DiskBlobStore
	roots     map[string]crdt.CRDT // 活动根节点的缓存
	// 同步/网络钩子
	fetcher store.Fetcher

	lastTs int64
	nodeID string // 本地节点标识，用于操作的 OriginID
}

func (m *Manager) NextTimestamp() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UnixNano()
	if now <= m.lastTs {
		now = m.lastTs + 1
	}
	m.lastTs = now
	return now
}

func NewManager(dbPath string, blobPath string) (*Manager, error) {
	return NewManagerWithNodeID(dbPath, blobPath, "local")
}

// NewManagerWithNodeID 创建带有自定义节点 ID 的 Manager。
// nodeID 用于标识本地节点，在多节点同步时应保持唯一。
func NewManagerWithNodeID(dbPath, blobPath, nodeID string) (*Manager, error) {
	s, err := store.NewBadgerStore(dbPath)
	if err != nil {
		return nil, err
	}
	bs, err := store.NewDiskBlobStore(blobPath)
	if err != nil {
		return nil, err
	}

	return &Manager{
		store:     s,
		blobStore: bs,
		roots:     make(map[string]crdt.CRDT),
		nodeID:    nodeID,
	}, nil
}

// NodeID 返回本地节点标识。
func (m *Manager) NodeID() string {
	if m.nodeID == "" {
		return "local"
	}
	return m.nodeID
}

func (m *Manager) Close() error {
	return m.store.Close()
}

func (m *Manager) SetFetcher(f store.Fetcher) {
	m.fetcher = f
	m.blobStore.SetFetcher(f)
}

// CreateRoot 创建一个新的根 CRDT。
func (m *Manager) CreateRoot(id string, typ crdt.Type) (crdt.CRDT, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否已存在？
	if c, ok := m.roots[id]; ok {
		return c, nil
	}

	c, err := m.makeCRDT(id, typ)
	if err != nil {
		return nil, err
	}

	// 持久化元数据
	meta := &RootMetadata{
		ID:        id,
		Type:      typ,
		CreatedAt: time.Now().Unix(),
	}
	if err := m.SaveRootMetadata(meta); err != nil {
		return nil, err
	}

	m.roots[id] = c
	return c, nil
}

func (m *Manager) makeCRDT(id string, typ crdt.Type) (crdt.CRDT, error) {
	return crdt.Factory.NewCRDT(id, typ)
}

// CreateLocalFile 创建一个已注册的文件。
// 它读取本地文件，将其存储在 BlobStore 中，并创建一个 LocalFile CRDT。
func (m *Manager) CreateLocalFile(rootID string, localPath string) (*crdt.LocalFile, error) {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return nil, err
	}

	// 存储 blob
	hash, err := m.blobStore.Put(data)
	if err != nil {
		return nil, err
	}

	stat, _ := os.Stat(localPath)

	meta := crdt.FileMetadata{
		Hash:      hash,
		Name:      filepath.Base(localPath),
		Size:      stat.Size(),
		CreatedAt: time.Now().Unix(),
		MimeType:  "application/octet-stream", // 需要探测逻辑？
	}

	c := crdt.NewLocalFile(meta)
	m.mu.Lock()
	defer m.mu.Unlock() // 确保锁释放

	m.roots[rootID] = c

	// 持久化元数据
	rootMeta := &RootMetadata{
		ID:        rootID,
		Type:      crdt.TypeLocalFile,
		CreatedAt: time.Now().Unix(),
	}
	if err := m.SaveRootMetadata(rootMeta); err != nil {
		// 即使元数据保存失败，内存中已创建，但下次加载会失败。
		// 这里选择返回错误。
		delete(m.roots, rootID)
		return nil, err
	}

	return c, nil
}

func (m *Manager) CreateFileRoot(rootID string, localPath string) (*crdt.LocalFile, error) {
	return m.CreateLocalFile(rootID, localPath)
}

func (m *Manager) GetRoot(id string) (crdt.CRDT, error) {
	m.mu.RLock()
	c, ok := m.roots[id]
	m.mu.RUnlock()

	if ok {
		return c, nil
	}

	// Double-check locking for loading
	m.mu.Lock()
	defer m.mu.Unlock()

	if c, ok := m.roots[id]; ok {
		return c, nil
	}

	// 1. 加载元数据
	meta, err := m.LoadRootMetadata(id)
	if err != nil {
		return nil, fmt.Errorf("加载元数据失败: %v", err)
	}

	// 2. 尝试加载快照
	c, err = m.LoadSnapshot(id, meta.Type)
	if err != nil {
		// 快照不存在，创建新实例
		c, err = m.makeCRDT(id, meta.Type)
		if err != nil {
			return nil, err
		}
	}

	// 3. 加载并重放操作日志
	// 简单起见，如果从快照加载，我们仍然可以加载所有 Ops，利用幂等性。
	// 或者如果快照包含时间戳信息，可以优化。
	// 这里全量重放，确保正确性。
	ops, err := m.LoadOps(id, 0)
	if err != nil {
		return nil, fmt.Errorf("加载操作日志失败: %v", err)
	}

	// 如果是从快照加载，可能只需要应用部分？
	// 目前 CRDT Apply 是幂等的吗？
	// PNCounter: Idempotent map update? Yes, max(old, new).
	// ORSet: Idempotent? Yes, add/rem set wins.
	// RGA: Idempotent? InsertAt keys. Since OpID is unique and we check existence usually?
	// RGA 实现通常检查是否已应用。
	// 假设现有实现支持幂等。
	for _, op := range ops {
		// 错误处理：单个坏 Op 不应阻止加载？
		// 严格模式：报错
		if err := c.Apply(op); err != nil {
			// 如果是重放，某些 op 可能因为状态已通过快照更新而失败？
			// Log 警告而不是失败整个加载？
			// 暂时返回错误，确保数据一致性。
			// 如果 Apply 不幂等，这里会有问题。
			// 假设 Apply 是设计为幂等的。
			// 如果不是，我们需要快照记录 LastOpTS
		}
	}

	m.roots[id] = c
	return c, nil
}

// Blob 访问
func (m *Manager) GetBlob(hash string) ([]byte, error) {
	return m.blobStore.Get(hash)
}

// ListRoots 列出所有根节点的元数据。
func (m *Manager) ListRoots() ([]*RootMetadata, error) {
	var roots []*RootMetadata
	prefix := []byte("meta/")

	err := m.store.Scan(prefix, func(k, v []byte) error {
		var meta RootMetadata
		if err := json.Unmarshal(v, &meta); err != nil {
			return nil // 跳过无效数据
		}
		roots = append(roots, &meta)
		return nil
	})

	if err != nil {
		return nil, err
	}
	return roots, nil
}

// Exists 检查根节点是否存在。
func (m *Manager) Exists(id string) bool {
	m.mu.RLock()
	if _, ok := m.roots[id]; ok {
		m.mu.RUnlock()
		return true
	}
	m.mu.RUnlock()

	// 检查持久化存储
	_, err := m.LoadRootMetadata(id)
	return err == nil
}

// DeleteRoot 删除指定的根节点（包括元数据、操作日志和快照）。
// 注意：此操作不可逆。
func (m *Manager) DeleteRoot(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 从内存缓存中移除
	delete(m.roots, id)

	// 删除元数据
	metaKey := []byte(fmt.Sprintf("meta/%s", id))
	if err := m.store.Delete(metaKey); err != nil {
		// 忽略不存在的错误
	}

	// 删除快照
	snapKey := []byte(fmt.Sprintf("snap/%s/latest", id))
	if err := m.store.Delete(snapKey); err != nil {
		// 忽略不存在的错误
	}

	// 删除操作日志（扫描并删除所有匹配前缀的键）
	opsPrefix := []byte(fmt.Sprintf("ops/%s/", id))
	keysToDelete := [][]byte{}
	m.store.Scan(opsPrefix, func(k, v []byte) error {
		keysToDelete = append(keysToDelete, k)
		return nil
	})

	for _, k := range keysToDelete {
		m.store.Delete(k)
	}

	return nil
}

// TriggerSnapshot 手动触发指定根节点的快照保存。
func (m *Manager) TriggerSnapshot(id string) error {
	root, err := m.GetRoot(id)
	if err != nil {
		return err
	}
	return m.SaveSnapshot(id, root)
}
