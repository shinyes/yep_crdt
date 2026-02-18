package crdt

import "sync"

// MapCRDT 实现列 -> CRDT 的映射。
// 这是 "行" 容器。
type MapCRDT struct {
	mu      sync.RWMutex
	Entries map[string]*Entry
	// cache 存储已反序列化的 CRDT 对象。
	// 这是一个回写缓存 (Write) : 产生的变更首先在 cache 中更新，
	// 只有在调用 Bytes() 时才序列化回 Entries。
	cache   map[string]CRDT
	baseDir string // 文件存储的基础目录

	// 内存泄漏防护：最大缓存大小
	maxCacheSize int
	lruKeys      []string       // 简单的 LRU 键跟踪
	lruIndex     map[string]int // 键到索引的映射，用于 O(1) 查找
}

// Entry 表示 MapCRDT 中的一列数据。
// TypeHint 用于在序列化/反序列化时保留泛型类型信息。
// 格式: "pkg/crdt.ORSet[string]" 或简单的 "string" 用于基本类型
type Entry struct {
	Type     Type
	Data     []byte
	TypeHint string // 可选的 Go 类型提示，如 "string", "int", "[]byte"
}

func NewMapCRDT() *MapCRDT {
	return &MapCRDT{
		Entries:      make(map[string]*Entry),
		cache:        make(map[string]CRDT),
		maxCacheSize: 1000, // 默认最大缓存大小
		lruKeys:      make([]string, 0, 1000),
		lruIndex:     make(map[string]int),
	}
}

// SetBaseDir 设置用于 LocalFile CRDT 的基础目录。
// 会递归设置缓存中的 LocalFile CRDT 和嵌套的 MapCRDT。
func (m *MapCRDT) SetBaseDir(dir string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.baseDir = dir
	for _, c := range m.cache {
		if lf, ok := c.(*LocalFileCRDT); ok {
			lf.SetBaseDir(dir)
		} else if subMap, ok := c.(*MapCRDT); ok {
			subMap.SetBaseDir(dir)
		}
	}
}

func (m *MapCRDT) Type() Type { return TypeMap }

// updateLRU 更新 LRU 缓存跟踪
// 优化：使用 map 直接查找位置，避免 O(n) 遍历
func (m *MapCRDT) updateLRU(key string) {
	// 如果 key 已存在，直接移动到末尾
	if pos, exists := m.lruIndex[key]; exists {
		// 移除当前位置
		m.lruKeys = append(m.lruKeys[:pos], m.lruKeys[pos+1:]...)
		// 更新所有后续元素的位置
		for i := pos; i < len(m.lruKeys); i++ {
			m.lruIndex[m.lruKeys[i]] = i
		}
	}

	// 添加到末尾
	m.lruKeys = append(m.lruKeys, key)
	m.lruIndex[key] = len(m.lruKeys) - 1

	// 如果超过最大缓存大小，移除最旧的
	if m.maxCacheSize > 0 && len(m.lruKeys) > m.maxCacheSize {
		oldest := m.lruKeys[0]
		m.lruKeys = m.lruKeys[1:]
		delete(m.cache, oldest)
		delete(m.lruIndex, oldest)
		// 更新所有元素的位置
		for i, k := range m.lruKeys {
			m.lruIndex[k] = i
		}
	}
}

func (m *MapCRDT) Value() any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 返回 map[string]any
	// 仅返回反序列化的值用于演示。
	// 调用者可能需要原始 CRDT。
	res := make(map[string]any)
	for k, e := range m.Entries {
		// 优先从 Cache 获取
		if c, ok := m.cache[k]; ok {
			res[k] = c.Value()
			continue
		}
		// 使用 TypeHint 进行反序列化
		c, err := DeserializeWithHint(e.Type, e.Data, e.TypeHint)
		if err == nil {
			// 在 Value() 中我们不做 SetBaseDir，因为通常 Value() 只是查看元数据。
			// 如果真的需要读取内容，应该使用 GetLocalFile。
			// 不过为了一致性，如果我们临时反序列化了，也许应该设置？
			// 但这里没有把 c 放入 cache，所以它是临时的。
			// 如果 c 是 LocalFile CRDT，Value() 返回 Metadata，不需要 BaseDir。
			res[k] = c.Value()
		}
	}
	return res
}

type serializerRegistry struct {
	mu               sync.RWMutex
	orSetSerializers map[string]func([]byte) (any, error)
	rgaSerializers   map[string]func([]byte) (any, error)
}

func newSerializerRegistry() *serializerRegistry {
	return &serializerRegistry{
		orSetSerializers: make(map[string]func([]byte) (any, error)),
		rgaSerializers:   make(map[string]func([]byte) (any, error)),
	}
}

func (r *serializerRegistry) GetORSetSerializer(typeHint string) (func([]byte) (any, error), bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	serializer, ok := r.orSetSerializers[typeHint]
	return serializer, ok
}

func (r *serializerRegistry) SetORSetSerializer(typeHint string, serializer func([]byte) (any, error)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.orSetSerializers[typeHint] = serializer
}

func (r *serializerRegistry) DeleteORSetSerializer(typeHint string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.orSetSerializers, typeHint)
}

func (r *serializerRegistry) GetRGASerializer(typeHint string) (func([]byte) (any, error), bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	serializer, ok := r.rgaSerializers[typeHint]
	return serializer, ok
}

func (r *serializerRegistry) SetRGASerializer(typeHint string, serializer func([]byte) (any, error)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rgaSerializers[typeHint] = serializer
}

func (r *serializerRegistry) DeleteRGASerializer(typeHint string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.rgaSerializers, typeHint)
}

// TypeRegistry stores generic deserializers and is safe for concurrent access.
var TypeRegistry = newSerializerRegistry()

// RegisterORSet 注册 ORSet 的特定类型反序列化函数
func RegisterORSet[T comparable](typeHint string, serializer func([]byte) (*ORSet[T], error)) {
	TypeRegistry.SetORSetSerializer(typeHint, func(data []byte) (any, error) {
		return serializer(data)
	})
}

// RegisterRGA 注册 RGA 的特定类型反序列化函数
func RegisterRGA[T any](typeHint string, serializer func([]byte) (*RGA[T], error)) {
	TypeRegistry.SetRGASerializer(typeHint, func(data []byte) (any, error) {
		return serializer(data)
	})
}
