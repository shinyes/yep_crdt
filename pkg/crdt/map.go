package crdt

import (
	"encoding/json"
	"fmt"
	"sync"
)

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

// TypeRegistry 存储泛型类型的反序列化函数
var TypeRegistry = struct {
	ORSetSerializers map[string]func([]byte) (any, error)
	RGASerializers   map[string]func([]byte) (any, error)
	ORSetTypeHints   map[string]string
	RGATypeHints     map[string]string
}{
	ORSetSerializers: make(map[string]func([]byte) (any, error)),
	RGASerializers:   make(map[string]func([]byte) (any, error)),
	ORSetTypeHints:   map[string]string{},
	RGATypeHints:     map[string]string{},
}

// RegisterORSet 注册 ORSet 的特定类型反序列化函数
func RegisterORSet[T comparable](typeHint string, serializer func([]byte) (*ORSet[T], error)) {
	TypeRegistry.ORSetSerializers[typeHint] = func(data []byte) (any, error) {
		return serializer(data)
	}
	TypeRegistry.ORSetTypeHints[typeHint] = fmt.Sprintf("%T", *new(T))
}

// RegisterRGA 注册 RGA 的特定类型反序列化函数
func RegisterRGA[T any](typeHint string, serializer func([]byte) (*RGA[T], error)) {
	TypeRegistry.RGASerializers[typeHint] = func(data []byte) (any, error) {
		return serializer(data)
	}
	TypeRegistry.RGATypeHints[typeHint] = fmt.Sprintf("%T", *new(T))
}

// Deserialize 根据类型反序列化 CRDT。
// 注意：对于泛型类型，这里只能使用默认类型（例如 string）。
// 如果需要特定类型，请使用 GetORSet[T] 等方法。
func Deserialize(t Type, data []byte) (CRDT, error) {
	return DeserializeWithHint(t, data, "")
}

// DeserializeWithHint 根据类型和类型提示反序列化 CRDT。
// typeHint 可以是 "string", "int", "uint" 等基本类型，或 "pkg/path.Type" 格式
func DeserializeWithHint(t Type, data []byte, typeHint string) (CRDT, error) {
	if data == nil {
		return nil, &InvalidDataError{CRDTType: t, Reason: "输入数据为 nil", DataLength: 0}
	}

	if len(data) == 0 {
		return nil, &InvalidDataError{CRDTType: t, Reason: "输入数据为空", DataLength: 0}
	}

	switch t {
	case TypeLWW:
		c, err := FromBytesLWW(data)
		if err != nil {
			return nil, fmt.Errorf("%w: LWW: %v", ErrDeserialization, err)
		}
		return c, nil
	case TypeORSet:
		// 优先使用类型注册表
		if typeHint != "" {
			if serializer, ok := TypeRegistry.ORSetSerializers[typeHint]; ok {
				c, err := serializer(data)
				if err == nil {
					return c.(CRDT), nil
				}
				// 回退到默认
			}
		}
		// 默认反序列化为 ORSet[string] 以保持兼容性
		c, err := FromBytesORSet[string](data)
		if err != nil {
			return nil, fmt.Errorf("%w: ORSet[string]: %v", ErrDeserialization, err)
		}
		return c, nil
	case TypePNCounter:
		c, err := FromBytesPNCounter(data)
		if err != nil {
			return nil, fmt.Errorf("%w: PNCounter: %v", ErrDeserialization, err)
		}
		return c, nil
	case TypeRGA:
		// 优先使用类型注册表
		if typeHint != "" {
			if serializer, ok := TypeRegistry.RGASerializers[typeHint]; ok {
				c, err := serializer(data)
				if err == nil {
					return c.(CRDT), nil
				}
				// 回退到默认
			}
		}
		// 默认反序列化为 RGA[[]byte] 以保持兼容性
		c, err := FromBytesRGA[[]byte](data)
		if err != nil {
			return nil, fmt.Errorf("%w: RGA[[]byte]: %v", ErrDeserialization, err)
		}
		return c, nil
	case TypeMap:
		c, err := FromBytesMap(data)
		if err != nil {
			return nil, fmt.Errorf("%w: MapCRDT: %v", ErrDeserialization, err)
		}
		return c, nil
	case TypeLocalFile:
		c, err := FromBytesLocalFile(data)
		if err != nil {
			return nil, fmt.Errorf("%w: LocalFile CRDT: %v", ErrDeserialization, err)
		}
		return c, nil
	default:
		return nil, &InvalidDataError{CRDTType: t, Reason: "未知的 CRDT 类型", DataLength: len(data)}
	}
}

// OpMapSet 为列设置 CRDT。
type OpMapSet struct {
	Key   string
	Value CRDT
}

func (op OpMapSet) Type() Type { return TypeMap }

// OpMapUpdate 对 Map 中的现有 CRDT 应用操作。
type OpMapUpdate struct {
	Key string
	Op  Op
}

func (op OpMapUpdate) Type() Type { return TypeMap }

func (m *MapCRDT) Apply(op Op) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if op == nil {
		return fmt.Errorf("%w: 操作不能为 nil", ErrInvalidOp)
	}

	switch o := op.(type) {
	case OpMapSet:
		if o.Key == "" {
			return fmt.Errorf("%w: 键不能为空", ErrInvalidOp)
		}
		if o.Value == nil {
			return fmt.Errorf("%w: 值不能为 nil (键: %s)", ErrInvalidOp, o.Key)
		}

		// 如果是 LocalFile CRDT，注入 BaseDir
		if lf, ok := o.Value.(*LocalFileCRDT); ok && m.baseDir != "" {
			lf.SetBaseDir(m.baseDir)
		} else if subMap, ok := o.Value.(*MapCRDT); ok && m.baseDir != "" {
			subMap.SetBaseDir(m.baseDir)
		}

		// 更新缓存
		m.cache[o.Key] = o.Value
		// 同步更新 Entry，保证 Data 也是最新的（虽然有 Flush 机制，但 Set 较少见，稳妥起见）
		b, err := o.Value.Bytes()
		if err != nil {
			return fmt.Errorf("%w: 键 '%s': %v", ErrSerialization, o.Key, err)
		}
		m.Entries[o.Key] = &Entry{
			Type: o.Value.Type(),
			Data: b,
		}

	case OpMapUpdate:
		if o.Key == "" {
			return fmt.Errorf("%w: 键不能为空", ErrInvalidOp)
		}
		if o.Op == nil {
			return fmt.Errorf("%w: 操作不能为 nil (键: %s)", ErrInvalidOp, o.Key)
		}

		// 1. 尝试从缓存获取
		c, inCache := m.cache[o.Key]

		if !inCache {
			// 2. 缓存未命中，从 Entry 加载
			e, ok := m.Entries[o.Key]
			if !ok {
				return &KeyNotFoundError{Key: o.Key}
			}
			var err error
			c, err = DeserializeWithHint(e.Type, e.Data, e.TypeHint)
			if err != nil {
				return fmt.Errorf("反序列化键 '%s' 失败: %w", o.Key, err)
			}
			// 注入 BaseDir
			if lf, ok := c.(*LocalFileCRDT); ok && m.baseDir != "" {
				lf.SetBaseDir(m.baseDir)
			} else if subMap, ok := c.(*MapCRDT); ok && m.baseDir != "" {
				subMap.SetBaseDir(m.baseDir)
			}
			// 放入缓存
			m.cache[o.Key] = c
		}

		// 3. 应用操作到对象（内存中）
		if err := c.Apply(o.Op); err != nil {
			return fmt.Errorf("应用操作到键 '%s' 失败: %w", o.Key, err)
		}

		// 4. 不再立即序列化回 Entry.Data。
		// Entry.Data 现在是脏数据，将在 Bytes() 或显式 Flush 时更新。
		// 这将 O(N) 的序列化操作平摊到了 Save 时刻。

	default:
		return fmt.Errorf("%w: 未知操作类型 %T", ErrInvalidOp, op)
	}
	return nil
}

func (m *MapCRDT) Merge(other CRDT) error {
	if other == nil {
		return fmt.Errorf("%w: 合并的 CRDT 不能为 nil", ErrInvalidData)
	}

	o, ok := other.(*MapCRDT)
	if !ok {
		return fmt.Errorf("%w: 期望 *MapCRDT, 得到 %T", ErrTypeMismatch, other)
	}

	// 合并前，我们需要确保本地 cache 中的脏数据已经持久化到 Entries 吗？
	// 或者 Merge 直接操作 Entries？
	// 远程过来的数据在 Entries 中。
	// 本地的数据可能在 cache 中较新。
	// 筀略：
	// 1. 遍历远程 Entries。
	// 2. 如果本地 cache 中有对应项，直接 Merge 到 cache 中。并标记为脏（虽然已经是脏的）。
	// 3. 如果本地 cache 没有，但 Entries 有，反序列化到 cache，然后 Merge。
	// 4. 如果本地都没有，直接拷贝 Entry 到本地 Entries（且不通过 cache，或者放入 cache）。

	// 简单起见，且为了正确性：
	// 我们把远程的数据 merge 进本地的活跃对象 (cache) 中。

	for k, remoteEntry := range o.Entries {
		// 1. 尝试从缓存获取活跃对象
		localC, inCache := m.cache[k]

		if inCache {
			// 本地有活跃对象，必须反序列化远程对象并 Merge 进去
			remoteC, err := DeserializeWithHint(remoteEntry.Type, remoteEntry.Data, remoteEntry.TypeHint)
			if err != nil {
				return fmt.Errorf("反序列化远程键 '%s' 失败: %w", k, err)
			}
			// 注入 BaseDir 到远程对象 (虽然它只是用来读取数据的，但为了 Merge 安全?)
			// Merge 通常只比较元数据，不需要 ReadAll。
			if err := localC.Merge(remoteC); err != nil {
				return fmt.Errorf("合并键 '%s' 失败: %w", k, err)
			}
			// Merge 完成，localC 更新了。Entry.Data 仍是陈旧的。
			continue
		}

		// 2. 缓存无，检查本地 Entry
		localEntry, exists := m.Entries[k]
		if !exists {
			// 本地完全没有，直接采纳远程 Entry
			// 这种情况下，不需要反序列化，直接存 Entry 即可。cache 保持空白。
			m.Entries[k] = remoteEntry
			continue
		}

		// 3. 本地有 Entry 但无 Cache。需要比较/合并。
		if localEntry.Type != remoteEntry.Type {
			// 类型冲突，LWW 或其他策略。这里简单覆盖？或者保留本地？
			// 假设类型一旦确定不变。如果变了，覆盖。
			m.Entries[k] = remoteEntry
		} else {
			// 两个都是冷数据 (Bytes)。
			// 反序列化 -> Merge -> 序列化 -> 存回 Entry?
			// 还是反序列化 -> Merge -> 放入 Cache? (推荐后者，Lazy)

			lC, err := DeserializeWithHint(localEntry.Type, localEntry.Data, localEntry.TypeHint)
			if err != nil {
				return fmt.Errorf("反序列化本地键 '%s' 失败: %w", k, err)
			}
			if lf, ok := lC.(*LocalFileCRDT); ok && m.baseDir != "" {
				lf.SetBaseDir(m.baseDir)
			} else if subMap, ok := lC.(*MapCRDT); ok && m.baseDir != "" {
				subMap.SetBaseDir(m.baseDir)
			}

			rC, err := DeserializeWithHint(remoteEntry.Type, remoteEntry.Data, remoteEntry.TypeHint)
			if err != nil {
				return fmt.Errorf("反序列化远程键 '%s' (第二次) 失败: %w", k, err)
			}

			if err := lC.Merge(rC); err != nil {
				return fmt.Errorf("合并键 '%s' 失败: %w", k, err)
			}

			// 放入 Cache，标记为活跃/脏
			m.cache[k] = lC
		}
	}
	return nil
}

func (m *MapCRDT) Bytes() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Flush cache into Entries in place, then serialize snapshot directly.
	for k, c := range m.cache {
		b, err := c.Bytes()
		if err != nil {
			return nil, fmt.Errorf("%w: 序列化键 '%s' 失败: %v", ErrSerialization, k, err)
		}
		if existing, ok := m.Entries[k]; ok {
			existing.Data = b
			existing.Type = c.Type()
		} else {
			m.Entries[k] = &Entry{Type: c.Type(), Data: b}
		}
	}

	state := &struct {
		Entries map[string]*Entry `json:"entries"`
	}{
		Entries: m.Entries,
	}

	data, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("%w: JSON 序列化失败: %v", ErrSerialization, err)
	}
	return data, nil
}

func (m *MapCRDT) GC(safeTimestamp int64) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	// 遍历所有数据。优先遍历 Cache，再遍历 Entries 中不在 Cache 的。
	// 或者：先 Flush？
	// GC 可能不需要 Flush，但如果 GC 修改了数据，需要更新。

	// 策略：遍历 Entries 的所有 Key。
	for k, e := range m.Entries {
		var c CRDT
		var inCache bool

		if cached, ok := m.cache[k]; ok {
			c = cached
			inCache = true
		} else {
			var err error
			c, err = DeserializeWithHint(e.Type, e.Data, e.TypeHint)
			if err != nil {
				// Skip bad data
				continue
			}
			// 不一定非要放入 Cache，除非 GC 发生了修改。
			// 但为了简化，如果反序列化了，不妨放入 Cache？
			// 算了，按需加载。
		}

		if c != nil {
			removed := c.GC(safeTimestamp)
			count += removed
			if removed > 0 {
				if !inCache {
					// 如果刚才不在 Cache 里，现在修改了，必须放入 Cache (变成脏数据)
					m.cache[k] = c
				}
				// 如果已经在 Cache 里，它已经被修改了，无需额外操作。
			}
		}
	}

	return count
}

func FromBytesMap(data []byte) (*MapCRDT, error) {
	if data == nil {
		return nil, &InvalidDataError{CRDTType: TypeMap, Reason: "输入数据为 nil", DataLength: 0}
	}
	if len(data) == 0 {
		return nil, &InvalidDataError{CRDTType: TypeMap, Reason: "输入数据为空", DataLength: 0}
	}

	m := NewMapCRDT()
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("%w: JSON 反序列化失败: %v", ErrDeserialization, err)
	}
	// Entries 已加载。Cache 为空。
	return m, nil
}

func (m *MapCRDT) GetCRDT(key string) CRDT {
	if key == "" {
		return nil
	}

	m.mu.RLock()
	// Try cache first
	if c, ok := m.cache[key]; ok {
		m.mu.RUnlock()
		return c
	}

	e, exists := m.Entries[key]
	m.mu.RUnlock()

	if !exists {
		return nil
	}

	c, err := DeserializeWithHint(e.Type, e.Data, e.TypeHint)
	if err != nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查在反序列化期间是否已被其他goroutine添加到缓存
	if existing, ok := m.cache[key]; ok {
		return existing
	}

	// 在持有锁的情况下注入 BaseDir
	if baseDir := m.baseDir; baseDir != "" {
		if lf, ok := c.(*LocalFileCRDT); ok {
			lf.SetBaseDir(baseDir)
		} else if subMap, ok := c.(*MapCRDT); ok {
			subMap.SetBaseDir(baseDir)
		}
	}

	m.cache[key] = c
	m.updateLRU(key)
	return c
}

// GetORSet 获取指定类型的 ORSet。
func GetORSet[T comparable](m *MapCRDT, key string) (*ORSet[T], error) {
	if m == nil {
		return nil, ErrNilCRDT
	}
	if key == "" {
		return nil, fmt.Errorf("%w: 键不能为空", ErrInvalidOp)
	}

	m.mu.RLock()
	// Try cache
	if c, ok := m.cache[key]; ok {
		m.mu.RUnlock()
		if val, castOk := c.(*ORSet[T]); castOk {
			return val, nil
		}
		return nil, &TypeMismatchError{Key: key, ExpectedType: TypeORSet, GotType: c.Type()}
	}

	e, ok := m.Entries[key]
	m.mu.RUnlock()

	if ok {
		if e.Type != TypeORSet {
			return nil, &TypeMismatchError{Key: key, ExpectedType: TypeORSet, GotType: e.Type}
		}
		c, err := FromBytesORSet[T](e.Data)
		if err != nil {
			return nil, fmt.Errorf("反序列化 ORSet 键 '%s' 失败: %w", key, err)
		}
		m.mu.Lock()
		m.cache[key] = c
		m.updateLRU(key)
		m.mu.Unlock()
		return c, nil
	}
	return nil, &KeyNotFoundError{Key: key}
}

// Has 检查键是否存在。
func (m *MapCRDT) Has(key string) bool {
	if key == "" {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, ok := m.cache[key]; ok {
		return true
	}
	_, ok := m.Entries[key]
	return ok
}

// Get 获取键的值。
func (m *MapCRDT) Get(key string) (any, bool) {
	c := m.GetCRDT(key)
	if c == nil {
		return nil, false
	}
	return c.Value(), true
}

// GetString 获取字符串类型的值。
func (m *MapCRDT) GetString(key string) (string, bool) {
	v, ok := m.Get(key)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// GetInt 获取整数类型的值。
func (m *MapCRDT) GetInt(key string) (int, bool) {
	v, ok := m.Get(key)
	if !ok {
		return 0, false
	}
	switch val := v.(type) {
	case int:
		return val, true
	case int64:
		return int(val), true
	case float64:
		return int(val), true
	}
	return 0, false
}

// GetRGA 获取通用的 RGA 只读接口。
func (m *MapCRDT) GetRGA(key string) (ReadOnlyRGA[any], error) {
	if key == "" {
		return nil, fmt.Errorf("%w: 键不能为空", ErrInvalidOp)
	}

	// 注意：底层 RGA 必须实际上是 RGA[any]，否则会类型转换失败。
	// 如果底层是 RGA[string]，这里会报错。
	r, err := GetRGA[any](m, key)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, &KeyNotFoundError{Key: key}
	}
	return &readOnlyRGA[any]{r: r}, nil
}

// GetRGAString 获取字符串类型的 RGA 只读接口。
func (m *MapCRDT) GetRGAString(key string) (ReadOnlyRGA[string], error) {
	if key == "" {
		return nil, fmt.Errorf("%w: 键不能为空", ErrInvalidOp)
	}

	r, err := GetRGA[string](m, key)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, &KeyNotFoundError{Key: key}
	}
	return &readOnlyRGA[string]{r: r}, nil
}

// GetRGABytes 获取字节数组类型的 RGA 只读接口。
func (m *MapCRDT) GetRGABytes(key string) (ReadOnlyRGA[[]byte], error) {
	if key == "" {
		return nil, fmt.Errorf("%w: 键不能为空", ErrInvalidOp)
	}

	r, err := GetRGA[[]byte](m, key)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, &KeyNotFoundError{Key: key}
	}
	return &readOnlyRGA[[]byte]{r: r}, nil
}

// GetSetString 获取字符串类型的 ORSet 只读接口。
func (m *MapCRDT) GetSetString(key string) (ReadOnlySet[string], error) {
	if key == "" {
		return nil, fmt.Errorf("%w: 键不能为空", ErrInvalidOp)
	}

	s, err := GetORSet[string](m, key)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, &KeyNotFoundError{Key: key}
	}
	return &readOnlySet[string]{s: s}, nil
}

// GetSetInt 获取 int 类型的 ORSet 只读接口。
func (m *MapCRDT) GetSetInt(key string) (ReadOnlySet[int], error) {
	if key == "" {
		return nil, fmt.Errorf("%w: 键不能为空", ErrInvalidOp)
	}

	s, err := GetORSet[int](m, key)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, &KeyNotFoundError{Key: key}
	}
	return &readOnlySet[int]{s: s}, nil
}

// GetLocalFile 获取 LocalFile CRDT 只读接口。
func (m *MapCRDT) GetLocalFile(key string) (ReadOnlyLocalFile, error) {
	if key == "" {
		return nil, fmt.Errorf("%w: 键不能为空", ErrInvalidOp)
	}

	// Try cache via GetCRDT (which handles lazy loading and baseDir injection)
	c := m.GetCRDT(key)
	if c == nil {
		return nil, &KeyNotFoundError{Key: key}
	}

	lf, ok := c.(*LocalFileCRDT)
	if !ok {
		return nil, &TypeMismatchError{Key: key, ExpectedType: TypeLocalFile, GotType: c.Type()}
	}
	return lf, nil
}

// GetRGA 获取指定类型的 RGA。
func GetRGA[T any](m *MapCRDT, key string) (*RGA[T], error) {
	if m == nil {
		return nil, ErrNilCRDT
	}
	if key == "" {
		return nil, fmt.Errorf("%w: 键不能为空", ErrInvalidOp)
	}

	m.mu.RLock()
	// Try cache
	if c, ok := m.cache[key]; ok {
		m.mu.RUnlock()
		if val, castOk := c.(*RGA[T]); castOk {
			return val, nil
		}
		return nil, &TypeMismatchError{Key: key, ExpectedType: TypeRGA, GotType: c.Type()}
	}

	e, ok := m.Entries[key]
	m.mu.RUnlock()

	if ok {
		if e.Type != TypeRGA {
			return nil, &TypeMismatchError{Key: key, ExpectedType: TypeRGA, GotType: e.Type}
		}
		c, err := FromBytesRGA[T](e.Data)
		if err != nil {
			return nil, fmt.Errorf("反序列化 RGA 键 '%s' 失败: %w", key, err)
		}
		m.mu.Lock()
		m.cache[key] = c
		m.updateLRU(key)
		m.mu.Unlock()
		return c, nil
	}
	return nil, &KeyNotFoundError{Key: key}
}
