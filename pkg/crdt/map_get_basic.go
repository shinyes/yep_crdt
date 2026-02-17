package crdt

import "fmt"

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

	// 检查在反序列化期间是否已被其他 goroutine 添加到缓存
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
