package crdt

import "fmt"

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
