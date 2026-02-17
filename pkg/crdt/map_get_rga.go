package crdt

import "fmt"

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
