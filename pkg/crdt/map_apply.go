package crdt

import "fmt"

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
