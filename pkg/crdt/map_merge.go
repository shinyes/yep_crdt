package crdt

import (
	"encoding/json"
	"fmt"
)

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
