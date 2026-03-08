package crdt

import (
	"encoding/json"
	"fmt"
)

func (m *MapCRDT) Merge(other CRDT) error {
	if other == nil {
		return fmt.Errorf("%w: merged CRDT cannot be nil", ErrInvalidData)
	}

	o, ok := other.(*MapCRDT)
	if !ok {
		return fmt.Errorf("%w: expected *MapCRDT, got %T", ErrTypeMismatch, other)
	}
	if m == o {
		return nil
	}

	remoteEntries, err := o.snapshotEntriesForMerge()
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Entries == nil {
		m.Entries = make(map[string]*Entry)
	}
	if m.cache == nil {
		m.cache = make(map[string]CRDT)
	}
	if m.lruIndex == nil {
		m.lruIndex = make(map[string]int)
	}
	if m.lruKeys == nil {
		m.lruKeys = make([]string, 0)
	}

	for k, remoteEntry := range remoteEntries {
		localC, inCache := m.cache[k]
		if inCache {
			remoteC, err := DeserializeWithHint(remoteEntry.Type, remoteEntry.Data, remoteEntry.TypeHint)
			if err != nil {
				return fmt.Errorf("failed to deserialize remote key %q: %w", k, err)
			}
			injectBaseDirIntoCRDT(remoteC, m.baseDir)
			if err := localC.Merge(remoteC); err != nil {
				return fmt.Errorf("failed to merge key %q: %w", k, err)
			}
			m.updateLRU(k)
			continue
		}

		localEntry, exists := m.Entries[k]
		if !exists || localEntry == nil {
			m.Entries[k] = cloneEntry(remoteEntry)
			continue
		}

		if localEntry.Type != remoteEntry.Type {
			m.Entries[k] = cloneEntry(remoteEntry)
			delete(m.cache, k)
			continue
		}

		localC, err := DeserializeWithHint(localEntry.Type, localEntry.Data, localEntry.TypeHint)
		if err != nil {
			return fmt.Errorf("failed to deserialize local key %q: %w", k, err)
		}
		injectBaseDirIntoCRDT(localC, m.baseDir)

		remoteC, err := DeserializeWithHint(remoteEntry.Type, remoteEntry.Data, remoteEntry.TypeHint)
		if err != nil {
			return fmt.Errorf("failed to deserialize remote key %q: %w", k, err)
		}
		injectBaseDirIntoCRDT(remoteC, m.baseDir)

		if err := localC.Merge(remoteC); err != nil {
			return fmt.Errorf("failed to merge key %q: %w", k, err)
		}

		m.cache[k] = localC
		m.updateLRU(k)
	}

	return nil
}

func (m *MapCRDT) snapshotEntriesForMerge() (map[string]*Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := make(map[string]*Entry, len(m.Entries)+len(m.cache))
	for k, e := range m.Entries {
		if e == nil {
			continue
		}
		snapshot[k] = cloneEntry(e)
	}

	for k, c := range m.cache {
		if c == nil {
			continue
		}
		b, err := c.Bytes()
		if err != nil {
			return nil, fmt.Errorf("%w: serialize cached key %q failed: %v", ErrSerialization, k, err)
		}
		typeHint := ""
		if existing, ok := snapshot[k]; ok {
			typeHint = existing.TypeHint
		}
		snapshot[k] = &Entry{
			Type:     c.Type(),
			Data:     copyBytes(b),
			TypeHint: typeHint,
		}
	}

	return snapshot, nil
}

func cloneEntry(e *Entry) *Entry {
	if e == nil {
		return nil
	}
	return &Entry{
		Type:     e.Type,
		Data:     copyBytes(e.Data),
		TypeHint: e.TypeHint,
	}
}

func copyBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	return append([]byte(nil), data...)
}

func injectBaseDirIntoCRDT(c CRDT, baseDir string) {
	if c == nil || baseDir == "" {
		return
	}
	if lf, ok := c.(*LocalFileCRDT); ok {
		lf.SetBaseDir(baseDir)
		return
	}
	if subMap, ok := c.(*MapCRDT); ok {
		subMap.SetBaseDir(baseDir)
	}
}

func (m *MapCRDT) Bytes() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, c := range m.cache {
		b, err := c.Bytes()
		if err != nil {
			return nil, fmt.Errorf("%w: serialize key %q failed: %v", ErrSerialization, k, err)
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
		return nil, fmt.Errorf("%w: json marshal failed: %v", ErrSerialization, err)
	}
	return data, nil
}

func (m *MapCRDT) GC(safeTimestamp int64) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
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
				continue
			}
		}

		if c != nil {
			removed := c.GC(safeTimestamp)
			count += removed
			if removed > 0 {
				if !inCache {
					m.cache[k] = c
				}
			}
		}
	}

	return count
}

func FromBytesMap(data []byte) (*MapCRDT, error) {
	if data == nil {
		return nil, &InvalidDataError{CRDTType: TypeMap, Reason: "input data is nil", DataLength: 0}
	}
	if len(data) == 0 {
		return nil, &InvalidDataError{CRDTType: TypeMap, Reason: "input data is empty", DataLength: 0}
	}

	m := NewMapCRDT()
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("%w: json unmarshal failed: %v", ErrDeserialization, err)
	}
	return m, nil
}
