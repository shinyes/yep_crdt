package db

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/index"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// Table 代表逻辑表。
type Table struct {
	db           *DB
	schema       *meta.TableSchema
	indexManager *index.Manager
	tx           store.Tx
}

func (t *Table) inTx(update bool, fn func(store.Tx) error) error {
	if t.tx != nil {
		return fn(t.tx)
	}
	if update {
		return t.db.store.Update(fn)
	}
	return t.db.store.View(fn)
}

func (t *Table) tablePrefix() []byte {
	return []byte(fmt.Sprintf("/d/%s/", t.schema.Name))
}

// Set 插入或更新一行。
// data 是 column -> value 的映射 (原始 Go 类型，还不是 CRDT)。
// 为了简化 V1，我们假设数据映射到 LWWRegister 或者我们需要根据模式处理类型。
// 为了使其完全支持 CRDT 泛型，输入可能应该是 CRDT Ops 或就绪的 CRDT。
// 但检查 Fluent API 设计：Set(key, Data{...})。
// 所以我们需要将 Data 转换为 CRDT。
func (t *Table) Set(key uuid.UUID, data map[string]any) error {

	return t.inTx(true, func(txn store.Tx) error {
		// 1. 加载现有 CRDT (用于索引比较)
		keyBytes := t.dataKey(key)
		existingBytes, err := txn.Get(keyBytes)

		var currentMap *crdt.MapCRDT
		var oldBody map[string]any

		if err == store.ErrKeyNotFound {
			currentMap = crdt.NewMapCRDT()
		} else if err != nil {
			return err
		} else {
			currentMap, err = crdt.FromBytesMap(existingBytes)
			if err != nil {
				return fmt.Errorf("failed to decode existing data: %w", err)
			}
			oldBody = currentMap.Value().(map[string]any)
		}

		// 2. 应用更改
		// 我们遍历输入数据并更新 MapCRDT。
		// 对于 MVP，除非在模式中指定，否则我们假设所有输入都是 LWWRegister。
		// 我们需要模式来知道类型。

		for col, val := range data {
			// 查找列模式
			// colType := t.findColType(col)
			// 目前，默认为 LWW。

			// 构造 LWW 寄存器
			ts := t.db.clock.Now()
			lww := crdt.NewLWWRegister(encodeValue(val), ts)

			// 应用到 Map
			op := crdt.OpMapSet{
				Key:   col,
				Value: lww,
			}
			if err := currentMap.Apply(op); err != nil {
				return err
			}
		}

		// 3. 更新索引
		newBody := currentMap.Value().(map[string]any)
		if err := t.indexManager.UpdateIndexes(txn, t.schema.ID, t.schema.Indexes, key[:], oldBody, newBody); err != nil {
			return err
		}

		// 4. 保存
		finalBytes, err := currentMap.Bytes()
		if err != nil {
			return err
		}
		return txn.Set(keyBytes, finalBytes, 0)
	})
}

func (t *Table) Get(key uuid.UUID) (map[string]any, error) {
	var res map[string]any
	err := t.inTx(false, func(txn store.Tx) error {
		val, err := txn.Get(t.dataKey(key))
		if err != nil {
			return err
		}
		m, err := crdt.FromBytesMap(val)
		if err != nil {
			return err
		}
		res = m.Value().(map[string]any)
		return nil
	})
	return res, err
}

func (t *Table) dataKey(u uuid.UUID) []byte {
	// 格式：/d/<table>/<16-byte-uuid>
	prefix := t.tablePrefix()
	return append(prefix, u[:]...)
}

// Add 执行特定 CRDT 的添加/增加操作。
// Counter: Inc(val)
// ORSet: Add(val)
// RGA: Append(val)
// LWW: Set(val) (回退)
func (t *Table) Add(key uuid.UUID, col string, val any) error {
	// Validate key version if strict
	if key.Version() != 7 {
		return fmt.Errorf("invalid key version: must be UUIDv7")
	}
	colType, err := t.getColCrdtType(col)
	if err != nil {
		return err
	}

	return t.inTx(true, func(txn store.Tx) error {
		currentMap, oldBody, err := t.loadRow(txn, key)
		if err != nil {
			return err
		}

		// Initialize if missing
		if currentMap.Entries[col] == nil {
			var newCrdt crdt.CRDT
			switch colType {
			case meta.CrdtCounter:
				newCrdt = crdt.NewPNCounter(t.db.NodeID)
			case meta.CrdtORSet:
				newCrdt = crdt.NewORSet[string]()
			case meta.CrdtRGA:
				newCrdt = crdt.NewRGA[[]byte](t.db.clock)
			}

			if newCrdt != nil {
				// Apply initialization
				initOp := crdt.OpMapSet{Key: col, Value: newCrdt}
				if err := currentMap.Apply(initOp); err != nil {
					return err
				}
			}
		}

		var op crdt.Op
		ts := t.db.clock.Now()

		switch colType {
		case meta.CrdtCounter:
			delta, ok := toInt64(val)
			if !ok {
				return fmt.Errorf("value %v is not int for Counter", val)
			}
			op = crdt.OpMapUpdate{
				Key: col,
				Op:  crdt.OpPNCounterInc{Val: delta},
			}
		case meta.CrdtORSet:
			op = crdt.OpMapUpdate{
				Key: col,
				Op:  crdt.OpORSetAdd[string]{Element: string(encodeValue(val))},
			}
		case meta.CrdtRGA:
			// Append to end. Need to find tail?
			// RGA Insert requires AnchorID.
			// If we append, we need the ID of the last element.
			// MapCRDT doesn't expose RGA structure directly via Value().
			// We need to access the RGA instance.
			rga, err := t.getRGA(currentMap, col)
			if err != nil {
				return err
			}
			// Find last element
			lastID := rga.Head
			// Traverse to find end. (O(N) - optimized later?)
			curr := rga.Head
			for curr != "" {
				v := rga.Vertices[curr]
				if v.Next == "" {
					lastID = v.ID
					break
				}
				curr = v.Next
			}

			op = crdt.OpMapUpdate{
				Key: col,
				Op:  crdt.OpRGAInsert[[]byte]{AnchorID: lastID, Value: encodeValue(val)},
			}
		default: // LWW or Unknown
			lww := crdt.NewLWWRegister(encodeValue(val), ts)
			op = crdt.OpMapSet{Key: col, Value: lww}
		}

		if err := currentMap.Apply(op); err != nil {
			return err
		}

		return t.saveRow(txn, key, currentMap, oldBody)

	})
}

// Remove 执行特定 CRDT 的移除/减少操作。
// ORSet: Remove(val)
// RGA: RemoveByValue(val) (Remove all instances of val)
func (t *Table) Remove(key uuid.UUID, col string, val any) error {
	if key.Version() != 7 {
		return fmt.Errorf("invalid key version: must be UUIDv7")
	}
	colType, err := t.getColCrdtType(col)
	if err != nil {
		return err
	}

	return t.inTx(true, func(txn store.Tx) error {
		currentMap, oldBody, err := t.loadRow(txn, key)
		if err != nil {
			return err
		}

		var op crdt.Op

		switch colType {
		case meta.CrdtCounter:
			delta, ok := toInt64(val)
			if !ok {
				return fmt.Errorf("value %v is not int for Counter", val)
			}
			op = crdt.OpMapUpdate{
				Key: col,
				Op:  crdt.OpPNCounterInc{Val: -delta},
			}
		case meta.CrdtORSet:
			op = crdt.OpMapUpdate{
				Key: col,
				Op:  crdt.OpORSetRemove[string]{Element: string(encodeValue(val))},
			}
		case meta.CrdtRGA:
			// Remove by Value. Need to find all IDs with this value.
			rga, err := t.getRGA(currentMap, col)
			if err != nil {
				return err
			}
			targetVal := encodeValue(val)
			var idsToRemove []string
			for id, v := range rga.Vertices {
				if !v.Deleted && string(v.Value) == string(targetVal) {
					idsToRemove = append(idsToRemove, id)
				}
			}

			// Apply multiple remove ops? MapUpdate supports single Op.
			// We might need multiple applies.
			for _, id := range idsToRemove {
				subOp := crdt.OpMapUpdate{
					Key: col,
					Op:  crdt.OpRGARemove{ID: id},
				}
				if err := currentMap.Apply(subOp); err != nil {
					return err
				}
			}
			return t.saveRow(txn, key, currentMap, oldBody)

		default:
			return fmt.Errorf("remove not supported for type %s", colType)
		}

		if err := currentMap.Apply(op); err != nil {
			return err
		}

		return t.saveRow(txn, key, currentMap, oldBody)

	})
}

// InsertAfter 在 RGA 中指定元素后插入。
func (t *Table) InsertAfter(key uuid.UUID, col string, anchorVal any, newVal any) error {
	if key.Version() != 7 {
		return fmt.Errorf("invalid key version: must be UUIDv7")
	}
	colType, err := t.getColCrdtType(col)
	if err != nil || colType != meta.CrdtRGA {
		return fmt.Errorf("InsertAfter only supported for RGA")
	}

	return t.inTx(true, func(txn store.Tx) error {
		currentMap, oldBody, err := t.loadRow(txn, key)
		if err != nil {
			return err
		}

		rga, err := t.getRGA(currentMap, col)
		if err != nil {
			return err
		}

		// Find ID for anchorVal (first occurrence)
		anchorID := ""
		targetVal := encodeValue(anchorVal)

		// Traversal to find first visible match
		curr := rga.Head
		for curr != "" {
			v := rga.Vertices[curr]
			if !v.Deleted && v.ID != rga.Head && string(v.Value) == string(targetVal) {
				anchorID = v.ID
				break
			}
			curr = v.Next
		}

		if anchorID == "" {
			return fmt.Errorf("anchor value not found: %v", anchorVal)
		}

		op := crdt.OpMapUpdate{
			Key: col,
			Op:  crdt.OpRGAInsert[[]byte]{AnchorID: anchorID, Value: encodeValue(newVal)},
		}

		if err := currentMap.Apply(op); err != nil {
			return err
		}

		return t.saveRow(txn, key, currentMap, oldBody)
	})
}

// InsertAt 在 RGA 第 N 个位置插入 (0-based).
func (t *Table) InsertAt(key uuid.UUID, col string, index int, val any) error {
	if key.Version() != 7 {
		return fmt.Errorf("invalid key version: must be UUIDv7")
	}
	colType, err := t.getColCrdtType(col)
	if err != nil || colType != meta.CrdtRGA {
		return fmt.Errorf("InsertAt only supported for RGA")
	}

	return t.inTx(true, func(txn store.Tx) error {
		currentMap, oldBody, err := t.loadRow(txn, key)
		if err != nil {
			return err
		}

		rga, err := t.getRGA(currentMap, col)
		if err != nil {
			return err
		}

		// Find Anchor ID (element at index-1)

		if index < 0 {
			return fmt.Errorf("invalid index: %d", index)
		}

		currentIndex := 0
		curr := rga.Vertices[rga.Head].Next

		// Traverse to find element at index-1 (if index=0, anchor is Head)
		// We need to skip deleted nodes.
		for curr != "" && currentIndex < index {
			v := rga.Vertices[curr]
			if !v.Deleted {
				currentIndex++
			}
			// Maintain anchorID as the last visible node or Head
			// Wait, simple logic:
			// If index is 0, anchor is Head.
			// If index is 1, anchor is 0-th element.

			// Correct traversal:
			// Scan until we pass `index` visible elements? No.
			// We need the ID of the element *before* the insertion point.

			if currentIndex == index { // Found our spot? No, index-1
				// If index=0, loop doesn't run, anchorID=Head. Correct.
				// If index=1, we need 0-th element ID.
			}

			if !v.Deleted {
				if currentIndex == index {
					// We successfully passed index-1 visible items.
					// But we are at `index`-th item.
					// We need the previous one.
					// Use a prev pointer?
				}
			}
			curr = v.Next
		}

		// Let's retry traversal simpler
		// Find the ID of the (index-1)-th visible element.
		// If index=0, anchor=Head.

		targetAnchor := rga.Head
		if index > 0 {
			steps := 0
			curr := rga.Vertices[rga.Head].Next
			for curr != "" {
				v := rga.Vertices[curr]
				if !v.Deleted {
					steps++
					if steps == index {
						targetAnchor = v.ID
						break
					}
				}
				curr = v.Next
			}
			if steps < index {
				return fmt.Errorf("index out of bounds: %d", index)
			}
		}

		op := crdt.OpMapUpdate{
			Key: col,
			Op:  crdt.OpRGAInsert[[]byte]{AnchorID: targetAnchor, Value: encodeValue(val)},
		}

		if err := currentMap.Apply(op); err != nil {
			return err
		}

		return t.saveRow(txn, key, currentMap, oldBody)
	})
}

// RemoveAt Removes element at index N.
func (t *Table) RemoveAt(key uuid.UUID, col string, index int) error {
	if key.Version() != 7 {
		return fmt.Errorf("invalid key version: must be UUIDv7")
	}
	colType, err := t.getColCrdtType(col)
	if err != nil || colType != meta.CrdtRGA {
		return fmt.Errorf("RemoveAt only supported for RGA")
	}

	return t.inTx(true, func(txn store.Tx) error {
		currentMap, oldBody, err := t.loadRow(txn, key)
		if err != nil {
			return err
		}

		rga, err := t.getRGA(currentMap, col)
		if err != nil {
			return err
		}

		// Find ID of N-th visible element (0-based)
		targetID := ""
		steps := -1 // 0-based index

		curr := rga.Vertices[rga.Head].Next
		for curr != "" {
			v := rga.Vertices[curr]
			if !v.Deleted {
				steps++
				if steps == index {
					targetID = v.ID
					break
				}
			}
			curr = v.Next
		}

		if targetID == "" {
			return fmt.Errorf("index out of bounds: %d", index)
		}

		op := crdt.OpMapUpdate{
			Key: col,
			Op:  crdt.OpRGARemove{ID: targetID},
		}

		if err := currentMap.Apply(op); err != nil {
			return err
		}

		return t.saveRow(txn, key, currentMap, oldBody)
	})
}

// Internal Helpers

func (t *Table) getColCrdtType(col string) (string, error) {
	for _, c := range t.schema.Columns {
		if c.Name == col {
			if c.CrdtType == "" {
				return meta.CrdtLWW, nil // Default
			}
			return c.CrdtType, nil
		}
	}
	return "", fmt.Errorf("column not found: %s", col)
}

// validateKey logic removed as we use uuid.UUID type. Check version if needed.

func (t *Table) loadRow(txn store.Tx, pk uuid.UUID) (*crdt.MapCRDT, map[string]any, error) {
	keyBytes := t.dataKey(pk)
	existingBytes, err := txn.Get(keyBytes)

	var currentMap *crdt.MapCRDT
	var oldBody map[string]any

	if err == store.ErrKeyNotFound {
		currentMap = crdt.NewMapCRDT()
	} else if err != nil {
		return nil, nil, err
	} else {
		currentMap, err = crdt.FromBytesMap(existingBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode existing data: %w", err)
		}
		oldBody = currentMap.Value().(map[string]any)
	}
	return currentMap, oldBody, nil
}

func (t *Table) saveRow(txn store.Tx, pk uuid.UUID, currentMap *crdt.MapCRDT, oldBody map[string]any) error {
	newBody := currentMap.Value().(map[string]any)
	if err := t.indexManager.UpdateIndexes(txn, t.schema.ID, t.schema.Indexes, pk[:], oldBody, newBody); err != nil {
		return err
	}
	finalBytes, err := currentMap.Bytes()
	if err != nil {
		return err
	}
	keyBytes := t.dataKey(pk)
	return txn.Set(keyBytes, finalBytes, 0)
}

func (t *Table) getRGA(m *crdt.MapCRDT, col string) (*crdt.RGA[[]byte], error) {
	// Map stores generic CRDTs. We need to cast it.
	rga, err := crdt.GetRGA[[]byte](m, col)
	if err != nil {
		return nil, err
	}
	if rga == nil {
		// Initialize
		newRGA := crdt.NewRGA[[]byte](t.db.clock)
		m.Apply(crdt.OpMapSet{Key: col, Value: newRGA})
		return newRGA, nil
	}
	return rga, nil
}

func toInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case int32:
		return int64(val), true
	default:
		return 0, false
	}
}

// Helper to encode value for LWW/RGA (bytes) or ORSet (string->bytes)
func encodeValue(v any) []byte {
	if b, ok := v.([]byte); ok {
		return b
	}
	if s, ok := v.(string); ok {
		return []byte(s)
	}
	return []byte(fmt.Sprintf("%v", v))
}
