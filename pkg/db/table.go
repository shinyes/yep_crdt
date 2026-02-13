package db

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"

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

// FileImport 用于在 Set 操作中指定要导入的外部文件。
// 数据库会自动计算哈希、大小，并将文件复制到 storageDir。
type FileImport struct {
	LocalPath    string // 本地源文件路径
	RelativePath string // 目标存储相对路径
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
			// 查找列模式，根据 schema 决定 CRDT 类型
			colCrdtType := meta.CrdtLWW // 默认类型
			for _, schemaCol := range t.schema.Columns {
				if schemaCol.Name == col {
					if schemaCol.CrdtType != "" {
						colCrdtType = schemaCol.CrdtType
					}
					break
				}
			}

			// 检查是否已经是 CRDT
			if c, ok := val.(crdt.CRDT); ok {
				op := crdt.OpMapSet{
					Key:   col,
					Value: c,
				}
				if err := currentMap.Apply(op); err != nil {
					return err
				}
				continue
			}

			// 检查是否是 FileImport (自动导入)
			if fImport, ok := val.(FileImport); ok {
				if t.db.FileStorageDir == "" {
					return fmt.Errorf("FileStorageDir not configured, cannot import file")
				}

				// 1. 确保源文件存在
				srcInfo, err := os.Stat(fImport.LocalPath)
				if err != nil {
					return fmt.Errorf("source file error: %w", err)
				}
				if srcInfo.IsDir() {
					return fmt.Errorf("source path is a directory")
				}

				// 2. 准备目标路径
				destPath := filepath.Join(t.db.FileStorageDir, fImport.RelativePath)
				// 确保父目录存在
				if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
					return fmt.Errorf("failed to create destination dir: %w", err)
				}

				// 3. 复制文件
				if err := copyFile(fImport.LocalPath, destPath); err != nil {
					return fmt.Errorf("failed to copy file: %w", err)
				}

				// 4. 创建元数据 (从目标文件读取，确保一致性)
				meta, err := createFileMetadata(destPath, fImport.RelativePath)
				if err != nil {
					return fmt.Errorf("failed to create file metadata: %w", err)
				}

				lf := crdt.NewLocalFileCRDT(meta, t.db.clock.Now())
				op := crdt.OpMapSet{
					Key:   col,
					Value: lf,
				}
				if err := currentMap.Apply(op); err != nil {
					return err
				}
				continue
			}

			// 根据 schema 定义的 CRDT 类型创建对应的 CRDT
			ts := t.db.clock.Now()
			var crdtVal crdt.CRDT

			switch colCrdtType {
			case meta.CrdtCounter:
				// PN-Counter
				crdtVal = crdt.NewPNCounter(t.db.NodeID)
				intVal, err := extractInt64(val)
				if err != nil {
					return fmt.Errorf("failed to extract int64 value for counter: %w", err)
				}
				if err := crdtVal.Apply(crdt.OpPNCounterInc{Val: intVal}); err != nil {
					return fmt.Errorf("failed to initialize counter: %w", err)
				}
			case meta.CrdtORSet:
				// OR-Set
				crdtVal = crdt.NewORSet[string]()
				// 添加初始元素
				if strVal, ok := val.(string); ok {
					crdtVal.Apply(crdt.OpORSetAdd[string]{Element: strVal})
				}
			case meta.CrdtRGA:
				// RGA
				crdtVal = crdt.NewRGA[[]byte](t.db.clock)
				// 添加初始元素
				if strVal, ok := val.(string); ok {
					crdtVal.Apply(crdt.OpRGAInsert[[]byte]{AnchorID: crdtVal.(*crdt.RGA[[]byte]).Head, Value: []byte(strVal)})
				}
			case meta.CrdtLocalFile:
				return fmt.Errorf("LocalFile requires FileImport type, got %T", val)
			default:
				// LWW (默认)
				crdtVal = crdt.NewLWWRegister(encodeValue(val), ts)
			}

			// 应用到 Map
			op := crdt.OpMapSet{
				Key:   col,
				Value: crdtVal,
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

// GetCRDT returns the raw ReadOnlyMap CRDT for a given key.
// This is useful for accessing nested CRDTs (like RGA) without loading the entire map value.
func (t *Table) GetCRDT(key uuid.UUID) (crdt.ReadOnlyMap, error) {
	var res crdt.ReadOnlyMap
	err := t.inTx(false, func(txn store.Tx) error {
		m, _, err := t.loadRow(txn, key)
		if err != nil {
			return err
		}
		res = m
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

// extractInt64 从任意数值类型提取 int64 值
func extractInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case uint:
		return int64(val), nil
	case uint8:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint64:
		return int64(val), nil
	case float32:
		return int64(val), nil
	case float64:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("cannot extract int64 from %T", v)
	}
}

func copyFile(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()

	if _, err := io.Copy(destination, source); err != nil {
		return err
	}
	return nil
}
// TableGCResult 包含表级 GC 操作的结果统计。
type TableGCResult struct {
	RowsScanned       int   // 扫描的行数量
	TombstonesRemoved int   // 移除的墓碑数量
	Errors           []error // 遇到的错误
}

// GC 对表中所有 CRDT 执行垃圾回收。
// safeTimestamp 是安全的时间戳，所有在此时间戳之前被删除的数据都可以安全清理。
func (t *Table) GC(safeTimestamp int64) *TableGCResult {
	result := &TableGCResult{}
	
	err := t.inTx(true, func(txn store.Tx) error {
		prefix := t.tablePrefix()
		
		// 使用 Prefix 迭代器遍历所有行
		iterator := txn.NewIterator(store.IteratorOptions{Prefix: prefix})
		defer iterator.Close()
		
		// 移动到第一个匹配的键
		iterator.Seek(prefix)
		
		for iterator.ValidForPrefix(prefix) {
			// 获取键和值
			key, value, err := iterator.Item()
			if err != nil {
				result.Errors = append(result.Errors,
					fmt.Errorf("failed to get iterator item: %w", err))
				continue
			}
			
			// 反序列化 MapCRDT
			mapCRDT, err := crdt.FromBytesMap(value)
			if err != nil {
				result.Errors = append(result.Errors, 
					fmt.Errorf("failed to decode row %x: %w", key, err))
				continue
			}
			
			result.RowsScanned++
			
			// 对 MapCRDT 中的每个 CRDT 调用 GC
			removed := mapCRDT.GC(safeTimestamp)
			result.TombstonesRemoved += removed
			
			// 如果有数据被清理，更新存储
			if removed > 0 {
				updatedBytes, err := mapCRDT.Bytes()
				if err != nil {
					result.Errors = append(result.Errors,
						fmt.Errorf("failed to encode row %x after GC: %w", key, err))
					continue
				}
				
				if err := txn.Set(key, updatedBytes, 0); err != nil {
					result.Errors = append(result.Errors,
						fmt.Errorf("failed to update row %x after GC: %w", key, err))
					continue
				}
			}
			
			// 移动到下一个
			iterator.Next()
		}
		
		return nil
	})
	
	if err != nil {
		result.Errors = append(result.Errors, err)
	}
	
	return result
}

// createFileMetadata 根据本地文件路径创建 FileMetadata。
// 它会自动计算文件大小和 SHA256 哈希值。
func createFileMetadata(localPath string, relativePath string) (crdt.FileMetadata, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return crdt.FileMetadata{}, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return crdt.FileMetadata{}, err
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return crdt.FileMetadata{}, err
	}

	return crdt.FileMetadata{
		Path: relativePath,
		Size: info.Size(),
		Hash: fmt.Sprintf("%x", h.Sum(nil)),
	}, nil
}
