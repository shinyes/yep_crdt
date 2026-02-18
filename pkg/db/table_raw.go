package db

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// ScanRawRows 扫描表中所有行的原始 CRDT 字节数据。
// 用于全量同步时导出整个表。
func (t *Table) ScanRawRows() ([]RawRow, error) {
	var rows []RawRow
	err := t.inTx(false, func(txn store.Tx) error {
		prefix := t.tablePrefix()
		iterator := txn.NewIterator(store.IteratorOptions{Prefix: prefix})
		defer iterator.Close()

		iterator.Seek(prefix)
		for iterator.ValidForPrefix(prefix) {
			key, value, err := iterator.Item()
			if err != nil {
				return fmt.Errorf("读取行失败: %w", err)
			}

			// 从 key 中提取 UUID（去掉 tablePrefix 部分）
			uuidBytes := key[len(prefix):]
			if len(uuidBytes) != 16 {
				iterator.Next()
				continue
			}

			pk, err := uuid.FromBytes(uuidBytes)
			if err != nil {
				iterator.Next()
				continue
			}

			// 复制 value 数据（迭代器使用的是内部缓冲区）
			dataCopy := make([]byte, len(value))
			copy(dataCopy, value)

			rows = append(rows, RawRow{Key: pk, Data: dataCopy})
			iterator.Next()
		}
		return nil
	})
	return rows, err
}

// GetRawRow 获取单行的原始 CRDT 字节数据。
// 用于增量同步时获取需要广播的行数据。
func (t *Table) GetRawRow(key uuid.UUID) ([]byte, error) {
	if err := validateUUIDv7(key); err != nil {
		return nil, err
	}

	var data []byte
	err := t.inTx(false, func(txn store.Tx) error {
		val, err := txn.Get(t.dataKey(key))
		if err != nil {
			return err
		}
		data = make([]byte, len(val))
		copy(data, val)
		return nil
	})
	return data, err
}

// GetRawRowColumns returns a partial MapCRDT payload containing only selected columns.
func (t *Table) GetRawRowColumns(key uuid.UUID, columns []string) ([]byte, error) {
	if err := validateUUIDv7(key); err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return t.GetRawRow(key)
	}

	columnSet := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		if col == "" {
			continue
		}
		columnSet[col] = struct{}{}
	}
	if len(columnSet) == 0 {
		return t.GetRawRow(key)
	}

	var data []byte
	err := t.inTx(false, func(txn store.Tx) error {
		raw, err := txn.Get(t.dataKey(key))
		if err != nil {
			return err
		}

		rowMap, err := crdt.FromBytesMap(raw)
		if err != nil {
			return fmt.Errorf("failed to decode existing data: %w", err)
		}

		partial := crdt.NewMapCRDT()
		matched := 0
		for col := range columnSet {
			entry, ok := rowMap.Entries[col]
			if !ok {
				continue
			}

			entryCopy := &crdt.Entry{
				Type:     entry.Type,
				TypeHint: entry.TypeHint,
				Data:     make([]byte, len(entry.Data)),
			}
			copy(entryCopy.Data, entry.Data)
			partial.Entries[col] = entryCopy
			matched++
		}

		if matched == 0 {
			return fmt.Errorf("no matching columns found for key: %s", key.String())
		}

		data, err = partial.Bytes()
		if err != nil {
			return err
		}
		return nil
	})

	return data, err
}

// MergeRawRow 将远程 CRDT 原始字节与本地行进行 Merge。
// 如果本地行不存在，直接写入远程数据。
// 如果本地行存在，执行 MapCRDT.Merge() 进行无冲突合并。
func (t *Table) MergeRawRow(key uuid.UUID, remoteData []byte) error {
	if err := validateUUIDv7(key); err != nil {
		return err
	}

	return t.inTx(true, func(txn store.Tx) error {
		keyBytes := t.dataKey(key)

		// 反序列化远程 MapCRDT
		remoteMap, err := crdt.FromBytesMap(remoteData)
		if err != nil {
			return fmt.Errorf("反序列化远程数据失败: %w", err)
		}

		// 尝试加载本地行
		existingBytes, err := txn.Get(keyBytes)
		if err == store.ErrKeyNotFound {
			// 本地不存在，直接写入远程数据
			return txn.Set(keyBytes, remoteData, 0)
		}
		if err != nil {
			return fmt.Errorf("读取本地行失败: %w", err)
		}

		// 反序列化本地 MapCRDT
		localMap, err := crdt.FromBytesMap(existingBytes)
		if err != nil {
			return fmt.Errorf("反序列化本地数据失败: %w", err)
		}

		oldBody := localMap.Value().(map[string]any)

		// 执行 CRDT Merge
		if err := localMap.Merge(remoteMap); err != nil {
			return fmt.Errorf("CRDT Merge 失败: %w", err)
		}

		// 序列化合并后的结果
		mergedBytes, err := localMap.Bytes()
		if err != nil {
			return fmt.Errorf("序列化合并结果失败: %w", err)
		}

		// 更新索引
		oldIndexBody, err := t.decodeRowForIndex(oldBody)
		if err != nil {
			return fmt.Errorf("decode old row for index failed: %w", err)
		}
		newIndexBody, err := t.decodeRowForIndex(localMap.Value().(map[string]any))
		if err != nil {
			return fmt.Errorf("decode new row for index failed: %w", err)
		}
		if err := t.indexManager.UpdateIndexes(txn, t.schema.ID, t.schema.Indexes, key[:], oldIndexBody, newIndexBody); err != nil {
			return fmt.Errorf("更新索引失败: %w", err)
		}

		return txn.Set(keyBytes, mergedBytes, 0)
	})
}
