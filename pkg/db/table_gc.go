package db

import (
	"fmt"

	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/store"
)

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
