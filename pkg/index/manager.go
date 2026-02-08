package index

import (
	"fmt"

	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// Manager 处理索引更新。
type Manager struct{}

func NewManager() *Manager {
	return &Manager{}
}

// UpdateIndexes 基于从 oldBody 到 newBody 的变化更新表的所有索引。
// 必须在与数据更新相同的事务中调用此方法。
func (m *Manager) UpdateIndexes(txn store.Tx, tableID uint32, indexes []meta.IndexSchema, pk []byte, oldBody, newBody map[string]interface{}) error {
	for _, idx := range indexes {
		// 1. 计算旧键和新键
		oldValues_ := extractValues(idx.Columns, oldBody)
		newValues_ := extractValues(idx.Columns, newBody)

		// 2. 如果旧键存在 (意味着所有列都存在)，则将其删除
		if oldValues_ != nil {
			oldKey, err := EncodeKey(tableID, idx.ID, oldValues_, pk)
			if err != nil {
				return fmt.Errorf("failed to encode old index key: %w", err)
			}
			if err := txn.Delete(oldKey); err != nil {
				return err
			}
		}

		// 3. 如果新键存在，则设置它
		if newValues_ != nil {
			newKey, err := EncodeKey(tableID, idx.ID, newValues_, pk)
			if err != nil {
				return fmt.Errorf("failed to encode new index key: %w", err)
			}
			// 在该值中存储 PK 以便在扫描期间更容易检索
			if err := txn.Set(newKey, pk, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

// extractValues 返回列的值。
// 如果正文中缺少任何列，则返回 nil (意味着此行不应为此复合索引编制索引？
// 或者是索引为 Null？为了简化 V1：仅当所有字段都存在时才索引)。
func extractValues(cols []string, body map[string]interface{}) []interface{} {
	vals := make([]interface{}, len(cols))
	for i, col := range cols {
		v, ok := body[col]
		if !ok || v == nil {
			return nil
		}
		vals[i] = v
	}
	return vals
}
