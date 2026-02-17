package db

import "github.com/google/uuid"

// OnChange 注册数据变更回调。
// 当 Table.Set() 或 Table.Add() 成功写入后触发。
// MergeRawRow() 不会触发（避免远程数据合并时循环广播）。
func (db *DB) OnChange(fn ChangeCallback) {
	db.onChangeMu.Lock()
	defer db.onChangeMu.Unlock()
	db.onChangeCallbacks = append(db.onChangeCallbacks, fn)
}

func (db *DB) OnChangeDetailed(fn ChangeEventCallback) {
	db.onChangeMu.Lock()
	defer db.onChangeMu.Unlock()
	db.onChangeEventCallbacks = append(db.onChangeEventCallbacks, fn)
}

// notifyChange 触发所有变更回调（内部使用）。
func (db *DB) notifyChange(tableName string, key uuid.UUID) {
	db.notifyChangeWithColumns(tableName, key, nil)
}

func (db *DB) notifyChangeWithColumns(tableName string, key uuid.UUID, columns []string) {
	db.onChangeMu.RLock()
	callbacks := append([]ChangeCallback(nil), db.onChangeCallbacks...)
	eventCallbacks := append([]ChangeEventCallback(nil), db.onChangeEventCallbacks...)
	db.onChangeMu.RUnlock()

	for _, fn := range callbacks {
		fn(tableName, key)
	}

	event := ChangeEvent{
		TableName: tableName,
		Key:       key,
		Columns:   append([]string(nil), columns...),
	}
	for _, fn := range eventCallbacks {
		fn(event)
	}
}
