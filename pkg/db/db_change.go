package db

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

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

// OnObservedChangeDetailed 注册“已提交变更”回调。
// 本地写入与远端 MergeRawRow 成功落库后都会触发。
// 该回调仅用于观察，不应用于再次广播数据。
func (db *DB) OnObservedChangeDetailed(fn ChangeEventCallback) {
	db.onChangeMu.Lock()
	defer db.onChangeMu.Unlock()
	db.onObservedEventCallbacks = append(db.onObservedEventCallbacks, fn)
}

// notifyChange 触发所有变更回调（内部使用）。
func (db *DB) notifyChange(tableName string, key uuid.UUID) {
	db.notifyChangeWithColumns(tableName, key, nil)
}

func (db *DB) notifyChangeWithColumns(tableName string, key uuid.UUID, columns []string) {
	db.onChangeMu.RLock()
	callbacks := append([]ChangeCallback(nil), db.onChangeCallbacks...)
	eventCallbacks := append([]ChangeEventCallback(nil), db.onChangeEventCallbacks...)
	observedEventCallbacks := append([]ChangeEventCallback(nil), db.onObservedEventCallbacks...)
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
	db.dispatchObservedChange(event, observedEventCallbacks)
}

func (db *DB) NotifyObservedChangeDetailed(tableName string, key uuid.UUID, columns []string) {
	db.onChangeMu.RLock()
	observedEventCallbacks := append([]ChangeEventCallback(nil), db.onObservedEventCallbacks...)
	db.onChangeMu.RUnlock()

	event := ChangeEvent{
		TableName: tableName,
		Key:       key,
		Columns:   append([]string(nil), columns...),
	}
	db.dispatchObservedChange(event, observedEventCallbacks)
}

func (db *DB) CurrentObservedChange() ObservedChange {
	db.observedMu.Lock()
	defer db.observedMu.Unlock()
	return cloneObservedChange(db.observedState)
}

func (db *DB) WaitObservedChange(afterSequence uint64, timeout time.Duration) (ObservedChange, bool) {
	return db.waitObservedChangeMatching(afterSequence, timeout, nil)
}

func (db *DB) WaitObservedChangeForTable(tableName string, afterSequence uint64, timeout time.Duration) (ObservedChange, bool) {
	return db.WaitObservedChangeForTableColumns(tableName, nil, afterSequence, timeout)
}

func (db *DB) WaitObservedChangeForTableColumns(
	tableName string,
	columns []string,
	afterSequence uint64,
	timeout time.Duration,
) (ObservedChange, bool) {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return db.WaitObservedChange(afterSequence, timeout)
	}
	filteredColumns := normalizeObservedColumns(columns)
	return db.waitObservedChangeMatching(afterSequence, timeout, func(event ChangeEvent) bool {
		if event.TableName != tableName {
			return false
		}
		if len(filteredColumns) == 0 || len(event.Columns) == 0 {
			return true
		}
		for _, column := range event.Columns {
			if _, ok := filteredColumns[column]; ok {
				return true
			}
		}
		return false
	})
}

func (db *DB) waitObservedChangeMatching(
	afterSequence uint64,
	timeout time.Duration,
	match func(ChangeEvent) bool,
) (ObservedChange, bool) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		db.observedMu.Lock()
		current := cloneObservedChange(db.observedState)
		if current.Sequence > afterSequence && (match == nil || match(current.Event)) {
			db.observedMu.Unlock()
			return current, true
		}
		ch := db.observedCh
		db.observedMu.Unlock()
		if current.Sequence > afterSequence {
			afterSequence = current.Sequence
		}

		select {
		case <-ch:
		case <-deadline.C:
			return current, false
		}
	}
}

func (db *DB) dispatchObservedChange(event ChangeEvent, callbacks []ChangeEventCallback) {
	db.recordObservedChange(event)
	for _, fn := range callbacks {
		fn(event)
	}
}

func (db *DB) recordObservedChange(event ChangeEvent) {
	db.observedMu.Lock()
	ch := db.observedCh
	db.observedState = ObservedChange{
		Sequence: db.observedState.Sequence + 1,
		Event: ChangeEvent{
			TableName: event.TableName,
			Key:       event.Key,
			Columns:   append([]string(nil), event.Columns...),
		},
	}
	db.observedCh = make(chan struct{})
	db.observedMu.Unlock()
	close(ch)
}

func cloneObservedChange(change ObservedChange) ObservedChange {
	return ObservedChange{
		Sequence: change.Sequence,
		Event: ChangeEvent{
			TableName: change.Event.TableName,
			Key:       change.Event.Key,
			Columns:   append([]string(nil), change.Event.Columns...),
		},
	}
}

func normalizeObservedColumns(columns []string) map[string]struct{} {
	if len(columns) == 0 {
		return nil
	}
	filtered := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		column = strings.TrimSpace(column)
		if column == "" {
			continue
		}
		filtered[column] = struct{}{}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}
