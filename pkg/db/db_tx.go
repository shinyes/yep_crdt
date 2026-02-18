package db

import "github.com/shinyes/yep_crdt/pkg/store"

// Table 返回绑定到当前事务的表句柄。
// 如果表不存在，返回 nil。
func (tx *Tx) Table(name string) *Table {
	// 获取基础表信息
	baseTable := tx.db.Table(name)
	if baseTable == nil {
		return nil
	}

	// 创建一个新的 Table 实例，注入当前事务
	return &Table{
		db:           baseTable.db,
		schema:       baseTable.schema,
		indexManager: baseTable.indexManager,
		tx:           tx.txn,
		txCtx:        tx,
	}
}

// Update 执行读写事务。
func (db *DB) Update(fn func(*Tx) error) error {
	tx := &Tx{db: db}
	err := db.store.Update(func(txn store.Tx) error {
		tx.txn = txn
		return fn(tx)
	})
	if err != nil {
		tx.runAfterRollback()
		return err
	}
	tx.runAfterCommit()
	return nil
}

// View 执行只读事务。
func (db *DB) View(fn func(*Tx) error) error {
	return db.store.View(func(txn store.Tx) error {
		return fn(&Tx{db: db, txn: txn})
	})
}

func (tx *Tx) onCommit(fn func()) {
	if tx == nil || fn == nil {
		return
	}
	tx.afterCommit = append(tx.afterCommit, fn)
}

func (tx *Tx) onRollback(fn func()) {
	if tx == nil || fn == nil {
		return
	}
	tx.afterRollback = append(tx.afterRollback, fn)
}

func (tx *Tx) runAfterCommit() {
	if tx == nil {
		return
	}
	for _, fn := range tx.afterCommit {
		if fn != nil {
			fn()
		}
	}
	tx.afterCommit = nil
	tx.afterRollback = nil
}

func (tx *Tx) runAfterRollback() {
	if tx == nil {
		return
	}
	for i := len(tx.afterRollback) - 1; i >= 0; i-- {
		fn := tx.afterRollback[i]
		if fn != nil {
			fn()
		}
	}
	tx.afterRollback = nil
	tx.afterCommit = nil
}
