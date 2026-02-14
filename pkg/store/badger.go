package store

import (
	"time"

	"github.com/dgraph-io/badger/v4"
)

type BadgerStore struct {
	db *badger.DB
}

// NewBadgerStore 创建一个新的 BadgerStore 实例。
func NewBadgerStore(path string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path)
	// 如果需要，优化低内存使用率，或者为了性能保持默认值。
	// 对于多租户场景，我们可能需要进一步调整。
	opts.Logger = nil // 暂时禁用默认日志记录器

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &BadgerStore{db: db}, nil
}

func (s *BadgerStore) Close() error {
	return s.db.Close()
}

func (s *BadgerStore) RunTx(update bool, fn func(Tx) error) error {
	if update {
		return s.db.Update(func(txn *badger.Txn) error {
			return fn(&BadgerTx{txn: txn})
		})
	}
	return s.db.View(func(txn *badger.Txn) error {
		return fn(&BadgerTx{txn: txn})
	})
}

func (s *BadgerStore) View(fn func(Tx) error) error {
	return s.RunTx(false, fn)
}

func (s *BadgerStore) Update(fn func(Tx) error) error {
	return s.RunTx(true, fn)
}

// BadgerTx 实现 Tx 接口
type BadgerTx struct {
	txn *badger.Txn
}

func (tx *BadgerTx) Set(key, value []byte, ttl int64) error {
	if ttl > 0 {
		e := badger.NewEntry(key, value).WithTTL(time.Duration(ttl) * time.Second)
		return tx.txn.SetEntry(e)
	}
	return tx.txn.Set(key, value)
}

func (tx *BadgerTx) Get(key []byte) ([]byte, error) {
	item, err := tx.txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	val, err := item.ValueCopy(nil)
	return val, err
}

func (tx *BadgerTx) Delete(key []byte) error {
	return tx.txn.Delete(key)
}

func (tx *BadgerTx) NewIterator(opts IteratorOptions) Iterator {
	bOpts := badger.DefaultIteratorOptions
	bOpts.Reverse = opts.Reverse
	bOpts.Prefix = opts.Prefix
	// 我们可能默认获取值，因为迭代器通常需要它们。
	// PrefetchSize 可以调整。

	it := tx.txn.NewIterator(bOpts)
	return &BadgerIterator{it: it}
}

// BadgerIterator 实现 Iterator 接口
type BadgerIterator struct {
	it *badger.Iterator
}

func (i *BadgerIterator) Seek(key []byte) {
	i.it.Seek(key)
}

func (i *BadgerIterator) Rewind() {
	i.it.Rewind()
}

func (i *BadgerIterator) Valid() bool {
	return i.it.Valid()
}

func (i *BadgerIterator) ValidForPrefix(prefix []byte) bool {
	return i.it.ValidForPrefix(prefix)
}

func (i *BadgerIterator) Next() {
	i.it.Next()
}

func (i *BadgerIterator) Item() ([]byte, []byte, error) {
	item := i.it.Item()
	k := item.KeyCopy(nil)
	v, err := item.ValueCopy(nil)
	return k, v, err
}

func (i *BadgerIterator) Close() {
	i.it.Close()
}
