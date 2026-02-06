package store

import (
	"github.com/dgraph-io/badger/v3"
)

type BadgerStore struct {
	db *badger.DB
}

func NewBadgerStore(path string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path)
	// 减少库使用的日志输出
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerStore{db: db}, nil
}

func (s *BadgerStore) Close() error {
	return s.db.Close()
}

func (s *BadgerStore) Get(key []byte) ([]byte, error) {
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return val, err
}

func (s *BadgerStore) Set(key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (s *BadgerStore) Delete(key []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (s *BadgerStore) Scan(prefix []byte, fn func(k, v []byte) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := fn(k, v); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerStore) NewTransaction(update bool) Transaction {
	return &BadgerTxn{
		txn: s.db.NewTransaction(update),
	}
}

type BadgerTxn struct {
	txn *badger.Txn
}

func (t *BadgerTxn) Get(key []byte) ([]byte, error) {
	item, err := t.txn.Get(key)
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (t *BadgerTxn) Set(key, value []byte) error {
	return t.txn.Set(key, value)
}

func (t *BadgerTxn) Delete(key []byte) error {
	return t.txn.Delete(key)
}

func (t *BadgerTxn) Commit() error {
	return t.txn.Commit()
}

func (t *BadgerTxn) Discard() {
	t.txn.Discard()
}

func (t *BadgerTxn) Scan(prefix []byte, fn func(k, v []byte) error) error {
	it := t.txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		v, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}
