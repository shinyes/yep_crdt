package store

import (
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type BadgerStore struct {
	db *badger.DB
}

const defaultBadgerValueLogFileSize = 128 * 1024 * 1024 // 128MB

type badgerConfig struct {
	valueLogFileSize int64
}

// BadgerOption customizes how Badger is opened.
type BadgerOption func(*badgerConfig) error

// WithBadgerValueLogFileSize sets max bytes per value log (vlog) file.
func WithBadgerValueLogFileSize(sizeBytes int64) BadgerOption {
	return func(cfg *badgerConfig) error {
		if sizeBytes <= 0 {
			return fmt.Errorf("badger value log file size must be > 0, got %d", sizeBytes)
		}
		cfg.valueLogFileSize = sizeBytes
		return nil
	}
}

// NewBadgerStore creates a Badger-backed store.
func NewBadgerStore(path string, options ...BadgerOption) (*BadgerStore, error) {
	cfg := badgerConfig{
		valueLogFileSize: defaultBadgerValueLogFileSize,
	}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}

	opts := badger.DefaultOptions(path)
	opts = opts.WithValueLogFileSize(cfg.valueLogFileSize)
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

// BadgerTx implements Tx.
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
	it := tx.txn.NewIterator(bOpts)
	return &BadgerIterator{it: it}
}

// BadgerIterator implements Iterator.
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
