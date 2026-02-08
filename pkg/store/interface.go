package store

import (
	"errors"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

// Store 代表底层 KV 存储接口 (例如 BadgerDB)。
// 每个“租户/数据库”将拥有一个 Store 实例。
type Store interface {
	// Close 关闭存储。
	Close() error

	// RunTx 运行事务。
	// 如果 update 为 true，则为读写事务。否则为只读事务。
	RunTx(update bool, fn func(Tx) error) error

	// View 执行只读事务。
	View(fn func(Tx) error) error

	// Update 执行读写事务。
	Update(fn func(Tx) error) error
}

// Tx 代表事务。
type Tx interface {
	// Set 设置键的值。
	// ttl 是可选的，0 表示无 TTL。
	Set(key, value []byte, ttl int64) error

	// Get 获取键的值。
	// 如果键不存在返回 ErrKeyNotFound。
	Get(key []byte) ([]byte, error)

	// Delete 删除键。
	Delete(key []byte) error

	// NewIterator 使用选项创建新的迭代器。
	NewIterator(opts IteratorOptions) Iterator
}

// IteratorOptions 定义迭代器的选项。
type IteratorOptions struct {
	Prefix  []byte
	Reverse bool // 如果为 true，则按反序迭代。
}

// Iterator 遍历存储中的键。
type Iterator interface {
	// Seek 将迭代器移动到第一个 >= key 的键。
	Seek(key []byte)

	// Rewind 将迭代器移动到范围的开头。
	Rewind()

	// Valid 如果迭代器指向有效的键，则返回 true。
	Valid() bool

	// ValidForPrefix 如果迭代器指向带有给定前缀的有效键，则返回 true。
	ValidForPrefix(prefix []byte) bool

	// Next 将迭代器移动到下一个键。
	Next()

	// Item 返回当前项（键和值）。
	Item() (key, value []byte, err error)

	// Close 关闭迭代器。
	Close()
}
