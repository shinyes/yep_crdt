package store

// Store 定义了底层键值存储的接口。
// 它支持基本的 KV 操作和事务。
type Store interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
	Close() error

	// Scan 迭代具有给定前缀的键。
	Scan(prefix []byte, fn func(k, v []byte) error) error

	// NewTransaction 创建一个新的事务。
	NewTransaction(update bool) Transaction
}

type Transaction interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
	Commit() error
	Discard()

	// Scan 在事务内进行迭代。
	Scan(prefix []byte, fn func(k, v []byte) error) error
}

// BlobStore 定义内容寻址存储 (CAS) 的接口。
type BlobStore interface {
	// Put 存储数据并返回其哈希值。
	Put(data []byte) (string, error)
	// Get 通过哈希值检索数据。
	Get(hash string) ([]byte, error)
	// Has 检查 blob 是否存在。
	Has(hash string) (bool, error)
}
