package db

import (
	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func (t *Table) Get(key uuid.UUID) (map[string]any, error) {
	if err := validateUUIDv7(key); err != nil {
		return nil, err
	}

	var res map[string]any
	err := t.inTx(false, func(txn store.Tx) error {
		val, err := txn.Get(t.dataKey(key))
		if err != nil {
			return err
		}
		m, err := crdt.FromBytesMap(val)
		if err != nil {
			return err
		}
		res, err = t.decodeRowForResult(m.Value().(map[string]any))
		return err
	})
	return res, err
}

// GetCRDT returns the raw ReadOnlyMap CRDT for a given key.
// This is useful for accessing nested CRDTs (like RGA) without loading the entire map value.
func (t *Table) GetCRDT(key uuid.UUID) (crdt.ReadOnlyMap, error) {
	if err := validateUUIDv7(key); err != nil {
		return nil, err
	}

	var res crdt.ReadOnlyMap
	err := t.inTx(false, func(txn store.Tx) error {
		m, _, err := t.loadRow(txn, key)
		if err != nil {
			return err
		}
		res = m
		return nil
	})
	return res, err
}

// ScanRowDigest 扫描表中所有行，返回每行的摘要信息。
// 用于版本沟通时快速比较两个节点的数据差异。
func (t *Table) ScanRowDigest() ([]RowDigest, error) {
	var digests []RowDigest

	err := t.inTx(false, func(txn store.Tx) error {
		prefix := t.tablePrefix()
		iterator := txn.NewIterator(store.IteratorOptions{Prefix: prefix})
		defer iterator.Close()

		iterator.Seek(prefix)
		for iterator.ValidForPrefix(prefix) {
			keyRaw, valBytes, err := iterator.Item()
			if err != nil {
				iterator.Next()
				continue
			}

			// 提取 UUID
			uidBytes := keyRaw[len(prefix):]
			if len(uidBytes) != 16 {
				iterator.Next()
				continue
			}

			key, err := uuid.FromBytes(uidBytes)
			if err != nil {
				iterator.Next()
				continue
			}

			// FNV-1a 哈希（快速且分布均匀）
			h := fnv32a(valBytes)

			digests = append(digests, RowDigest{
				Key:  key,
				Hash: h,
			})

			iterator.Next()
		}
		return nil
	})

	return digests, err
}

// fnv32a FNV-1a 哈希算法（用于快速数据比较）
func fnv32a(data []byte) uint32 {
	const (
		offset32 = uint32(2166136261)
		prime32  = uint32(16777619)
	)
	h := offset32
	for _, b := range data {
		h ^= uint32(b)
		h *= prime32
	}
	return h
}
