package db

import (
	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/index"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// scanIndex returns map[string]any
func (q *Query) scanIndex(txn store.Tx, idxID uint32, prefixValues []any, rangeCond *Condition) ([]map[string]any, error) {
	crdts, err := q.scanIndexCRDT(txn, idxID, prefixValues, rangeCond)
	if err != nil {
		return nil, err
	}
	results := make([]map[string]any, len(crdts))
	for i, c := range crdts {
		row, err := q.table.decodeRowForResult(c.Value().(map[string]any))
		if err != nil {
			return nil, err
		}
		results[i] = row
	}
	return results, nil
}

func (q *Query) scanIndexCRDT(txn store.Tx, idxID uint32, prefixValues []any, rangeCond *Condition) ([]crdt.ReadOnlyMap, error) {
	basePrefix, err := index.EncodePrefix(q.table.schema.ID, idxID, prefixValues)
	if err != nil {
		return nil, err
	}

	opts := store.IteratorOptions{
		Reverse: q.desc,
	}

	iter := txn.NewIterator(opts)
	defer iter.Close()

	seekKey := basePrefix
	if rangeCond != nil && !q.desc {
		seekValues := make([]any, len(prefixValues))
		copy(seekValues, prefixValues)
		if rangeCond.Op == OpGt || rangeCond.Op == OpGte {
			seekValues = append(seekValues, rangeCond.Value)
			sk, err := index.EncodePrefix(q.table.schema.ID, idxID, seekValues)
			if err == nil {
				seekKey = sk
			}
		}
	} else if q.desc {
		seekKey = append(basePrefix, 0xFF)
	}

	iter.Seek(seekKey)

	capHint := 0
	if q.limit > 0 {
		capHint = q.limit
	}
	results := make([]crdt.ReadOnlyMap, 0, capHint)
	count := 0
	skipped := 0
	for ; iter.ValidForPrefix(basePrefix); iter.Next() {
		_, pkBytes, _ := iter.Item()

		// Fetch CRDT
		pk, err := uuid.FromBytes(pkBytes)
		if err != nil {
			continue
		}
		m, err := q.fetchCRDT(txn, pk)
		if err != nil {
			continue
		}

		// Optimization: matches() takes ReadOnlyMap and checks fields lazily
		if q.matches(m) {
			if skipped < q.offset {
				skipped++
				continue
			}
			results = append(results, m)
			count++
			if q.limit > 0 && count >= q.limit {
				break
			}
		}
	}
	return results, nil
}

// scanTable returns map[string]any
func (q *Query) scanTable(txn store.Tx) ([]map[string]any, error) {
	crdts, err := q.scanTableCRDT(txn)
	if err != nil {
		return nil, err
	}
	results := make([]map[string]any, len(crdts))
	for i, c := range crdts {
		row, err := q.table.decodeRowForResult(c.Value().(map[string]any))
		if err != nil {
			return nil, err
		}
		results[i] = row
	}
	return results, nil
}

func (q *Query) scanTableCRDT(txn store.Tx) ([]crdt.ReadOnlyMap, error) {
	prefix := q.table.tablePrefix()

	opts := store.IteratorOptions{
		Prefix:  prefix,
		Reverse: q.desc,
	}

	iter := txn.NewIterator(opts)
	defer iter.Close()

	if q.desc {
		iter.Seek(append(prefix, 0xFF))
	} else {
		iter.Seek(prefix)
	}

	capHint := 0
	if q.limit > 0 {
		capHint = q.limit
	}
	results := make([]crdt.ReadOnlyMap, 0, capHint)
	count := 0
	skipped := 0
	for ; iter.ValidForPrefix(prefix); iter.Next() {
		_, val, _ := iter.Item()

		m, err := crdt.FromBytesMap(val)
		if err != nil {
			continue
		}
		if q.table.db.FileStorageDir != "" {
			m.SetBaseDir(q.table.db.FileStorageDir)
		}

		// Optimization: matches() takes ReadOnlyMap
		if q.matches(m) {
			if skipped < q.offset {
				skipped++
				continue
			}
			results = append(results, m)
			count++
			if q.limit > 0 && count >= q.limit {
				break
			}
		}
	}
	return results, nil
}

func (q *Query) fetchRow(txn store.Tx, pk uuid.UUID) (map[string]any, error) {
	m, err := q.fetchCRDT(txn, pk)
	if err != nil {
		return nil, err
	}
	return q.table.decodeRowForResult(m.Value().(map[string]any))
}

func (q *Query) fetchCRDT(txn store.Tx, pk uuid.UUID) (*crdt.MapCRDT, error) {
	key := q.table.dataKey(pk)
	val, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	m, err := crdt.FromBytesMap(val)
	if err == nil && q.table.db.FileStorageDir != "" {
		m.SetBaseDir(q.table.db.FileStorageDir)
	}
	return m, err
}
