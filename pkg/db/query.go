package db

import (
	"fmt"
	"strings"

	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/index"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

type Operator string

const (
	OpEq  Operator = "="
	OpGt  Operator = ">"
	OpGte Operator = ">="
	OpLt  Operator = "<"
	OpLte Operator = "<="
	OpNe  Operator = "!="
	OpIn  Operator = "IN"
)

type Condition struct {
	Field string
	Op    Operator
	Value any
}

type Query struct {
	table      *Table
	conditions []Condition
	limit      int
	offset     int
	orderBy    string // Field name
	desc       bool
}

func (t *Table) Where(field string, op Operator, val any) *Query {
	return &Query{
		table: t,
		conditions: []Condition{
			{Field: field, Op: op, Value: val},
		},
	}
}

func (q *Query) And(field string, op Operator, val any) *Query {
	q.conditions = append(q.conditions, Condition{Field: field, Op: op, Value: val})
	return q
}

func (q *Query) Offset(offset int) *Query {
	q.offset = offset
	return q
}

func (q *Query) OrderBy(field string, desc bool) *Query {
	q.orderBy = field
	q.desc = desc
	return q
}

func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	return q
}

func (q *Query) Find() ([]map[string]any, error) {
	// 1. Plan Selection
	bestIndexID, bestPrefix, bestScore, bestRangeCond := q.selectPlan()

	results := make([]map[string]any, 0)

	err := q.table.inTx(false, func(txn store.Tx) error {
		// 2. Execution
		var err error
		if bestScore > 0 {
			// Index Scan
			results, err = q.scanIndex(txn, bestIndexID, bestPrefix, bestRangeCond)
		} else {
			// Fallback: Table Scan
			results, err = q.scanTable(txn)
		}
		return err
	})

	return results, err
}

// FindCRDTs returns the raw MapCRDT objects as ReadOnlyMap interface.
// This allows access to nested CRDTs (like RGA) for iterator usage but prevents modification.
func (q *Query) FindCRDTs() ([]crdt.ReadOnlyMap, error) {
	// 1. Plan Selection
	bestIndexID, bestPrefix, bestScore, bestRangeCond := q.selectPlan()

	results := make([]crdt.ReadOnlyMap, 0)

	err := q.table.inTx(false, func(txn store.Tx) error {
		// 2. Execution
		var err error
		if bestScore > 0 {
			// Index Scan
			results, err = q.scanIndexCRDT(txn, bestIndexID, bestPrefix, bestRangeCond)
		} else {
			// Fallback: Table Scan
			results, err = q.scanTableCRDT(txn)
		}
		return err
	})

	return results, err
}

func (q *Query) selectPlan() (uint32, []any, int, *Condition) {
	var bestIndexID uint32
	var bestPrefix []any
	var bestScore int = -1
	var bestRangeCond *Condition

	for _, idx := range q.table.schema.Indexes {
		currentPrefix := []any{}
		currentScore := 0
		var currentRange *Condition

		// Check columns in order
		for _, col := range idx.Columns {
			cond := q.findCondition(col)
			if cond == nil {
				break // Prefix broken
			}

			if cond.Op == OpEq {
				currentPrefix = append(currentPrefix, cond.Value)
				currentScore++
			} else {
				// Range condition. Must be the last part of prefix usage.
				currentRange = cond
				currentScore++ // Gives extra point? Yes, because we use index for it.
				break
			}
		}

		if currentScore > bestScore {
			bestScore = currentScore
			bestIndexID = idx.ID
			bestPrefix = currentPrefix
			bestRangeCond = currentRange
		}
	}
	return bestIndexID, bestPrefix, bestScore, bestRangeCond
}

func (q *Query) findCondition(col string) *Condition {
	for _, c := range q.conditions {
		if c.Field == col {
			return &c
		}
	}
	return nil
}

// scanIndex returns map[string]any
func (q *Query) scanIndex(txn store.Tx, idxID uint32, prefixValues []any, rangeCond *Condition) ([]map[string]any, error) {
	crdts, err := q.scanIndexCRDT(txn, idxID, prefixValues, rangeCond)
	if err != nil {
		return nil, err
	}
	results := make([]map[string]any, len(crdts))
	for i, c := range crdts {
		results[i] = c.Value().(map[string]any)
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

	results := make([]crdt.ReadOnlyMap, 0)
	count := 0
	skipped := 0
	for ; iter.ValidForPrefix(basePrefix); iter.Next() {
		_, pk, _ := iter.Item()

		// Fetch CRDT
		m, err := q.fetchCRDT(txn, pk)
		if err != nil {
			continue
		}

		// To check conditions, we might need Values.
		// Performance trade-off: We MUST load values to filter.
		// But if we return MapCRDT, the user might NOT access the heavy RGA fields.
		// matches() checks row map[string]any.
		// For filtering, we can call Value().
		// If the query is *only* on indexed fields, we assume index covered it?
		// No, we still need to check other conditions.
		// So we have to materialize the row for filtering.
		// The optimization of FindCRDTs comes when we select HUGE documents
		// but check conditions on small fields.
		// Note: MapCRDT.Value() deserializes everything.
		// To truly optimize, MapCRDT should support partial Value(), or matches() should check CRDT directly.
		// For now, we rely on MapCRDT caching.
		// If valid, append `m`.

		// Optimization: matches() takes map[string]any.
		row := m.Value().(map[string]any)

		if q.matches(row) {
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
		results[i] = c.Value().(map[string]any)
	}
	return results, nil
}

func (q *Query) scanTableCRDT(txn store.Tx) ([]crdt.ReadOnlyMap, error) {
	prefix := []byte(strings.Split(string(q.table.dataKey([]byte("dummy"))), "dummy")[0])

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

	results := make([]crdt.ReadOnlyMap, 0)
	count := 0
	skipped := 0
	for ; iter.ValidForPrefix(prefix); iter.Next() {
		_, val, _ := iter.Item()

		m, err := crdt.FromBytesMap(val)
		if err != nil {
			continue
		}

		// For checking non-indexed conditions, we currently need full Value.
		// Improving this is a future task (Partial Deserialization).
		row := m.Value().(map[string]any)

		if q.matches(row) {
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

func (q *Query) fetchRow(txn store.Tx, pk []byte) (map[string]any, error) {
	m, err := q.fetchCRDT(txn, pk)
	if err != nil {
		return nil, err
	}
	return m.Value().(map[string]any), nil
}

func (q *Query) fetchCRDT(txn store.Tx, pk []byte) (*crdt.MapCRDT, error) {
	key := q.table.dataKey(pk)
	val, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	return crdt.FromBytesMap(val)
}

func (q *Query) matches(row map[string]any) bool {
	for _, cond := range q.conditions {
		val, ok := row[cond.Field]
		if !ok {
			return false
		}

		// Find column type from schema
		colType := meta.ColTypeString // Default fallback
		for _, col := range q.table.schema.Columns {
			if col.Name == cond.Field {
				colType = col.Type
				break
			}
		}

		cmp := compare(val, cond.Value, colType)

		switch cond.Op {
		case OpEq:
			if cmp != 0 {
				return false
			}
		case OpNe:
			if cmp == 0 {
				return false
			}
		case OpGt:
			if cmp <= 0 {
				return false
			}
		case OpGte:
			if cmp < 0 {
				return false
			}
		case OpLt:
			if cmp >= 0 {
				return false
			}
		case OpLte:
			if cmp > 0 {
				return false
			}
		case OpIn:
			found := false
			if list, ok := cond.Value.([]any); ok {
				for _, item := range list {
					if compare(val, item, colType) == 0 {
						found = true
						break
					}
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

func compare(a, b any, t meta.ColumnType) int {
	// Robust comparison based on Schema Type

	if t == meta.ColTypeInt {
		// Attempt numeric comparison
		numA, okA := toFloat(a)
		numB, okB := toFloat(b)
		if okA && okB {
			if numA < numB {
				return -1
			} else if numA > numB {
				return 1
			}
			return 0
		}
	}

	// Default / String comparison for other types or if numeric conversion failed
	strA := toString(a)
	strB := toString(b)
	return strings.Compare(strA, strB)
}

func toFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case []byte:
		// Attempt to convert bytes to string then parse?
		// For now, treat bytes as non-numeric unless we implement strict schema typing
		return 0, false
	case string:
		// Attempt to parse string as float?
		// var f float64
		// if _, err := fmt.Sscanf(val, "%f", &f); err == nil {
		// 	return f, true
		// }
		var f float64
		if _, err := fmt.Sscanf(val, "%f", &f); err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func toString(v any) string {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}
