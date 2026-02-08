package db

import (
	"fmt"
	"strings"

	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/index"
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
	Value interface{}
}

type Query struct {
	table      *Table
	conditions []Condition
	limit      int
	offset     int
	orderBy    string // Field name
	desc       bool
}

func (t *Table) Where(field string, op Operator, val interface{}) *Query {
	return &Query{
		table: t,
		conditions: []Condition{
			{Field: field, Op: op, Value: val},
		},
	}
}

func (q *Query) And(field string, op Operator, val interface{}) *Query {
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

func (q *Query) Find() ([]map[string]interface{}, error) {
	// 1. Plan Selection
	// Find best index based on conditions (Longest Prefix Match).

	// type plan struct {
	// 	indexID      uint32
	// 	prefixValues []interface{}
	// 	score        int // Number of matched columns
	//  rangeCond    *Condition // If last matched column is a range query
	// }

	var bestIndexID uint32
	var bestPrefix []interface{}
	var bestScore int = -1
	var bestRangeCond *Condition

	for _, idx := range q.table.schema.Indexes {
		currentPrefix := []interface{}{}
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
				// But we stop here, we can't use further columns for prefix seek.
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

	results := make([]map[string]interface{}, 0)

	err := q.table.inTx(false, func(txn store.Tx) error {
		// 2. Execution
		if bestScore > 0 {
			// Index Scan
			return q.scanIndex(txn, bestIndexID, bestPrefix, bestRangeCond, &results)
		}

		// Fallback: Table Scan
		return q.scanTable(txn, &results)
	})

	return results, err
}

func (q *Query) findCondition(col string) *Condition {
	for _, c := range q.conditions {
		if c.Field == col {
			return &c
		}
	}
	return nil
}

func (q *Query) scanIndex(txn store.Tx, idxID uint32, prefixValues []interface{}, rangeCond *Condition, results *[]map[string]interface{}) error {
	basePrefix, err := index.EncodePrefix(q.table.schema.ID, idxID, prefixValues)
	if err != nil {
		return err
	}

	opts := store.IteratorOptions{
		Reverse: q.desc,
	}

	iter := txn.NewIterator(opts)
	defer iter.Close()

	// Seek Logic
	// If NOT reverse, we seek to specific range start.
	// If Reverse, we might need to seek to end of range?
	// For MVP: Simple seek to prefix for now, optimizing reverse range seek is complex.
	// But if we have range condition (e.g. Age > 20) and Reverse=true, we need to be careful.
	// Let's keep simple Prefix scan for now.

	seekKey := basePrefix
	if rangeCond != nil && !q.desc {
		// Only optimize forward seek with range for now
		seekValues := make([]interface{}, len(prefixValues))
		copy(seekValues, prefixValues)
		if rangeCond.Op == OpGt || rangeCond.Op == OpGte {
			seekValues = append(seekValues, rangeCond.Value)
			sk, err := index.EncodePrefix(q.table.schema.ID, idxID, seekValues)
			if err == nil {
				seekKey = sk
			}
		}
	} else if q.desc {
		// For reverse, we might want to seek to the "end" of the prefix?
		// Badger's Reverse iterator + Seek(prefix + 0xFF) usually works.
		// Construct a key that is strictly greater than prefix?
		seekKey = append(basePrefix, 0xFF)
	}

	iter.Seek(seekKey)

	count := 0
	skipped := 0
	for ; iter.ValidForPrefix(basePrefix); iter.Next() {
		_, pk, _ := iter.Item()

		// Fetch Row
		row, err := q.fetchRow(txn, pk)
		if err != nil {
			continue
		}

		if q.matches(row) {
			if skipped < q.offset {
				skipped++
				continue
			}
			*results = append(*results, row)
			count++
			if q.limit > 0 && count >= q.limit {
				break
			}
		}
	}
	return nil
}

func (q *Query) scanTable(txn store.Tx, results *[]map[string]interface{}) error {
	prefix := []byte(strings.Split(string(q.table.dataKey([]byte("dummy"))), "dummy")[0])

	opts := store.IteratorOptions{
		Prefix:  prefix,
		Reverse: q.desc, // Support Order By PK desc
	}

	iter := txn.NewIterator(opts)
	defer iter.Close()

	if q.desc {
		iter.Seek(append(prefix, 0xFF))
	} else {
		iter.Seek(prefix)
	}

	count := 0
	skipped := 0
	for ; iter.ValidForPrefix(prefix); iter.Next() {
		_, val, _ := iter.Item()

		m, err := crdt.FromBytesMap(val)
		if err != nil {
			continue
		}
		row := m.Value().(map[string]interface{})

		if q.matches(row) {
			if skipped < q.offset {
				skipped++
				continue
			}
			*results = append(*results, row)
			count++
			if q.limit > 0 && count >= q.limit {
				break
			}
		}
	}
	return nil
}

func (q *Query) fetchRow(txn store.Tx, pk []byte) (map[string]interface{}, error) {
	key := q.table.dataKey(pk)
	val, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	m, err := crdt.FromBytesMap(val)
	if err != nil {
		return nil, err
	}
	return m.Value().(map[string]interface{}), nil
}

func (q *Query) matches(row map[string]interface{}) bool {
	for _, cond := range q.conditions {
		val, ok := row[cond.Field]
		if !ok {
			return false
		}

		cmp := compare(val, cond.Value)

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
			if list, ok := cond.Value.([]interface{}); ok {
				for _, item := range list {
					if compare(val, item) == 0 {
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

func compare(a, b interface{}) int {
	// Robust comparison handling []byte and string
	strA := toString(a)
	strB := toString(b)

	// Try numeric comparison if both look like numbers?
	// For MVP, strictly string comparison unless we implement type-aware schema logic.
	// But in test: Age is int.
	// We should try to cast to int if possible for numeric fields in schema.
	// But here we don't have schema type info handy in `compare`.
	// Let's rely on `toString` doing the right thing for now.
	// EXCEPT: "30" > "20" works. "100" < "20" fails.
	// Hack: if both are int/int64/float, compare numerically.

	// Try numeric comparison
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

	return strings.Compare(strA, strB)
}

func toFloat(v interface{}) (float64, bool) {
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
		return 0, false
	default:
		return 0, false
	}
}

func toString(v interface{}) string {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}
