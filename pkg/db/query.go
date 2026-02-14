package db

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
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
	// conditionFirstIndex keeps the first condition position per field.
	// This preserves existing behavior while avoiding repeated linear scans in selectPlan.
	conditionFirstIndex map[string]int
	columnTypes         map[string]meta.ColumnType
	limit               int
	offset              int
	orderBy             string // Field name
	desc                bool
}

func (t *Table) Where(field string, op Operator, val any) *Query {
	return &Query{
		table: t,
		conditions: []Condition{
			{Field: field, Op: op, Value: val},
		},
		conditionFirstIndex: map[string]int{field: 0},
		columnTypes:         buildColumnTypeMap(t.schema),
	}
}

func buildColumnTypeMap(schema *meta.TableSchema) map[string]meta.ColumnType {
	if schema == nil {
		return nil
	}
	m := make(map[string]meta.ColumnType, len(schema.Columns))
	for _, col := range schema.Columns {
		m[col.Name] = col.Type
	}
	return m
}

func (q *Query) And(field string, op Operator, val any) *Query {
	q.conditions = append(q.conditions, Condition{Field: field, Op: op, Value: val})
	if q.conditionFirstIndex == nil {
		q.rebuildConditionIndex()
	} else if _, exists := q.conditionFirstIndex[field]; !exists {
		q.conditionFirstIndex[field] = len(q.conditions) - 1
	}
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

	capHint := 0
	if q.limit > 0 {
		capHint = q.limit
	}
	results := make([]crdt.ReadOnlyMap, 0, capHint)

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
	if q.conditionFirstIndex == nil {
		q.rebuildConditionIndex()
	}
	if idx, ok := q.conditionFirstIndex[col]; ok && idx >= 0 && idx < len(q.conditions) {
		return &q.conditions[idx]
	}
	return nil
}

func (q *Query) rebuildConditionIndex() {
	q.conditionFirstIndex = make(map[string]int, len(q.conditions))
	for i, cond := range q.conditions {
		if _, exists := q.conditionFirstIndex[cond.Field]; !exists {
			q.conditionFirstIndex[cond.Field] = i
		}
	}
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
		results[i] = c.Value().(map[string]any)
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
	return m.Value().(map[string]any), nil
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

func (q *Query) matches(row crdt.ReadOnlyMap) bool {
	for _, cond := range q.conditions {
		// Lazy load optimization: Only fetch the specific field needed for this condition
		val, ok := row.Get(cond.Field)
		if !ok {
			return false
		}

		// Find column type from prebuilt map
		colType := meta.ColTypeString // Default fallback
		if t, exists := q.columnTypes[cond.Field]; exists {
			colType = t
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
	switch t {
	case meta.ColTypeInt:
		// Strict numeric comparison
		numA, okA := toFloat(a) // Helper handles int/int64/float variants
		numB, okB := toFloat(b)
		if !okA || !okB {
			// Type mismatch for schema type Int -> treat as not equal (or handled upstream?)
			// Returning -2 to indicate error? simpler: fallback to string or inequalities fail.
			// Let's fallback to string comparison if types are wildly different,
			// to maintain legacy "try best effort" but safer.
			// Actually, for strict safety, if schema says Int and we can't parse, it's invalid.
			// However `matches` expects -1, 0, 1.
			// If type mismatch, they are definitely not equal.
			// For ordering (>, <), undefined behavior if types mismatch.
			// Let's return a stable non-zero.
			return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
		}
		if numA < numB {
			return -1
		} else if numA > numB {
			return 1
		}
		return 0

	case meta.ColTypeString:
		// Strict string comparison
		strA, okA := a.(string)
		if !okA {
			// If source is []byte (common in our storage), convert.
			if bBytes, ok := a.([]byte); ok {
				strA = string(bBytes)
			} else {
				strA = fmt.Sprintf("%v", a)
			}
		}
		strB, okB := b.(string)
		if !okB {
			if bBytes, ok := b.([]byte); ok {
				strB = string(bBytes)
			} else {
				strB = fmt.Sprintf("%v", b)
			}
		}
		return strings.Compare(strA, strB)

	default:
		// Fallback for unknown types
		return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
	}
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
	case string:
		return parseNumericText(val)
	case []byte:
		// Attempt to parse bytes as numeric text since storage is often LWWRegister bytes.
		return parseNumericText(string(val))
	default:
		return 0, false
	}
}

func parseNumericText(text string) (float64, bool) {
	if text == "" {
		return 0, false
	}
	if i, err := strconv.ParseInt(text, 10, 64); err == nil {
		return float64(i), true
	}
	if u, err := strconv.ParseUint(text, 10, 64); err == nil {
		return float64(u), true
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}
