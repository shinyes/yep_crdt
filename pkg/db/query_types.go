package db

import (
	"fmt"

	"github.com/shinyes/yep_crdt/pkg/meta"
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
	// It is used by normalization/indexing helpers and rebuilt when needed.
	conditionFirstIndex map[string]int
	columnTypes         map[string]meta.ColumnType
	limit               int
	offset              int
	orderBy             string // Field name
	desc                bool
	// benchmark/debug knob: when true, disables index-key range pre-filter.
	disableIndexRangePreFilter bool
	normalizationErr           error
}

func (t *Table) Where(field string, op Operator, val any) *Query {
	q := &Query{
		table: t,
		conditions: []Condition{
			{Field: field, Op: op, Value: val},
		},
		conditionFirstIndex: map[string]int{field: 0},
		columnTypes:         buildColumnTypeMap(t.schema),
	}
	q.conditions[0].Value = q.normalizeConditionValue(field, op, q.conditions[0].Value)
	return q
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
	q.conditions = append(q.conditions, Condition{
		Field: field,
		Op:    op,
		Value: q.normalizeConditionValue(field, op, val),
	})
	if q.conditionFirstIndex == nil {
		q.rebuildConditionIndex()
	} else if _, exists := q.conditionFirstIndex[field]; !exists {
		q.conditionFirstIndex[field] = len(q.conditions) - 1
	}
	return q
}

func (q *Query) normalizeConditionValue(field string, op Operator, value any) any {
	colType, exists := q.columnTypes[field]
	if !exists {
		return value
	}
	if op == OpIn {
		list, ok := value.([]any)
		if !ok {
			q.recordNormalizationError(fmt.Errorf("field %q with OpIn expects []any, got %T", field, value))
			return value
		}
		normalizedList := make([]any, len(list))
		for i, item := range list {
			normalized, err := normalizeValueByColumnType(colType, item)
			if err != nil {
				q.recordNormalizationError(fmt.Errorf("normalize OpIn item %d for field %q failed: %w", i, field, err))
				return value
			}
			normalizedList[i] = normalized
		}
		return normalizedList
	}
	normalized, err := normalizeValueByColumnType(colType, value)
	if err != nil {
		q.recordNormalizationError(fmt.Errorf("normalize condition for field %q failed: %w", field, err))
		return value
	}
	return normalized
}

func (q *Query) recordNormalizationError(err error) {
	if err == nil || q.normalizationErr != nil {
		return
	}
	q.normalizationErr = err
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
