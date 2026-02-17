package db

import "github.com/shinyes/yep_crdt/pkg/meta"

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
