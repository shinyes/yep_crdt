package db

import (
	"bytes"
	"fmt"

	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/meta"
)

func (q *Query) matches(row crdt.ReadOnlyMap) bool {
	for _, cond := range q.conditions {
		// Lazy load optimization: only fetch the field needed for this condition.
		val, ok := row.Get(cond.Field)
		if !ok {
			return false
		}

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
	switch normalizeColumnType(t) {
	case meta.ColTypeInt:
		av, okA := parseInt64Like(a)
		bv, okB := parseInt64Like(b)
		if !okA || !okB {
			return fallbackCompare(a, b)
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0

	case meta.ColTypeFloat:
		av, okA := parseFloat64Like(a)
		bv, okB := parseFloat64Like(b)
		if !okA || !okB {
			return fallbackCompare(a, b)
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0

	case meta.ColTypeBool:
		av, okA := parseBoolLike(a)
		bv, okB := parseBoolLike(b)
		if !okA || !okB {
			return fallbackCompare(a, b)
		}
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1
		}
		return 1

	case meta.ColTypeTimestamp:
		av, okA := parseTimestampLike(a)
		bv, okB := parseTimestampLike(b)
		if !okA || !okB {
			return fallbackCompare(a, b)
		}
		if av.Before(bv) {
			return -1
		}
		if av.After(bv) {
			return 1
		}
		return 0

	case meta.ColTypeBytes:
		av, okA := bytesFromAny(a)
		bv, okB := bytesFromAny(b)
		if !okA || !okB {
			return fallbackCompare(a, b)
		}
		return bytes.Compare(av, bv)

	case meta.ColTypeString:
		return bytes.Compare([]byte(stringify(a)), []byte(stringify(b)))

	default:
		return fallbackCompare(a, b)
	}
}

func stringify(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}
