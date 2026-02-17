package db

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/meta"
)

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
