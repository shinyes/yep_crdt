package db

import (
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/store"
)

func (q *Query) Find() ([]map[string]any, error) {
	if q.normalizationErr != nil {
		return nil, q.normalizationErr
	}

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
	if q.normalizationErr != nil {
		return nil, q.normalizationErr
	}

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
	bestEqPrefixLen := -1
	bestCoveredCols := -1
	bestHasRange := false
	var bestRangeCond *Condition

	for _, idx := range q.table.schema.Indexes {
		currentPrefix := []any{}
		eqPrefixLen := 0
		coveredCols := 0
		hasRange := false
		var currentRange *Condition

		// Only contiguous prefix can be used by the index engine.
		for _, col := range idx.Columns {
			if eqCond := q.findConditionByOp(col, OpEq); eqCond != nil {
				currentPrefix = append(currentPrefix, eqCond.Value)
				eqPrefixLen++
				coveredCols++
				continue
			}

			// Range condition can only appear once and must end prefix usage.
			if rangeCond := q.findRangeCondition(col); rangeCond != nil {
				rangeCopy := *rangeCond
				currentRange = &rangeCopy
				hasRange = true
				coveredCols++
			}
			break
		}

		if eqPrefixLen == 0 && !hasRange {
			continue
		}

		// Prefer longer equality prefix, then range support, then wider covered prefix.
		currentScore := eqPrefixLen * 10
		if hasRange {
			currentScore += 5
		}

		better := false
		switch {
		case currentScore > bestScore:
			better = true
		case currentScore == bestScore && eqPrefixLen > bestEqPrefixLen:
			better = true
		case currentScore == bestScore && eqPrefixLen == bestEqPrefixLen && hasRange && !bestHasRange:
			better = true
		case currentScore == bestScore && eqPrefixLen == bestEqPrefixLen && hasRange == bestHasRange && coveredCols > bestCoveredCols:
			better = true
		}

		if better {
			bestIndexID = idx.ID
			bestPrefix = currentPrefix
			bestScore = currentScore
			bestEqPrefixLen = eqPrefixLen
			bestCoveredCols = coveredCols
			bestHasRange = hasRange
			bestRangeCond = currentRange
		}
	}
	return bestIndexID, bestPrefix, bestScore, bestRangeCond
}

func (q *Query) findConditionByOp(field string, op Operator) *Condition {
	for i := range q.conditions {
		cond := q.conditions[i]
		if cond.Field == field && cond.Op == op {
			return &q.conditions[i]
		}
	}
	return nil
}

func (q *Query) findRangeCondition(field string) *Condition {
	for i := range q.conditions {
		cond := q.conditions[i]
		if cond.Field != field {
			continue
		}
		if isIndexRangeOperator(cond.Op) {
			return &q.conditions[i]
		}
	}
	return nil
}

func isIndexRangeOperator(op Operator) bool {
	switch op {
	case OpGt, OpGte, OpLt, OpLte:
		return true
	default:
		return false
	}
}

func (q *Query) rebuildConditionIndex() {
	q.conditionFirstIndex = make(map[string]int, len(q.conditions))
	for i, cond := range q.conditions {
		if _, exists := q.conditionFirstIndex[cond.Field]; !exists {
			q.conditionFirstIndex[cond.Field] = i
		}
	}
}
