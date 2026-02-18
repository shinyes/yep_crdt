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
