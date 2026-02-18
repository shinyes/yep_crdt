package db

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// Add 执行特定 CRDT 的添加/增加操作。
// Counter: Inc(val)
// ORSet: Add(val)
// RGA: Append(val)
// LWW: Set(val) (回退)
func (t *Table) Add(key uuid.UUID, col string, val any) error {
	if err := validateUUIDv7(key); err != nil {
		return err
	}
	colCrdtType, err := t.getColCrdtType(col)
	if err != nil {
		return err
	}

	err = t.inTx(true, func(txn store.Tx) error {
		currentMap, oldBody, err := t.loadRow(txn, key)
		if err != nil {
			return err
		}

		// Initialize if missing
		if currentMap.Entries[col] == nil {
			var newCrdt crdt.CRDT
			switch colCrdtType {
			case meta.CrdtCounter:
				newCrdt = crdt.NewPNCounter(t.db.NodeID)
			case meta.CrdtORSet:
				newCrdt = crdt.NewORSet[string]()
			case meta.CrdtRGA:
				newCrdt = crdt.NewRGA[[]byte](t.db.clock)
			}

			if newCrdt != nil {
				// Apply initialization
				initOp := crdt.OpMapSet{Key: col, Value: newCrdt}
				if err := currentMap.Apply(initOp); err != nil {
					return err
				}
			}
		}

		var op crdt.Op
		ts := t.db.clock.Now()

		switch colCrdtType {
		case meta.CrdtCounter:
			delta, ok := toInt64(val)
			if !ok {
				return fmt.Errorf("value %v is not int for Counter", val)
			}
			op = crdt.OpMapUpdate{
				Key: col,
				Op:  crdt.OpPNCounterInc{Val: delta},
			}
		case meta.CrdtORSet:
			op = crdt.OpMapUpdate{
				Key: col,
				Op:  crdt.OpORSetAdd[string]{Element: string(encodeValue(val))},
			}
		case meta.CrdtRGA:
			// Append to end. Need to find tail?
			// RGA Insert requires AnchorID.
			// If we append, we need the ID of the last element.
			// MapCRDT doesn't expose RGA structure directly via Value().
			// We need to access the RGA instance.
			rga, err := t.getRGA(currentMap, col)
			if err != nil {
				return err
			}
			// Find last element
			lastID := rga.Head
			// Traverse to find end. (O(N) - optimized later?)
			curr := rga.Head
			for curr != "" {
				v := rga.Vertices[curr]
				if v.Next == "" {
					lastID = v.ID
					break
				}
				curr = v.Next
			}

			op = crdt.OpMapUpdate{
				Key: col,
				Op:  crdt.OpRGAInsert[[]byte]{AnchorID: lastID, Value: encodeValue(val)},
			}
		default: // LWW or Unknown
			colType := meta.ColTypeString
			if schemaCol, ok := t.getColumnSchema(col); ok && schemaCol.Type != "" {
				colType = schemaCol.Type
			}
			encoded, err := encodeLWWValueByColumnType(colType, val)
			if err != nil {
				return fmt.Errorf("failed to encode column %q: %w", col, err)
			}
			lww := crdt.NewLWWRegister(encoded, ts)
			op = crdt.OpMapSet{Key: col, Value: lww}
		}

		if err := currentMap.Apply(op); err != nil {
			return err
		}

		return t.saveRow(txn, key, currentMap, oldBody)

	})

	if err == nil {
		t.notifyChangeAfterWrite(key, []string{col})
	}
	return err
}

// Remove 执行特定 CRDT 的移除/减少操作。
// ORSet: Remove(val)
// RGA: RemoveByValue(val) (Remove all instances of val)
func (t *Table) Remove(key uuid.UUID, col string, val any) error {
	if err := validateUUIDv7(key); err != nil {
		return err
	}
	colCrdtType, err := t.getColCrdtType(col)
	if err != nil {
		return err
	}

	err = t.inTx(true, func(txn store.Tx) error {
		currentMap, oldBody, err := t.loadRow(txn, key)
		if err != nil {
			return err
		}

		var op crdt.Op

		switch colCrdtType {
		case meta.CrdtCounter:
			delta, ok := toInt64(val)
			if !ok {
				return fmt.Errorf("value %v is not int for Counter", val)
			}
			op = crdt.OpMapUpdate{
				Key: col,
				Op:  crdt.OpPNCounterInc{Val: -delta},
			}
		case meta.CrdtORSet:
			op = crdt.OpMapUpdate{
				Key: col,
				Op:  crdt.OpORSetRemove[string]{Element: string(encodeValue(val))},
			}
		case meta.CrdtRGA:
			// Remove by Value. Need to find all IDs with this value.
			rga, err := t.getRGA(currentMap, col)
			if err != nil {
				return err
			}
			targetVal := encodeValue(val)
			var idsToRemove []string
			for id, v := range rga.Vertices {
				if !v.Deleted && string(v.Value) == string(targetVal) {
					idsToRemove = append(idsToRemove, id)
				}
			}

			// Apply multiple remove ops? MapUpdate supports single Op.
			// We might need multiple applies.
			for _, id := range idsToRemove {
				subOp := crdt.OpMapUpdate{
					Key: col,
					Op:  crdt.OpRGARemove{ID: id},
				}
				if err := currentMap.Apply(subOp); err != nil {
					return err
				}
			}
			return t.saveRow(txn, key, currentMap, oldBody)

		default:
			return fmt.Errorf("remove not supported for type %s", colCrdtType)
		}

		if err := currentMap.Apply(op); err != nil {
			return err
		}

		return t.saveRow(txn, key, currentMap, oldBody)

	})

	if err == nil {
		t.notifyChangeAfterWrite(key, []string{col})
	}
	return err
}
