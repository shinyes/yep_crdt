package db

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/crdt"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

// InsertAfter 在 RGA 中指定元素后插入。
func (t *Table) InsertAfter(key uuid.UUID, col string, anchorVal any, newVal any) error {
	if key.Version() != 7 {
		return fmt.Errorf("invalid key version: must be UUIDv7")
	}
	colType, err := t.getColCrdtType(col)
	if err != nil || colType != meta.CrdtRGA {
		return fmt.Errorf("InsertAfter only supported for RGA")
	}

	err = t.inTx(true, func(txn store.Tx) error {
		currentMap, oldBody, err := t.loadRow(txn, key)
		if err != nil {
			return err
		}

		rga, err := t.getRGA(currentMap, col)
		if err != nil {
			return err
		}

		// Find ID for anchorVal (first occurrence)
		anchorID := ""
		targetVal := encodeValue(anchorVal)

		// Traversal to find first visible match
		curr := rga.Head
		for curr != "" {
			v := rga.Vertices[curr]
			if !v.Deleted && v.ID != rga.Head && string(v.Value) == string(targetVal) {
				anchorID = v.ID
				break
			}
			curr = v.Next
		}

		if anchorID == "" {
			return fmt.Errorf("anchor value not found: %v", anchorVal)
		}

		op := crdt.OpMapUpdate{
			Key: col,
			Op:  crdt.OpRGAInsert[[]byte]{AnchorID: anchorID, Value: encodeValue(newVal)},
		}

		if err := currentMap.Apply(op); err != nil {
			return err
		}

		return t.saveRow(txn, key, currentMap, oldBody)
	})

	if err == nil {
		t.db.notifyChangeWithColumns(t.schema.Name, key, []string{col})
	}
	return err
}

// InsertAt 在 RGA 第 N 个位置插入 (0-based).
func (t *Table) InsertAt(key uuid.UUID, col string, index int, val any) error {
	if key.Version() != 7 {
		return fmt.Errorf("invalid key version: must be UUIDv7")
	}
	colType, err := t.getColCrdtType(col)
	if err != nil || colType != meta.CrdtRGA {
		return fmt.Errorf("InsertAt only supported for RGA")
	}

	err = t.inTx(true, func(txn store.Tx) error {
		currentMap, oldBody, err := t.loadRow(txn, key)
		if err != nil {
			return err
		}

		rga, err := t.getRGA(currentMap, col)
		if err != nil {
			return err
		}

		// Find Anchor ID (element at index-1)

		if index < 0 {
			return fmt.Errorf("invalid index: %d", index)
		}

		currentIndex := 0
		curr := rga.Vertices[rga.Head].Next

		// Traverse to find element at index-1 (if index=0, anchor is Head)
		// We need to skip deleted nodes.
		for curr != "" && currentIndex < index {
			v := rga.Vertices[curr]
			if !v.Deleted {
				currentIndex++
			}
			// Maintain anchorID as the last visible node or Head
			// Wait, simple logic:
			// If index is 0, anchor is Head.
			// If index is 1, anchor is 0-th element.

			// Correct traversal:
			// Scan until we pass `index` visible elements? No.
			// We need the ID of the element *before* the insertion point.

			if currentIndex == index { // Found our spot? No, index-1
				// If index=0, loop doesn't run, anchorID=Head. Correct.
				// If index=1, we need 0-th element ID.
			}

			if !v.Deleted {
				if currentIndex == index {
					// We successfully passed index-1 visible items.
					// But we are at `index`-th item.
					// We need the previous one.
					// Use a prev pointer?
				}
			}
			curr = v.Next
		}

		// Let's retry traversal simpler
		// Find the ID of the (index-1)-th visible element.
		// If index=0, anchor=Head.

		targetAnchor := rga.Head
		if index > 0 {
			steps := 0
			curr := rga.Vertices[rga.Head].Next
			for curr != "" {
				v := rga.Vertices[curr]
				if !v.Deleted {
					steps++
					if steps == index {
						targetAnchor = v.ID
						break
					}
				}
				curr = v.Next
			}
			if steps < index {
				return fmt.Errorf("index out of bounds: %d", index)
			}
		}

		op := crdt.OpMapUpdate{
			Key: col,
			Op:  crdt.OpRGAInsert[[]byte]{AnchorID: targetAnchor, Value: encodeValue(val)},
		}

		if err := currentMap.Apply(op); err != nil {
			return err
		}

		return t.saveRow(txn, key, currentMap, oldBody)
	})

	if err == nil {
		t.db.notifyChangeWithColumns(t.schema.Name, key, []string{col})
	}
	return err
}

// RemoveAt Removes element at index N.
func (t *Table) RemoveAt(key uuid.UUID, col string, index int) error {
	if key.Version() != 7 {
		return fmt.Errorf("invalid key version: must be UUIDv7")
	}
	colType, err := t.getColCrdtType(col)
	if err != nil || colType != meta.CrdtRGA {
		return fmt.Errorf("RemoveAt only supported for RGA")
	}

	err = t.inTx(true, func(txn store.Tx) error {
		currentMap, oldBody, err := t.loadRow(txn, key)
		if err != nil {
			return err
		}

		rga, err := t.getRGA(currentMap, col)
		if err != nil {
			return err
		}

		// Find ID of N-th visible element (0-based)
		targetID := ""
		steps := -1 // 0-based index

		curr := rga.Vertices[rga.Head].Next
		for curr != "" {
			v := rga.Vertices[curr]
			if !v.Deleted {
				steps++
				if steps == index {
					targetID = v.ID
					break
				}
			}
			curr = v.Next
		}

		if targetID == "" {
			return fmt.Errorf("index out of bounds: %d", index)
		}

		op := crdt.OpMapUpdate{
			Key: col,
			Op:  crdt.OpRGARemove{ID: targetID},
		}

		if err := currentMap.Apply(op); err != nil {
			return err
		}

		return t.saveRow(txn, key, currentMap, oldBody)
	})

	if err == nil {
		t.db.notifyChangeWithColumns(t.schema.Name, key, []string{col})
	}
	return err
}
