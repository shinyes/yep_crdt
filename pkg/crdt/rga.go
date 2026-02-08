package crdt

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// RGA 实现复制可增长数组 (Replicated Growable Array)。
type RGA struct {
	Vertices map[string]*RGAVertex
	Head     string // 虚拟头节点的 ID
}

type RGAVertex struct {
	ID        string
	Value     []byte
	Origin    string // ID of the node this was inserted after (Origin)
	Next      string // ID of the next vertex (derived/cached)
	Timestamp int64  // For conflict resolution
	Deleted   bool
}

func NewRGA() *RGA {
	head := &RGAVertex{
		ID:        uuid.NewString(), // 或者固定的 "HEAD"
		Value:     nil,
		Next:      "",
		Timestamp: 0,
		Deleted:   true, // Head 总是被删除/不可见的
	}
	return &RGA{
		Vertices: map[string]*RGAVertex{head.ID: head},
		Head:     head.ID,
	}
}

func (r *RGA) Type() Type { return TypeRGA }

// Value 按顺序返回值的列表。
func (r *RGA) Value() interface{} {
	var res []interface{}
	curr := r.Head
	for curr != "" {
		v := r.Vertices[curr]
		if !v.Deleted && v.ID != r.Head {
			res = append(res, v.Value)
		}
		curr = v.Next
	}
	return res
}

// OpRGAInsert 在特定 ID 之后插入值。
type OpRGAInsert struct {
	AnchorID string // 插入位置之后的节点 ID
	Value    []byte
}

func (op OpRGAInsert) Type() Type { return TypeRGA }

type OpRGARemove struct {
	ID string
}

func (op OpRGARemove) Type() Type { return TypeRGA }

func (r *RGA) Apply(op Op) error {
	switch o := op.(type) {
	case OpRGAInsert:
		newID := uuid.NewString()
		ts := time.Now().UnixNano()

		// Create new vertex
		v := &RGAVertex{
			ID:        newID,
			Value:     o.Value,
			Origin:    o.AnchorID, // Set Origin
			Timestamp: ts,
		}

		anchor, ok := r.Vertices[o.AnchorID]
		if !ok {
			return fmt.Errorf("anchor %s not found", o.AnchorID)
		}

		// Insert logic (simplified for local apply - still valid locally)
		// But strictly for RGA, we should follow properties.
		// Locally, we just insert after anchor.
		v.Next = anchor.Next
		anchor.Next = v.ID
		r.Vertices[v.ID] = v

	case OpRGARemove:
		if v, ok := r.Vertices[o.ID]; ok {
			v.Deleted = true
		}
	default:
		return ErrInvalidOp
	}
	return nil
}

// Merge merges another RGA state using the standard RGA algorithm (OR-based Tree Traversal).
func (r *RGA) Merge(other CRDT) error {
	o, ok := other.(*RGA)
	if !ok {
		return fmt.Errorf("cannot merge %T into RGA", other)
	}

	// 1. Union of Vertices
	for id, vRemote := range o.Vertices {
		if vLocal, exists := r.Vertices[id]; exists {
			// Resolve Conflicts / Updates
			// If remote is deleted, local becomes deleted (OR-Set nature for deletion)
			if vRemote.Deleted {
				vLocal.Deleted = true
			}
			// Value/Timestamp should be immutable for same ID in RGA, so no change needed.
		} else {
			// Copy missing vertex
			// We need a deep copy to avoid pointer issues if we modify later
			vNew := &RGAVertex{
				ID:        vRemote.ID,
				Value:     vRemote.Value,
				Origin:    vRemote.Origin,
				Timestamp: vRemote.Timestamp,
				Deleted:   vRemote.Deleted,
				// Next will be reconstructed
			}
			r.Vertices[id] = vNew
		}
	}

	// 2. Re-Linearize (Reconstruct Next pointers based on Origin Tree)
	// Build specific tree structure: Origin -> List of Children
	children := make(map[string][]*RGAVertex)

	for _, v := range r.Vertices {
		if v.ID == r.Head {
			continue
		}
		children[v.Origin] = append(children[v.Origin], v)
	}

	// Sort children for each origin to ensure deterministic order
	for _, childList := range children {
		sort.Slice(childList, func(i, j int) bool {
			// Sort by Timestamp Descending
			if childList[i].Timestamp != childList[j].Timestamp {
				return childList[i].Timestamp > childList[j].Timestamp
			}
			// Tie-breaker: ID Descending
			return childList[i].ID > childList[j].ID
		})
	}

	// Traverse and Link
	// We need to rebuild the linked list starting from Head.
	var traverse func(nodeID string) string

	// Returns the ID of the last node in the subtree
	traverse = func(nodeID string) string {
		last := nodeID

		// Get sorted children of this node
		if kids, ok := children[nodeID]; ok {
			for _, child := range kids {
				// Link current last to child
				r.Vertices[last].Next = child.ID

				// Traverse child's subtree
				last = traverse(child.ID)
			}
		}
		return last
	}

	// Start traversal from Head
	finalLast := traverse(r.Head)
	r.Vertices[finalLast].Next = "" // Terminate list

	return nil
}

func (r *RGA) Bytes() ([]byte, error) {
	return json.Marshal(r)
}

func FromBytesRGA(data []byte) (*RGA, error) {
	r := &RGA{}
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}
	return r, nil
}
