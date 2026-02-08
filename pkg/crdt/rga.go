package crdt

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/hlc"
)

// RGA 实现复制可增长数组 (Replicated Growable Array)。
type RGA struct {
	Vertices map[string]*RGAVertex
	Head     string     // 虚拟头节点的 ID
	Clock    *hlc.Clock // 混合逻辑时钟

	// internal cache for tree structure: Origin -> List of Children
	// This allows avoiding full reconstruction on every Merge.
	edges map[string][]*RGAVertex
}

type RGAVertex struct {
	ID        string
	Value     []byte
	Origin    string // ID of the node this was inserted after (Origin)
	Next      string // ID of the next vertex (derived/cached)
	Timestamp int64  // For conflict resolution
	Deleted   bool
}

func NewRGA(clock *hlc.Clock) *RGA {
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
		Clock:    clock,
		edges:    make(map[string][]*RGAVertex),
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

func (r *RGA) ensureEdges() {
	if len(r.edges) > 0 {
		return
	}
	if r.edges == nil {
		r.edges = make(map[string][]*RGAVertex)
	}
	if len(r.Vertices) > 1 {
		for _, v := range r.Vertices {
			if v.ID == r.Head {
				continue
			}
			r.edges[v.Origin] = append(r.edges[v.Origin], v)
		}
		for _, children := range r.edges {
			sortChildren(children)
		}
	}
}

func (r *RGA) Apply(op Op) error {
	r.ensureEdges()

	switch o := op.(type) {
	case OpRGAInsert:
		newID := uuid.NewString()
		var ts int64
		if r.Clock != nil {
			ts = r.Clock.Now()
		} else {
			ts = 0
		}

		if _, ok := r.Vertices[o.AnchorID]; !ok {
			return fmt.Errorf("anchor %s not found", o.AnchorID)
		}

		v := &RGAVertex{
			ID:        newID,
			Value:     o.Value,
			Origin:    o.AnchorID,
			Timestamp: ts,
		}

		r.Vertices[v.ID] = v
		r.edges[o.AnchorID] = append(r.edges[o.AnchorID], v)
		sortChildren(r.edges[o.AnchorID])

		// Local Insert Logic:
		// Newest child (highest TS) goes first.
		anchor := r.Vertices[o.AnchorID]
		v.Next = anchor.Next
		anchor.Next = v.ID

	case OpRGARemove:
		if v, ok := r.Vertices[o.ID]; ok {
			v.Deleted = true
		}
	default:
		return ErrInvalidOp
	}
	return nil
}

// sortChildren sorts by Timestamp DESC, then ID DESC
func sortChildren(children []*RGAVertex) {
	sort.Slice(children, func(i, j int) bool {
		if children[i].Timestamp != children[j].Timestamp {
			return children[i].Timestamp > children[j].Timestamp
		}
		return children[i].ID > children[j].ID
	})
}

// traverseRightMost finds the right-most node in the subtree rooted at node.
func (r *RGA) traverseRightMost(node *RGAVertex) *RGAVertex {
	curr := node
	for {
		children := r.edges[curr.ID]
		if len(children) == 0 {
			return curr
		}
		lastChild := children[len(children)-1]
		curr = lastChild
	}
}

// Merge merges another RGA state using incremental updates.
func (r *RGA) Merge(other CRDT) error {
	o, ok := other.(*RGA)
	if !ok {
		return fmt.Errorf("cannot merge %T into RGA", other)
	}

	r.ensureEdges()

	// 1. Identify missing vertices and add them
	var newVertices []*RGAVertex

	for id, vRemote := range o.Vertices {
		if vLocal, exists := r.Vertices[id]; exists {
			if vRemote.Deleted && !vLocal.Deleted {
				vLocal.Deleted = true
			}
		} else {
			valueCopy := make([]byte, len(vRemote.Value))
			copy(valueCopy, vRemote.Value)

			vNew := &RGAVertex{
				ID:        vRemote.ID,
				Value:     valueCopy,
				Origin:    vRemote.Origin,
				Timestamp: vRemote.Timestamp,
				Deleted:   vRemote.Deleted,
			}
			r.Vertices[id] = vNew
			newVertices = append(newVertices, vNew)
			r.edges[vNew.Origin] = append(r.edges[vNew.Origin], vNew)
		}
	}

	if len(newVertices) == 0 {
		return nil
	}

	// 2. Sort modified edge lists
	affectedOrigins := make(map[string]bool)
	for _, v := range newVertices {
		affectedOrigins[v.Origin] = true
	}

	siblingRanks := make(map[string]int)
	for originID := range affectedOrigins {
		if list, ok := r.edges[originID]; ok {
			sortChildren(list)
			// Capture ranks of new nodes
			for i, child := range list {
				siblingRanks[child.ID] = i
			}
		}
	}

	// 3. Topological sort of newVertices
	// Primary: Depth (Parent < Child)
	// Secondary: Sibling Rank (Left < Right)
	depths := make(map[string]int)
	newSet := make(map[string]bool)
	for _, v := range newVertices {
		newSet[v.ID] = true
	}

	var getDepth func(id string) int
	getDepth = func(id string) int {
		if d, ok := depths[id]; ok {
			return d
		}
		if !newSet[id] {
			return 0
		}
		v := r.Vertices[id]
		d := getDepth(v.Origin) + 1
		depths[id] = d
		return d
	}

	sort.Slice(newVertices, func(i, j int) bool {
		d1, d2 := getDepth(newVertices[i].ID), getDepth(newVertices[j].ID)
		if d1 != d2 {
			return d1 < d2
		}
		u, v := newVertices[i], newVertices[j]
		if u.Origin == v.Origin {
			return siblingRanks[u.ID] < siblingRanks[v.ID]
		}
		return u.Origin < v.Origin
	})

	// 4. Link new vertices into the linear list
	for _, v := range newVertices {
		origin := r.Vertices[v.Origin]
		if origin == nil {
			continue
		}

		rank := siblingRanks[v.ID]
		// Re-verify rank?
		// We can trust siblingRanks map since we just computed it from sorted list.

		var insertPos *RGAVertex
		if rank == 0 {
			insertPos = origin
		} else {
			// Find prevSibling.
			// We need access to children list again?
			children := r.edges[v.Origin]
			if rank >= len(children) {
				// Should not happen
				continue
			}
			prevSibling := children[rank-1]
			insertPos = r.traverseRightMost(prevSibling)
		}

		if insertPos == nil {
			continue
		}

		targetNext := insertPos.Next
		v.Next = targetNext
		insertPos.Next = v.ID
	}

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
