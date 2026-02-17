package crdt

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// OpRGAInsert 在特定 ID 之后插入值。
type OpRGAInsert[T any] struct {
	AnchorID string // 插入位置之后的节点 ID
	Value    T
}

func (op OpRGAInsert[T]) Type() Type { return TypeRGA }

type OpRGARemove struct {
	ID string
}

func (op OpRGARemove) Type() Type { return TypeRGA }

func (r *RGA[T]) ensureEdges() {
	if len(r.edges) > 0 {
		return
	}
	if r.edges == nil {
		r.edges = make(map[string][]*RGAVertex[T])
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

func (r *RGA[T]) Apply(op Op) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureEdges()

	switch o := op.(type) {
	case OpRGAInsert[T]:
		id, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("generate uuidv7: %w", err)
		}
		newID := id.String()
		var ts int64
		if r.Clock != nil {
			ts = r.Clock.Now()
		} else {
			ts = 0
		}

		if _, ok := r.Vertices[o.AnchorID]; !ok {
			return fmt.Errorf("anchor %s not found", o.AnchorID)
		}

		v := &RGAVertex[T]{
			ID:        newID,
			Value:     o.Value,
			Origin:    o.AnchorID,
			Timestamp: ts,
		}

		r.Vertices[v.ID] = v
		r.edges[o.AnchorID] = insertChildSorted(r.edges[o.AnchorID], v)

		anchor := r.Vertices[o.AnchorID]
		v.Next = anchor.Next
		anchor.Next = v.ID

	case OpRGARemove:
		if v, ok := r.Vertices[o.ID]; ok {
			v.Deleted = true
			if r.Clock != nil {
				v.DeletedAt = r.Clock.Now()
			}
			// 注意：不要在这里清理 edges 缓存
			// edges 缓存在 GC 时会被正确清理
			// 提前清理会导致 GC 无法正确判断节点是否有子节点
		}
	default:
		return ErrInvalidOp
	}
	return nil
}

// sortChildren sorts by Timestamp DESC, then ID DESC
func sortChildren[T any](children []*RGAVertex[T]) {
	sort.Slice(children, func(i, j int) bool {
		if children[i].Timestamp != children[j].Timestamp {
			return children[i].Timestamp > children[j].Timestamp
		}
		return children[i].ID > children[j].ID
	})
}

func childComesBefore[T any](left *RGAVertex[T], right *RGAVertex[T]) bool {
	if left.Timestamp != right.Timestamp {
		return left.Timestamp > right.Timestamp
	}
	return left.ID > right.ID
}

// insertChildSorted inserts one child into a DESC-sorted sibling list.
func insertChildSorted[T any](children []*RGAVertex[T], v *RGAVertex[T]) []*RGAVertex[T] {
	idx := sort.Search(len(children), func(i int) bool {
		return childComesBefore(v, children[i])
	})
	children = append(children, nil)
	copy(children[idx+1:], children[idx:])
	children[idx] = v
	return children
}

// traverseRightMost finds the right-most node in the subtree rooted at node.
func (r *RGA[T]) traverseRightMost(node *RGAVertex[T]) *RGAVertex[T] {
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
