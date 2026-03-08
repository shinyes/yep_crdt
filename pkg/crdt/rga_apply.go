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

// OpRGAAppend appends one value to the end of current linked list in O(1).
type OpRGAAppend[T any] struct {
	Value T
}

func (op OpRGAAppend[T]) Type() Type { return TypeRGA }

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
	r.ensureTailLocked()

	switch o := op.(type) {
	case OpRGAInsert[T]:
		return r.insertAfterLocked(o.AnchorID, o.Value)

	case OpRGAAppend[T]:
		return r.insertAfterLocked(r.Tail, o.Value)

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

func (r *RGA[T]) insertAfterLocked(anchorID string, value T) error {
	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generate uuidv7: %w", err)
	}
	newID := id.String()
	var ts int64
	if r.Clock != nil {
		ts = r.Clock.Now()
	}

	anchor, ok := r.Vertices[anchorID]
	if !ok || anchor == nil {
		return fmt.Errorf("anchor %s not found", anchorID)
	}

	v := &RGAVertex[T]{
		ID:        newID,
		Value:     value,
		Origin:    anchorID,
		Timestamp: ts,
	}

	r.Vertices[v.ID] = v
	r.edges[anchorID] = insertChildSorted(r.edges[anchorID], v)

	v.Next = anchor.Next
	anchor.Next = v.ID
	if v.Next == "" {
		r.Tail = v.ID
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
