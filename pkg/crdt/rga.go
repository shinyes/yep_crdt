package crdt

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/hlc"
)

// RGA 实现复制可增长数组 (Replicated Growable Array)。
type RGA[T any] struct {
	mu       sync.RWMutex
	Vertices map[string]*RGAVertex[T]
	Head     string     // 虚拟头节点的 ID
	Clock    *hlc.Clock // 混合逻辑时钟

	// internal cache for tree structure: Origin -> List of Children
	edges map[string][]*RGAVertex[T]
}

type RGAVertex[T any] struct {
	ID        string
	Value     T
	Origin    string // ID of the node this was inserted after (Origin)
	Next      string // ID of the next vertex (derived/cached)
	Timestamp int64  // For conflict resolution
	Deleted   bool
	DeletedAt int64 // Time of deletion for GC
}

func NewRGA[T any](clock *hlc.Clock) *RGA[T] {
	// Head has zero value of T
	var zero T
	head := &RGAVertex[T]{
		ID:        uuid.NewString(),
		Value:     zero,
		Next:      "",
		Timestamp: 0,
		Deleted:   true,
	}
	return &RGA[T]{
		Vertices: map[string]*RGAVertex[T]{head.ID: head},
		Head:     head.ID,
		Clock:    clock,
		edges:    make(map[string][]*RGAVertex[T]),
	}
}

func (r *RGA[T]) Type() Type { return TypeRGA }

// deepCopyValue 尝试对值进行深拷贝，主要处理 []byte 类型
func deepCopyValue[T any](value T) T {
	// 尝试处理 []byte 类型
	if bytesVal, ok := any(value).([]byte); ok {
		copied := make([]byte, len(bytesVal))
		copy(copied, bytesVal)
		return any(copied).(T)
	}
	// 其他类型假设是不可变的或可以浅拷贝的
	return value
}

// Value 按顺序返回值的列表。
// 注意：对于大数据量，建议使用 Iterator() 以避免切片分配。
func (r *RGA[T]) Value() any {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var res []T
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

// Iterator 返回一个迭代器函数。
// 每次调用该函数，返回 (下一个值, true)。
// 如果遍历结束，返回 (零值, false)。
// 这种模式避免了在这里分配整个切片。
// 注意：迭代器创建时会创建快照，迭代期间不需要持有锁。
func (r *RGA[T]) Iterator() func() (T, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 创建快照以避免在迭代期间持有锁
	currID := r.Head
	snapshot := make([]T, 0)
	for currID != "" {
		v := r.Vertices[currID]
		currID = v.Next
		if !v.Deleted && v.ID != r.Head {
			snapshot = append(snapshot, v.Value)
		}
	}

	index := 0
	return func() (T, bool) {
		if index < len(snapshot) {
			val := snapshot[index]
			index++
			return val, true
		}
		var zero T
		return zero, false
	}
}

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

		v := &RGAVertex[T]{
			ID:        newID,
			Value:     o.Value,
			Origin:    o.AnchorID,
			Timestamp: ts,
		}

		r.Vertices[v.ID] = v
		r.edges[o.AnchorID] = append(r.edges[o.AnchorID], v)
		sortChildren(r.edges[o.AnchorID])

		anchor := r.Vertices[o.AnchorID]
		v.Next = anchor.Next
		anchor.Next = v.ID

	case OpRGARemove:
		if v, ok := r.Vertices[o.ID]; ok {
			v.Deleted = true
			if r.Clock != nil {
				v.DeletedAt = r.Clock.Now()
			}
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

// Merge merges another RGA state using incremental updates.
func (r *RGA[T]) Merge(other CRDT) error {
	o, ok := other.(*RGA[T])
	if !ok {
		return fmt.Errorf("cannot merge %T into RGA", other)
	}

	o.mu.RLock()
	defer o.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureEdges()

	var newVertices []*RGAVertex[T]

	for id, vRemote := range o.Vertices {
		if vLocal, exists := r.Vertices[id]; exists {
			if vRemote.Deleted {
				if !vLocal.Deleted {
					vLocal.Deleted = true
					vLocal.DeletedAt = vRemote.DeletedAt
				} else {
					if vLocal.DeletedAt == 0 || (vRemote.DeletedAt > 0 && vRemote.DeletedAt < vLocal.DeletedAt) {
						vLocal.DeletedAt = vRemote.DeletedAt
					}
				}
			}
		} else {
			// Deep copy value for slice types like []byte
			vNew := &RGAVertex[T]{
				ID:        vRemote.ID,
				Value:     deepCopyValue(vRemote.Value),
				Origin:    vRemote.Origin,
				Timestamp: vRemote.Timestamp,
				Deleted:   vRemote.Deleted,
				DeletedAt: vRemote.DeletedAt,
			}
			r.Vertices[id] = vNew
			newVertices = append(newVertices, vNew)
			r.edges[vNew.Origin] = append(r.edges[vNew.Origin], vNew)
		}
	}

	if len(newVertices) == 0 {
		return nil
	}

	affectedOrigins := make(map[string]bool)
	for _, v := range newVertices {
		affectedOrigins[v.Origin] = true
	}

	siblingRanks := make(map[string]int)
	for originID := range affectedOrigins {
		if list, ok := r.edges[originID]; ok {
			sortChildren(list)
			for i, child := range list {
				siblingRanks[child.ID] = i
			}
		}
	}

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

	for _, v := range newVertices {
		origin := r.Vertices[v.Origin]
		if origin == nil {
			continue
		}

		rank := siblingRanks[v.ID]

		var insertPos *RGAVertex[T]
		if rank == 0 {
			insertPos = origin
		} else {
			children := r.edges[v.Origin]
			if rank >= len(children) {
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

func (r *RGA[T]) GC(safeTimestamp int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	count := 0
	prevID := r.Head
	currID := r.Vertices[r.Head].Next

	for currID != "" {
		v := r.Vertices[currID]
		nextID := v.Next

		isLeaf := true
		if children, ok := r.edges[v.ID]; ok && len(children) > 0 {
			isLeaf = false
		}

		canGC := v.Deleted && v.DeletedAt > 0 && v.DeletedAt < safeTimestamp && isLeaf

		if canGC {
			prev := r.Vertices[prevID]
			prev.Next = nextID

			delete(r.Vertices, currID)

			parentID := v.Origin
			if siblings, ok := r.edges[parentID]; ok {
				newSiblings := siblings[:0]
				for _, child := range siblings {
					if child.ID != currID {
						newSiblings = append(newSiblings, child)
					}
				}
				if len(newSiblings) == 0 {
					delete(r.edges, parentID)
				} else {
					r.edges[parentID] = newSiblings
				}
			}

			count++
			currID = nextID
		} else {
			prevID = currID
			currID = nextID
		}
	}

	return count
}

func (r *RGA[T]) Bytes() ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 创建临时结构体用于序列化，避免锁被 JSON 序列化期间持有
	tempVertices := make(map[string]*RGAVertex[T], len(r.Vertices))
	for k, v := range r.Vertices {
		tempVertices[k] = &RGAVertex[T]{
			ID:        v.ID,
			Value:     v.Value,
			Origin:    v.Origin,
			Next:      v.Next,
			Timestamp: v.Timestamp,
			Deleted:   v.Deleted,
			DeletedAt: v.DeletedAt,
		}
	}

	temp := &struct {
		Vertices map[string]*RGAVertex[T]
		Head     string
	}{
		Vertices: tempVertices,
		Head:     r.Head,
	}

	return json.Marshal(temp)
}

func FromBytesRGA[T any](data []byte) (*RGA[T], error) {
	r := &RGA[T]{}
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}
	return r, nil
}
