package crdt

import (
	"fmt"
	"sort"
)

func (r *RGA[T]) isTriviallyEmptyLocked() bool {
	if len(r.Vertices) != 1 {
		return false
	}
	head, ok := r.Vertices[r.Head]
	if !ok {
		return false
	}
	return head.Next == ""
}

func (r *RGA[T]) mergeIntoEmptyLocked(o *RGA[T]) {
	localHead := r.Head
	localHeadVertex := r.Vertices[localHead]
	if localHeadVertex == nil {
		var zero T
		localHeadVertex = &RGAVertex[T]{
			ID:      localHead,
			Value:   zero,
			Deleted: true,
		}
	}

	remoteHead, ok := o.Vertices[o.Head]
	if !ok {
		return
	}

	localHeadVertex.Next = remoteHead.Next
	localHeadVertex.Origin = ""
	localHeadVertex.Timestamp = 0
	localHeadVertex.Deleted = true
	localHeadVertex.DeletedAt = 0

	nonHeadCount := len(o.Vertices) - 1
	if nonHeadCount <= 0 {
		r.Vertices = map[string]*RGAVertex[T]{localHead: localHeadVertex}
		r.edges = make(map[string][]*RGAVertex[T])
		return
	}

	clonedVertices := make(map[string]*RGAVertex[T], len(o.Vertices))
	clonedVertices[localHead] = localHeadVertex

	storage := make([]RGAVertex[T], nonHeadCount)
	nextIndex := 0

	for id, vRemote := range o.Vertices {
		if id == o.Head {
			continue
		}

		origin := vRemote.Origin
		if origin == o.Head {
			origin = localHead
		}
		next := vRemote.Next
		if next == o.Head {
			next = localHead
		}

		cloned := &storage[nextIndex]
		nextIndex++
		*cloned = RGAVertex[T]{
			ID:        vRemote.ID,
			Value:     deepCopyValue(vRemote.Value),
			Origin:    origin,
			Next:      next,
			Timestamp: vRemote.Timestamp,
			Deleted:   vRemote.Deleted,
			DeletedAt: vRemote.DeletedAt,
		}
		clonedVertices[id] = cloned
	}

	r.Vertices = clonedVertices
	// Build edges lazily in ensureEdges(). This avoids heavy allocations on
	// merge-into-empty hot paths while keeping behavior unchanged.
	r.edges = make(map[string][]*RGAVertex[T])
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

	if r.isTriviallyEmptyLocked() {
		r.mergeIntoEmptyLocked(o)
		return nil
	}

	r.ensureEdges()

	var newVertices []*RGAVertex[T]

	for id, vRemote := range o.Vertices {
		if vLocal, exists := r.Vertices[id]; exists {
			if vRemote.Deleted {
				if !vLocal.Deleted {
					vLocal.Deleted = true
					vLocal.DeletedAt = vRemote.DeletedAt
					// 注意：不要在这里清理 edges，GC 时会处理
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
