package crdt

import "github.com/vmihailenco/msgpack/v5"

func (r *RGA[T]) GC(safeTimestamp int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureEdges()

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

	state := &struct {
		Vertices map[string]*RGAVertex[T] `msgpack:"vertices"`
		Head     string                   `msgpack:"head"`
	}{
		Vertices: r.Vertices,
		Head:     r.Head,
	}

	return msgpack.Marshal(state)
}

func FromBytesRGA[T any](data []byte) (*RGA[T], error) {
	state := &struct {
		Vertices map[string]*RGAVertex[T] `msgpack:"vertices"`
		Head     string                   `msgpack:"head"`
	}{}

	if err := msgpack.Unmarshal(data, state); err != nil {
		return nil, err
	}
	if state.Vertices == nil {
		state.Vertices = make(map[string]*RGAVertex[T])
	}
	return &RGA[T]{
		Vertices: state.Vertices,
		Head:     state.Head,
		edges:    make(map[string][]*RGAVertex[T]),
	}, nil
}
