package crdt

// readOnlyRGA 包装 RGA 以提供只读访问。
type readOnlyRGA[T any] struct {
	r *RGA[T]
}

func (w *readOnlyRGA[T]) Value() any {
	return w.r.Value()
}

func (w *readOnlyRGA[T]) Iterator() func() (T, bool) {
	return w.r.Iterator()
}

// readOnlySet 包装 ORSet 以提供只读访问。
type readOnlySet[T comparable] struct {
	s *ORSet[T]
}

func (w *readOnlySet[T]) Value() any {
	return w.s.Value()
}

func (w *readOnlySet[T]) Contains(element T) bool {
	return w.s.Contains(element)
}

func (w *readOnlySet[T]) Elements() []T {
	return w.s.Elements()
}
