package crdt

import (
	"encoding/json"
	"fmt"
)

// MapCRDT 实现列 -> CRDT 的映射。
// 这是 "行" 容器。
type MapCRDT struct {
	Entries map[string]*Entry
}

type Entry struct {
	Type Type
	Data []byte
}

func NewMapCRDT() *MapCRDT {
	return &MapCRDT{
		Entries: make(map[string]*Entry),
	}
}

func (m *MapCRDT) Type() Type { return TypeMap }

func (m *MapCRDT) Value() interface{} {
	// 返回 map[string]interface{}
	// 仅返回反序列化的值用于演示。
	// 调用者可能需要原始 CRDT。
	res := make(map[string]interface{})
	for k, e := range m.Entries {
		c, err := Deserialize(e.Type, e.Data)
		if err == nil {
			res[k] = c.Value()
		}
	}
	return res
}

// 根据类型反序列化的助手
func Deserialize(t Type, data []byte) (CRDT, error) {
	switch t {
	case TypeLWW:
		return FromBytesLWW(data)
	case TypeORSet:
		return FromBytesORSet(data)
	case TypePNCounter:
		return FromBytesPNCounter(data)
	case TypeRGA:
		return FromBytesRGA(data)
	// case TypeMap: 嵌套？
	default:
		return nil, fmt.Errorf("unknown type %v", t)
	}
}

// OpMapSet 为列设置 CRDT。
type OpMapSet struct {
	Key   string
	Value CRDT
}

func (op OpMapSet) Type() Type { return TypeMap }

// OpMapUpdate 对 Map 中的现有 CRDT 应用操作。
type OpMapUpdate struct {
	Key string
	Op  Op
}

func (op OpMapUpdate) Type() Type { return TypeMap }

func (m *MapCRDT) Apply(op Op) error {
	switch o := op.(type) {
	case OpMapSet:
		b, err := o.Value.Bytes()
		if err != nil {
			return err
		}
		m.Entries[o.Key] = &Entry{
			Type: o.Value.Type(),
			Data: b,
		}
	case OpMapUpdate:
		// MapUpdate needs to retrieve the CRDT and apply op.
		// We need to deserialize, apply, and serialize back.
		// Because MapCRDT stores []byte in Entry.
		e, ok := m.Entries[o.Key]
		if !ok {
			return fmt.Errorf("key %s not found", o.Key)
		}

		c, err := Deserialize(e.Type, e.Data)
		if err != nil {
			return err
		}

		if err := c.Apply(o.Op); err != nil {
			return err
		}

		newData, err := c.Bytes()
		if err != nil {
			return err
		}
		e.Data = newData

	default:
		return ErrInvalidOp
	}
	return nil
}

func (m *MapCRDT) Merge(other CRDT) error {
	o, ok := other.(*MapCRDT)
	if !ok {
		return fmt.Errorf("cannot merge %T into MapCRDT", other)
	}

	for k, remoteEntry := range o.Entries {
		localEntry, exists := m.Entries[k]
		if !exists {
			m.Entries[k] = remoteEntry
			continue
		}

		if localEntry.Type != remoteEntry.Type {
			m.Entries[k] = remoteEntry
		} else {
			lC, _ := Deserialize(localEntry.Type, localEntry.Data)
			rC, _ := Deserialize(remoteEntry.Type, remoteEntry.Data)
			if lC != nil && rC != nil {
				lC.Merge(rC)
				b, _ := lC.Bytes()
				localEntry.Data = b
			}
		}
	}
	return nil
}

func (m *MapCRDT) Bytes() ([]byte, error) {
	return json.Marshal(m)
}

func FromBytesMap(data []byte) (*MapCRDT, error) {
	m := NewMapCRDT()
	if err := json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *MapCRDT) GetCRDT(key string) CRDT {
	if e, ok := m.Entries[key]; ok {
		c, err := Deserialize(e.Type, e.Data)
		if err == nil {
			return c
		}
	}
	return nil
}
