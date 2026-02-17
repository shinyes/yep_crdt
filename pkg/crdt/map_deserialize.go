package crdt

import "fmt"

// Deserialize 根据类型反序列化 CRDT。
// 注意：对于泛型类型，这里只能使用默认类型（例如 string）。
// 如果需要特定类型，请使用 GetORSet[T] 等方法。
func Deserialize(t Type, data []byte) (CRDT, error) {
	return DeserializeWithHint(t, data, "")
}

// DeserializeWithHint 根据类型和类型提示反序列化 CRDT。
// typeHint 可以是 "string", "int", "uint" 等基本类型，或 "pkg/path.Type" 格式
func DeserializeWithHint(t Type, data []byte, typeHint string) (CRDT, error) {
	if data == nil {
		return nil, &InvalidDataError{CRDTType: t, Reason: "输入数据为 nil", DataLength: 0}
	}

	if len(data) == 0 {
		return nil, &InvalidDataError{CRDTType: t, Reason: "输入数据为空", DataLength: 0}
	}

	switch t {
	case TypeLWW:
		c, err := FromBytesLWW(data)
		if err != nil {
			return nil, fmt.Errorf("%w: LWW: %v", ErrDeserialization, err)
		}
		return c, nil
	case TypeORSet:
		// 优先使用类型注册表
		if typeHint != "" {
			if serializer, ok := TypeRegistry.ORSetSerializers[typeHint]; ok {
				c, err := serializer(data)
				if err == nil {
					return c.(CRDT), nil
				}
				// 回退到默认
			}
		}
		// 默认反序列化为 ORSet[string] 以保持兼容性
		c, err := FromBytesORSet[string](data)
		if err != nil {
			return nil, fmt.Errorf("%w: ORSet[string]: %v", ErrDeserialization, err)
		}
		return c, nil
	case TypePNCounter:
		c, err := FromBytesPNCounter(data)
		if err != nil {
			return nil, fmt.Errorf("%w: PNCounter: %v", ErrDeserialization, err)
		}
		return c, nil
	case TypeRGA:
		// 优先使用类型注册表
		if typeHint != "" {
			if serializer, ok := TypeRegistry.RGASerializers[typeHint]; ok {
				c, err := serializer(data)
				if err == nil {
					return c.(CRDT), nil
				}
				// 回退到默认
			}
		}
		// 默认反序列化为 RGA[[]byte] 以保持兼容性
		c, err := FromBytesRGA[[]byte](data)
		if err != nil {
			return nil, fmt.Errorf("%w: RGA[[]byte]: %v", ErrDeserialization, err)
		}
		return c, nil
	case TypeMap:
		c, err := FromBytesMap(data)
		if err != nil {
			return nil, fmt.Errorf("%w: MapCRDT: %v", ErrDeserialization, err)
		}
		return c, nil
	case TypeLocalFile:
		c, err := FromBytesLocalFile(data)
		if err != nil {
			return nil, fmt.Errorf("%w: LocalFile CRDT: %v", ErrDeserialization, err)
		}
		return c, nil
	default:
		return nil, &InvalidDataError{CRDTType: t, Reason: "未知的 CRDT 类型", DataLength: len(data)}
	}
}
