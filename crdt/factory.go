package crdt

import (
	"encoding/json"
	"fmt"
)

// ============== CRDT 工厂 ==============

// CRDTFactory 提供统一的 CRDT 实例创建
type CRDTFactory struct{}

// NewCRDT 根据类型创建新的 CRDT 实例
func (f *CRDTFactory) NewCRDT(id string, typ Type) (CRDT, error) {
	switch typ {
	case TypeCounter:
		return NewPNCounter(id), nil
	case TypeSet:
		return NewORSet(), nil
	case TypeMap:
		return NewMapCRDT(), nil
	case TypeSequence:
		return NewRGA(), nil
	case TypeImmutableFile:
		return &ImmutableFile{Register: NewLWWRegister(FileMetadata{}, 0)}, nil
	case TypeRegister:
		return NewLWWRegister(nil, 0), nil
	default:
		return nil, fmt.Errorf("未知 CRDT 类型: %v", typ)
	}
}

// ============== Op 注册表 ==============

// OpRegistry Op 反序列化注册表
type OpRegistry struct{}

// UnmarshalOp 根据类型反序列化 Op
func (r *OpRegistry) UnmarshalOp(typ Type, data []byte) (Op, error) {
	switch typ {
	case TypeCounter:
		var o PNCounterOp
		if err := json.Unmarshal(data, &o); err != nil {
			return nil, fmt.Errorf("反序列化 PNCounterOp 失败: %w", err)
		}
		return o, nil

	case TypeSet:
		// 优先尝试 ORSetOp
		var o ORSetOp
		if err := json.Unmarshal(data, &o); err != nil {
			// 尝试旧格式 SetOp
			var o2 SetOp
			if err2 := json.Unmarshal(data, &o2); err2 != nil {
				return nil, fmt.Errorf("反序列化 SetOp 失败: %w", err)
			}
			return o2, nil
		}
		return o, nil

	case TypeRegister, TypeImmutableFile:
		var o LWWOp
		if err := json.Unmarshal(data, &o); err != nil {
			return nil, fmt.Errorf("反序列化 LWWOp 失败: %w", err)
		}
		return o, nil

	case TypeSequence:
		var o RGAOp
		if err := json.Unmarshal(data, &o); err != nil {
			return nil, fmt.Errorf("反序列化 RGAOp 失败: %w", err)
		}
		return o, nil

	case TypeMap:
		var o MapOp
		if err := json.Unmarshal(data, &o); err != nil {
			return nil, fmt.Errorf("反序列化 MapOp 失败: %w", err)
		}
		return o, nil

	default:
		return nil, fmt.Errorf("未知操作类型: %v", typ)
	}
}

// ============== 全局单例 ==============

var (
	// Factory 是全局 CRDT 工厂实例
	Factory = &CRDTFactory{}

	// OpReg 是全局 Op 注册表实例
	OpReg = &OpRegistry{}
)

// ============== 便捷函数 ==============

// NewCRDTByType 是 Factory.NewCRDT 的便捷包装
func NewCRDTByType(id string, typ Type) (CRDT, error) {
	return Factory.NewCRDT(id, typ)
}

// UnmarshalOpByType 是 OpReg.UnmarshalOp 的便捷包装
func UnmarshalOpByType(typ Type, data []byte) (Op, error) {
	return OpReg.UnmarshalOp(typ, data)
}
