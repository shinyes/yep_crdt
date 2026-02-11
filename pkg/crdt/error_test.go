package crdt

import (
	"errors"
	"strings"
	"testing"
)

// TestInvalidDataError 测试InvalidDataError错误类型
func TestInvalidDataError(t *testing.T) {
	tests := []struct {
		name        string
		crdtType    Type
		reason      string
		dataLength  int
		wantMessage string
	}{
		{
			name:        "empty data",
			crdtType:    TypeLWW,
			reason:      "data为空",
			dataLength:  0,
			wantMessage: "无效的 CRDT 数据: 类型 1, 原因: data为空, 数据长度: 0",
		},
		{
			name:        "invalid length",
			crdtType:    TypePNCounter,
			reason:      "数据不足",
			dataLength:  5,
			wantMessage: "无效的 CRDT 数据: 类型 3, 原因: 数据不足, 数据长度: 5",
		},
		{
			name:        "negative data length",
			crdtType:    TypeRGA,
			reason:      "解析失败",
			dataLength:  -1,
			wantMessage: "无效的 CRDT 数据: 类型 4, 原因: 解析失败",
		},
		{
			name:        "no reason",
			crdtType:    TypeORSet,
			reason:      "",
			dataLength:  100,
			wantMessage: "无效的 CRDT 数据: 类型 2, 数据长度: 100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &InvalidDataError{
				CRDTType:   tt.crdtType,
				Reason:     tt.reason,
				DataLength: tt.dataLength,
			}

			// 验证Error()方法
			gotMessage := err.Error()
			if gotMessage != tt.wantMessage {
				t.Errorf("Error() = %q, want %q", gotMessage, tt.wantMessage)
			}

			// 验证Unwrap()方法
			if unwrapped := errors.Unwrap(err); unwrapped != ErrInvalidData {
				t.Errorf("Unwrap() = %v, want ErrInvalidData", unwrapped)
			}

			// 验证errors.Is
			if !errors.Is(err, ErrInvalidData) {
				t.Error("errors.Is(err, ErrInvalidData) should be true")
			}

			// 验证字段
			if err.CRDTType != tt.crdtType {
				t.Errorf("CRDTType = %d, want %d", err.CRDTType, tt.crdtType)
			}
			if err.Reason != tt.reason {
				t.Errorf("Reason = %q, want %q", err.Reason, tt.reason)
			}
			if err.DataLength != tt.dataLength {
				t.Errorf("DataLength = %d, want %d", err.DataLength, tt.dataLength)
			}
		})
	}
}

// TestNewInvalidDataError 测试NewInvalidDataError构造函数
func TestNewInvalidDataError(t *testing.T) {
	err := NewInvalidDataError(TypeMap, "测试错误")
	
	if err.CRDTType != TypeMap {
		t.Errorf("CRDTType = %d, want %d", err.CRDTType, TypeMap)
	}
	if err.Reason != "测试错误" {
		t.Errorf("Reason = %q, want %q", err.Reason, "测试错误")
	}
	if err.DataLength != -1 {
		t.Errorf("DataLength = %d, want -1", err.DataLength)
	}
}

// TestInvalidOpError 测试InvalidOpError错误类型
func TestInvalidOpError(t *testing.T) {
	tests := []struct {
		name        string
		crdtType    Type
		expectedOp  string
		gotOp       string
		wantMessage string
	}{
		{
			name:        "nil operation",
			crdtType:    TypeMap,
			expectedOp:  "OpMapSet",
			gotOp:       "nil",
			wantMessage: "操作类型不匹配: CRDT 类型 5 期望 OpMapSet, 得到 nil",
		},
		{
			name:        "wrong operation type",
			crdtType:    TypeORSet,
			expectedOp:  "OpORSetAdd",
			gotOp:       "OpLWWRegister",
			wantMessage: "操作类型不匹配: CRDT 类型 2 期望 OpORSetAdd, 得到 OpLWWRegister",
		},
		{
			name:        "PNCounter operation",
			crdtType:    TypePNCounter,
			expectedOp:  "OpPNCounterInc",
			gotOp:       "OpMapSet",
			wantMessage: "操作类型不匹配: CRDT 类型 3 期望 OpPNCounterInc, 得到 OpMapSet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &InvalidOpError{
				ExpectedOp: tt.expectedOp,
				GotOp:      tt.gotOp,
				CRDTType:   tt.crdtType,
			}

			// 验证Error()方法
			gotMessage := err.Error()
			if gotMessage != tt.wantMessage {
				t.Errorf("Error() = %q, want %q", gotMessage, tt.wantMessage)
			}

			// 验证Unwrap()方法
			if unwrapped := errors.Unwrap(err); unwrapped != ErrInvalidOp {
				t.Errorf("Unwrap() = %v, want ErrInvalidOp", unwrapped)
			}

			// 验证errors.Is
			if !errors.Is(err, ErrInvalidOp) {
				t.Error("errors.Is(err, ErrInvalidOp) should be true")
			}

			// 验证字段
			if err.ExpectedOp != tt.expectedOp {
				t.Errorf("ExpectedOp = %q, want %q", err.ExpectedOp, tt.expectedOp)
			}
			if err.GotOp != tt.gotOp {
				t.Errorf("GotOp = %q, want %q", err.GotOp, tt.gotOp)
			}
			if err.CRDTType != tt.crdtType {
				t.Errorf("CRDTType = %d, want %d", err.CRDTType, tt.crdtType)
			}
		})
	}
}

// TestKeyNotFoundError 测试KeyNotFoundError错误类型
func TestKeyNotFoundError(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		wantMessage string
	}{
		{
			name:        "simple key",
			key:         "column1",
			wantMessage: "键 'column1' 不存在",
		},
		{
			name:        "empty key",
			key:         "",
			wantMessage: "键 '' 不存在",
		},
		{
			name:        "complex key",
			key:         "user.profile.age",
			wantMessage: "键 'user.profile.age' 不存在",
		},
		{
			name:        "special characters",
			key:         "key-with.special_chars",
			wantMessage: "键 'key-with.special_chars' 不存在",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &KeyNotFoundError{
				Key: tt.key,
			}

			// 验证Error()方法
			gotMessage := err.Error()
			if gotMessage != tt.wantMessage {
				t.Errorf("Error() = %q, want %q", gotMessage, tt.wantMessage)
			}

			// 验证Unwrap()方法
			if unwrapped := errors.Unwrap(err); unwrapped != ErrKeyNotFound {
				t.Errorf("Unwrap() = %v, want ErrKeyNotFound", unwrapped)
			}

			// 验证errors.Is
			if !errors.Is(err, ErrKeyNotFound) {
				t.Error("errors.Is(err, ErrKeyNotFound) should be true")
			}

			// 验证字段
			if err.Key != tt.key {
				t.Errorf("Key = %q, want %q", err.Key, tt.key)
			}
		})
	}
}

// TestTypeMismatchError 测试TypeMismatchError错误类型
func TestTypeMismatchError(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		expected    Type
		got         Type
		wantMessage string
	}{
		{
			name:        "LWW vs ORSet",
			key:         "column1",
			expected:    TypeLWW,
			got:         TypeORSet,
			wantMessage: "类型不匹配: 键 'column1' 期望类型 1, 得到类型 2",
		},
		{
			name:        "Map vs PNCounter",
			key:         "map_key",
			expected:    TypeMap,
			got:         TypePNCounter,
			wantMessage: "类型不匹配: 键 'map_key' 期望类型 5, 得到类型 3",
		},
		{
			name:        "RGA vs LocalFile",
			key:         "file_data",
			expected:    TypeRGA,
			got:         TypeLocalFile,
			wantMessage: "类型不匹配: 键 'file_data' 期望类型 4, 得到类型 6",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &TypeMismatchError{
				Key:         tt.key,
				ExpectedType: tt.expected,
				GotType:     tt.got,
			}

			// 验证Error()方法
			gotMessage := err.Error()
			if gotMessage != tt.wantMessage {
				t.Errorf("Error() = %q, want %q", gotMessage, tt.wantMessage)
			}

			// 验证Unwrap()方法
			if unwrapped := errors.Unwrap(err); unwrapped != ErrTypeMismatch {
				t.Errorf("Unwrap() = %v, want ErrTypeMismatch", unwrapped)
			}

			// 验证errors.Is
			if !errors.Is(err, ErrTypeMismatch) {
				t.Error("errors.Is(err, ErrTypeMismatch) should be true")
			}

			// 验证字段
			if err.Key != tt.key {
				t.Errorf("Key = %q, want %q", err.Key, tt.key)
			}
			if err.ExpectedType != tt.expected {
				t.Errorf("ExpectedType = %d, want %d", err.ExpectedType, tt.expected)
			}
			if err.GotType != tt.got {
				t.Errorf("GotType = %d, want %d", err.GotType, tt.got)
			}
		})
	}
}

// TestBaseErrors 测试基础错误变量
func TestBaseErrors(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		check func(error) bool
	}{
		{
			name:  "ErrInvalidOp",
			err:   ErrInvalidOp,
			check: func(err error) bool { return err.Error() == "此 CRDT 类型的操作无效" },
		},
		{
			name:  "ErrInvalidData",
			err:   ErrInvalidData,
			check: func(err error) bool { return err.Error() == "无效的 CRDT 数据" },
		},
		{
			name:  "ErrKeyNotFound",
			err:   ErrKeyNotFound,
			check: func(err error) bool { return err.Error() == "键不存在" },
		},
		{
			name:  "ErrTypeMismatch",
			err:   ErrTypeMismatch,
			check: func(err error) bool { return err.Error() == "类型不匹配" },
		},
		{
			name:  "ErrNilCRDT",
			err:   ErrNilCRDT,
			check: func(err error) bool { return err.Error() == "CRDT 对象为 nil" },
		},
		{
			name:  "ErrSerialization",
			err:   ErrSerialization,
			check: func(err error) bool { return err.Error() == "序列化失败" },
		},
		{
			name:  "ErrDeserialization",
			err:   ErrDeserialization,
			check: func(err error) bool { return err.Error() == "反序列化失败" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Fatal("error should not be nil")
			}
			if !tt.check(tt.err) {
				t.Error("error check failed")
			}
		})
	}
}

// TestErrorWrapping 测试错误包装功能
func TestErrorWrapping(t *testing.T) {
	// 测试InvalidDataError的包装行为
	t.Run("InvalidDataError wraps ErrInvalidData", func(t *testing.T) {
		err := &InvalidDataError{
			CRDTType:   TypeLWW,
			Reason:     "测试错误",
			DataLength: 10,
		}

		// 使用errors.Is检查
		if !errors.Is(err, ErrInvalidData) {
			t.Error("InvalidDataError should wrap ErrInvalidData")
		}

		// 使用errors.As检查
		var dataErr *InvalidDataError
		if !errors.As(err, &dataErr) {
			t.Error("errors.As should succeed for InvalidDataError")
		}
	})

	// 测试InvalidOpError的包装行为
	t.Run("InvalidOpError wraps ErrInvalidOp", func(t *testing.T) {
		err := &InvalidOpError{
			ExpectedOp: "OpLWWRegister",
			GotOp:      "OpMapSet",
			CRDTType:   TypeMap,
		}

		if !errors.Is(err, ErrInvalidOp) {
			t.Error("InvalidOpError should wrap ErrInvalidOp")
		}

		var opErr *InvalidOpError
		if !errors.As(err, &opErr) {
			t.Error("errors.As should succeed for InvalidOpError")
		}
	})

	// 测试TypeMismatchError的包装行为
	t.Run("TypeMismatchError wraps ErrTypeMismatch", func(t *testing.T) {
		err := &TypeMismatchError{
			Key:         "column1",
			ExpectedType: TypeLWW,
			GotType:     TypeORSet,
		}

		if !errors.Is(err, ErrTypeMismatch) {
			t.Error("TypeMismatchError should wrap ErrTypeMismatch")
		}

		var typeErr *TypeMismatchError
		if !errors.As(err, &typeErr) {
			t.Error("errors.As should succeed for TypeMismatchError")
		}
	})

	// 测试多层包装
	t.Run("multi-level wrapping", func(t *testing.T) {
		inner := &KeyNotFoundError{
			Key: "column1",
		}
		wrapped := errors.New("wrapped: " + inner.Error())

		// 这不会通过errors.Is检测到，因为不是真正的包装
		if errors.Is(wrapped, ErrKeyNotFound) {
			t.Error("plain string wrapping should not be identifiable via errors.Is")
		}
	})
}

// TestErrorMessageContains 测试错误消息包含关键字段
func TestErrorMessageContains(t *testing.T) {
	t.Run("InvalidDataError contains fields", func(t *testing.T) {
		err := &InvalidDataError{
			CRDTType:   TypePNCounter,
			Reason:     "数据不足",
			DataLength: 10,
		}
		msg := err.Error()
		if !strings.Contains(msg, "3") {
			t.Error("message should contain CRDTType (3 for PNCounter)")
		}
		if !strings.Contains(msg, "数据不足") {
			t.Error("message should contain Reason")
		}
		if !strings.Contains(msg, "10") {
			t.Error("message should contain DataLength")
		}
	})

	t.Run("InvalidOpError contains fields", func(t *testing.T) {
		err := &InvalidOpError{
			ExpectedOp: "OpORSetAdd",
			GotOp:      "OpLWWRegister",
			CRDTType:   TypeORSet,
		}
		msg := err.Error()
		if !strings.Contains(msg, "2") {
			t.Error("message should contain CRDTType (2 for ORSet)")
		}
		if !strings.Contains(msg, "OpORSetAdd") {
			t.Error("message should contain ExpectedOp")
		}
		if !strings.Contains(msg, "OpLWWRegister") {
			t.Error("message should contain GotOp")
		}
	})

	t.Run("TypeMismatchError contains fields", func(t *testing.T) {
		err := &TypeMismatchError{
			Key:         "my_key",
			ExpectedType: TypeRGA,
			GotType:     TypeLWW,
		}
		msg := err.Error()
		if !strings.Contains(msg, "my_key") {
			t.Error("message should contain Key")
		}
		if !strings.Contains(msg, "4") {
			t.Error("message should contain ExpectedType (4 for RGA)")
		}
		if !strings.Contains(msg, "1") {
			t.Error("message should contain GotType (1 for LWW)")
		}
	})

	t.Run("KeyNotFoundError contains key", func(t *testing.T) {
		err := &KeyNotFoundError{
			Key: "special_key",
		}
		msg := err.Error()
		if !strings.Contains(msg, "special_key") {
			t.Error("message should contain Key")
		}
		if !strings.Contains(msg, "不存在") {
			t.Error("message should contain Chinese text")
		}
	})
}

// TestTypeConstants 测试Type常量
func TestTypeConstants(t *testing.T) {
	tests := []struct {
		name     string
		typeVal  Type
		expected byte
	}{
		{"TypeLWW", TypeLWW, 0x01},
		{"TypeORSet", TypeORSet, 0x02},
		{"TypePNCounter", TypePNCounter, 0x03},
		{"TypeRGA", TypeRGA, 0x04},
		{"TypeMap", TypeMap, 0x05},
		{"TypeLocalFile", TypeLocalFile, 0x06},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if byte(tt.typeVal) != tt.expected {
				t.Errorf("Type value = %v, want %v", byte(tt.typeVal), tt.expected)
			}
		})
	}
}
