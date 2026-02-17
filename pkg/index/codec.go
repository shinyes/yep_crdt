package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// 索引键的前缀
const IndexPrefix byte = 'i'

// EncodeKey 生成索引条目的 BadgerDB 键。
// 格式：前缀(1) + TableID(4) + IndexID(4) + 值... + PK
func EncodeKey(tableID uint32, indexID uint32, values []any, pk []byte) ([]byte, error) {
	buf := new(bytes.Buffer)

	// 1. 前缀
	buf.WriteByte(IndexPrefix)

	// 2. TableID
	if err := binary.Write(buf, binary.BigEndian, tableID); err != nil {
		return nil, err
	}

	// 3. IndexID
	if err := binary.Write(buf, binary.BigEndian, indexID); err != nil {
		return nil, err
	}

	// 4. 值 (保持顺序)
	for _, v := range values {
		if err := encodeValue(buf, v); err != nil {
			return nil, err
		}
	}

	// 5. PK (在末尾以保证唯一性和查找)
	buf.Write(pk)

	return buf.Bytes(), nil
}

func encodeValue(buf *bytes.Buffer, v any) error {
	switch val := v.(type) {
	case int:
		return encodeSignedInt64(buf, int64(val))
	case int8:
		return encodeSignedInt64(buf, int64(val))
	case int16:
		return encodeSignedInt64(buf, int64(val))
	case int32:
		return encodeSignedInt64(buf, int64(val))
	case int64:
		return encodeSignedInt64(buf, val)
	case uint:
		if uint64(val) > math.MaxInt64 {
			return fmt.Errorf("unsupported index type: uint overflow %d", val)
		}
		return encodeSignedInt64(buf, int64(val))
	case uint8:
		return encodeSignedInt64(buf, int64(val))
	case uint16:
		return encodeSignedInt64(buf, int64(val))
	case uint32:
		return encodeSignedInt64(buf, int64(val))
	case uint64:
		if val > math.MaxInt64 {
			return fmt.Errorf("unsupported index type: uint64 overflow %d", val)
		}
		return encodeSignedInt64(buf, int64(val))
	case float32:
		return encodeFloat64(buf, float64(val))
	case float64:
		return encodeFloat64(buf, val)
	case bool:
		if val {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	case time.Time:
		return encodeSignedInt64(buf, val.UTC().UnixNano())
	case string:
		buf.WriteString(val)
		buf.WriteByte(0x00) // 空终止符
	case []byte:
		if err := binary.Write(buf, binary.BigEndian, uint32(len(val))); err != nil {
			return err
		}
		buf.Write(val)
	default:
		return fmt.Errorf("unsupported index type: %T", v)
	}
	return nil
}

func encodeSignedInt64(buf *bytes.Buffer, val int64) error {
	encoded := uint64(val) ^ 0x8000000000000000
	return binary.Write(buf, binary.BigEndian, encoded)
}

func encodeFloat64(buf *bytes.Buffer, val float64) error {
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return fmt.Errorf("unsupported index type: invalid float64 %v", val)
	}
	bits := math.Float64bits(val)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	return binary.Write(buf, binary.BigEndian, bits)
}

// EncodePrefix 生成用于迭代的前缀 (例如 "WHERE age > 20")
// 格式：前缀(1) + TableID(4) + IndexID(4) + [部分值]
func EncodePrefix(tableID uint32, indexID uint32, values []any) ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(IndexPrefix)
	binary.Write(buf, binary.BigEndian, tableID)
	binary.Write(buf, binary.BigEndian, indexID)
	for _, v := range values {
		if err := encodeValue(buf, v); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
