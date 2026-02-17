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

const (
	indexTypeInt       byte = 0x01
	indexTypeFloat     byte = 0x02
	indexTypeBool      byte = 0x03
	indexTypeTimestamp byte = 0x04
	indexTypeString    byte = 0x05
	indexTypeBytes     byte = 0x06
)

const indexHeaderLen = 1 + 4 + 4

// EncodeKey 生成索引条目的 BadgerDB 键。
// 格式：前缀(1) + TableID(4) + IndexID(4) + TLV 值... + PK
func EncodeKey(tableID uint32, indexID uint32, values []any, pk []byte) ([]byte, error) {
	buf := new(bytes.Buffer)

	writeIndexHeader(buf, tableID, indexID)

	// TLV 值 (保持顺序)
	for _, v := range values {
		if err := encodeValue(buf, v); err != nil {
			return nil, err
		}
	}

	// PK (在末尾以保证唯一性和查找)
	buf.Write(pk)

	return buf.Bytes(), nil
}

func encodeValue(buf *bytes.Buffer, v any) error {
	switch val := v.(type) {
	case int:
		return writeEncodedUint64TLV(buf, indexTypeInt, sortableInt64(int64(val)))
	case int8:
		return writeEncodedUint64TLV(buf, indexTypeInt, sortableInt64(int64(val)))
	case int16:
		return writeEncodedUint64TLV(buf, indexTypeInt, sortableInt64(int64(val)))
	case int32:
		return writeEncodedUint64TLV(buf, indexTypeInt, sortableInt64(int64(val)))
	case int64:
		return writeEncodedUint64TLV(buf, indexTypeInt, sortableInt64(val))
	case uint:
		if uint64(val) > math.MaxInt64 {
			return fmt.Errorf("unsupported index type: uint overflow %d", val)
		}
		return writeEncodedUint64TLV(buf, indexTypeInt, sortableInt64(int64(val)))
	case uint8:
		return writeEncodedUint64TLV(buf, indexTypeInt, sortableInt64(int64(val)))
	case uint16:
		return writeEncodedUint64TLV(buf, indexTypeInt, sortableInt64(int64(val)))
	case uint32:
		return writeEncodedUint64TLV(buf, indexTypeInt, sortableInt64(int64(val)))
	case uint64:
		if val > math.MaxInt64 {
			return fmt.Errorf("unsupported index type: uint64 overflow %d", val)
		}
		return writeEncodedUint64TLV(buf, indexTypeInt, sortableInt64(int64(val)))
	case float32:
		encoded, err := sortableFloat64(float64(val))
		if err != nil {
			return err
		}
		return writeEncodedUint64TLV(buf, indexTypeFloat, encoded)
	case float64:
		encoded, err := sortableFloat64(val)
		if err != nil {
			return err
		}
		return writeEncodedUint64TLV(buf, indexTypeFloat, encoded)
	case bool:
		payload := []byte{0}
		if val {
			payload[0] = 1
		}
		return writeTLV(buf, indexTypeBool, payload)
	case time.Time:
		return writeEncodedUint64TLV(buf, indexTypeTimestamp, sortableInt64(val.UTC().UnixNano()))
	case string:
		return writeTLV(buf, indexTypeString, []byte(val))
	case []byte:
		return writeTLV(buf, indexTypeBytes, val)
	default:
		return fmt.Errorf("unsupported index type: %T", v)
	}
}

func writeIndexHeader(buf *bytes.Buffer, tableID uint32, indexID uint32) {
	buf.WriteByte(IndexPrefix)
	var b [8]byte
	binary.BigEndian.PutUint32(b[0:4], tableID)
	binary.BigEndian.PutUint32(b[4:8], indexID)
	buf.Write(b[:])
}

func writeIndexHeaderTo(dst []byte, tableID uint32, indexID uint32) {
	dst[0] = IndexPrefix
	binary.BigEndian.PutUint32(dst[1:5], tableID)
	binary.BigEndian.PutUint32(dst[5:9], indexID)
}

func writeTLV(buf *bytes.Buffer, typeTag byte, payload []byte) error {
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return fmt.Errorf("unsupported index value size: %d bytes", len(payload))
	}
	buf.WriteByte(typeTag)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	buf.Write(lenBuf[:])
	buf.Write(payload)
	return nil
}

func writeEncodedUint64TLV(buf *bytes.Buffer, typeTag byte, encoded uint64) error {
	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], encoded)
	return writeTLV(buf, typeTag, payload[:])
}

func sortableInt64(val int64) uint64 {
	return uint64(val) ^ 0x8000000000000000
}

func sortableFloat64(val float64) (uint64, error) {
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return 0, fmt.Errorf("unsupported index type: invalid float64 %v", val)
	}
	bits := math.Float64bits(val)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	return bits, nil
}

func decodeSortableInt64(encoded uint64) int64 {
	return int64(encoded ^ 0x8000000000000000)
}

func decodeSortableFloat64(encoded uint64) float64 {
	if encoded&(1<<63) != 0 {
		encoded ^= 1 << 63
	} else {
		encoded = ^encoded
	}
	return math.Float64frombits(encoded)
}

func parseHeader(key []byte) (tableID uint32, indexID uint32, ok bool) {
	if len(key) < indexHeaderLen || key[0] != IndexPrefix {
		return 0, 0, false
	}
	return binary.BigEndian.Uint32(key[1:5]), binary.BigEndian.Uint32(key[5:9]), true
}

func extractTLVValueAt(key []byte, valuePos int) (typeTag byte, payload []byte, err error) {
	if valuePos < 0 {
		return 0, nil, fmt.Errorf("invalid value position: %d", valuePos)
	}
	if len(key) < indexHeaderLen {
		return 0, nil, fmt.Errorf("invalid index key length: %d", len(key))
	}

	offset := indexHeaderLen
	for i := 0; ; i++ {
		if offset+5 > len(key) {
			return 0, nil, fmt.Errorf("value position %d out of range", valuePos)
		}
		tag := key[offset]
		payloadLen := int(binary.BigEndian.Uint32(key[offset+1 : offset+5]))
		start := offset + 5
		end := start + payloadLen
		if end > len(key) {
			return 0, nil, fmt.Errorf("corrupted index key: invalid payload length %d", payloadLen)
		}
		if i == valuePos {
			return tag, key[start:end], nil
		}
		offset = end
	}
}

func decodeTLVValue(typeTag byte, payload []byte) (any, error) {
	switch typeTag {
	case indexTypeInt:
		if len(payload) != 8 {
			return nil, fmt.Errorf("invalid int payload length: %d", len(payload))
		}
		encoded := binary.BigEndian.Uint64(payload)
		return decodeSortableInt64(encoded), nil
	case indexTypeFloat:
		if len(payload) != 8 {
			return nil, fmt.Errorf("invalid float payload length: %d", len(payload))
		}
		encoded := binary.BigEndian.Uint64(payload)
		return decodeSortableFloat64(encoded), nil
	case indexTypeBool:
		if len(payload) != 1 {
			return nil, fmt.Errorf("invalid bool payload length: %d", len(payload))
		}
		return payload[0] == 1, nil
	case indexTypeTimestamp:
		if len(payload) != 8 {
			return nil, fmt.Errorf("invalid timestamp payload length: %d", len(payload))
		}
		encoded := binary.BigEndian.Uint64(payload)
		nano := decodeSortableInt64(encoded)
		return time.Unix(0, nano).UTC(), nil
	case indexTypeString:
		return string(payload), nil
	case indexTypeBytes:
		out := make([]byte, len(payload))
		copy(out, payload)
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported index type tag: %d", typeTag)
	}
}

// DecodeValueAt decodes the N-th index value (0-based) from an encoded index key.
// It expects a key generated by EncodeKey.
func DecodeValueAt(key []byte, valuePos int) (any, error) {
	typeTag, payload, err := extractTLVValueAt(key, valuePos)
	if err != nil {
		return nil, err
	}
	return decodeTLVValue(typeTag, payload)
}

// EncodePrefix 生成用于迭代的前缀 (例如 "WHERE age > 20")
// 格式：前缀(1) + TableID(4) + IndexID(4) + [部分 TLV 值]
func EncodePrefix(tableID uint32, indexID uint32, values []any) ([]byte, error) {
	buf := new(bytes.Buffer)
	writeIndexHeader(buf, tableID, indexID)
	for _, v := range values {
		if err := encodeValue(buf, v); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// IndexKeyMatches reports whether key has the expected index header.
func IndexKeyMatches(key []byte, tableID uint32, indexID uint32) bool {
	tid, iid, ok := parseHeader(key)
	return ok && tid == tableID && iid == indexID
}

// BuildIndexHeader returns raw bytes for index header prefix.
func BuildIndexHeader(tableID uint32, indexID uint32) []byte {
	out := make([]byte, indexHeaderLen)
	writeIndexHeaderTo(out, tableID, indexID)
	return out
}
