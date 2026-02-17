package index

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

const testIndexHeaderLen = 1 + 4 + 4

func decodeFirstTLVForTest(t *testing.T, key []byte) (byte, []byte, []byte) {
	t.Helper()
	if len(key) < testIndexHeaderLen+5 {
		t.Fatalf("key too short for TLV: %d", len(key))
	}
	body := key[testIndexHeaderLen:]
	typeTag := body[0]
	payloadLen := int(binary.BigEndian.Uint32(body[1:5]))
	if len(body) < 5+payloadLen {
		t.Fatalf("invalid TLV length: %d (body=%d)", payloadLen, len(body))
	}
	payload := body[5 : 5+payloadLen]
	rest := body[5+payloadLen:]
	return typeTag, payload, rest
}

func TestEncodeKey(t *testing.T) {
	tests := []struct {
		name     string
		tableID  uint32
		indexID  uint32
		values   []any
		pk       []byte
		wantErr  bool
		validate func(*testing.T, []byte)
	}{
		{
			name:    "basic string value",
			tableID: 1,
			indexID: 1,
			values:  []any{"test"},
			pk:      []byte{1, 2, 3, 4},
			wantErr: false,
			validate: func(t *testing.T, key []byte) {
				// Check prefix
				if key[0] != IndexPrefix {
					t.Errorf("expected prefix %d, got %d", IndexPrefix, key[0])
				}
				typeTag, payload, _ := decodeFirstTLVForTest(t, key)
				if typeTag != indexTypeString {
					t.Fatalf("expected string type tag %d, got %d", indexTypeString, typeTag)
				}
				if string(payload) != "test" {
					t.Fatalf("expected payload test, got %q", payload)
				}
			},
		},
		{
			name:    "int value",
			tableID: 1,
			indexID: 2,
			values:  []any{42},
			pk:      []byte{1, 2, 3, 4},
			wantErr: false,
		},
		{
			name:    "int64 value",
			tableID: 1,
			indexID: 2,
			values:  []any{int64(100)},
			pk:      []byte{1, 2, 3, 4},
			wantErr: false,
		},
		{
			name:    "int64 negative value",
			tableID: 1,
			indexID: 2,
			values:  []any{int64(-50)},
			pk:      []byte{1, 2, 3, 4},
			wantErr: false,
		},
		{
			name:    "byte array value",
			tableID: 1,
			indexID: 3,
			values:  []any{[]byte("binary")},
			pk:      []byte{1, 2, 3, 4},
			wantErr: false,
		},
		{
			name:    "multiple values",
			tableID: 1,
			indexID: 4,
			values:  []any{"name", 25},
			pk:      []byte{1, 2, 3, 4},
			wantErr: false,
		},
		{
			name:    "float64 value",
			tableID: 1,
			indexID: 5,
			values:  []any{3.14},
			pk:      []byte{1, 2, 3, 4},
			wantErr: false,
		},
		{
			name:    "bool value",
			tableID: 1,
			indexID: 6,
			values:  []any{true},
			pk:      []byte{1, 2, 3, 4},
			wantErr: false,
		},
		{
			name:    "timestamp value",
			tableID: 1,
			indexID: 7,
			values:  []any{time.Unix(1700000000, 0).UTC()},
			pk:      []byte{1, 2, 3, 4},
			wantErr: false,
		},
		{
			name:    "unsupported type",
			tableID: 1,
			indexID: 8,
			values:  []any{map[string]any{"x": 1}},
			pk:      []byte{1, 2, 3, 4},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := EncodeKey(tt.tableID, tt.indexID, tt.values, tt.pk)
			if (err != nil) != tt.wantErr {
				t.Errorf("EncodeKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if key == nil {
					t.Error("EncodeKey() returned nil key")
				}
				if tt.validate != nil {
					tt.validate(t, key)
				}
			}
		})
	}
}

func TestEncodeKeyOrdering(t *testing.T) {
	// Test that keys are ordered correctly for range queries
	tableID := uint32(1)
	indexID := uint32(1)
	pk1 := []byte{1, 0, 0, 0}
	pk2 := []byte{2, 0, 0, 0}

	// String ordering for equal-length strings.
	// TLV length prefix comes before payload, so cross-length lexical ordering is not guaranteed.
	key1, _ := EncodeKey(tableID, indexID, []any{"anna"}, pk1)
	key2, _ := EncodeKey(tableID, indexID, []any{"beth"}, pk2)
	if bytes.Compare(key1, key2) >= 0 {
		t.Error("String encoding should produce ordered keys")
	}

	// Int ordering (positive)
	key3, _ := EncodeKey(tableID, indexID, []any{10}, pk1)
	key4, _ := EncodeKey(tableID, indexID, []any{20}, pk2)
	if bytes.Compare(key3, key4) >= 0 {
		t.Error("Int encoding should produce ordered keys")
	}

	// Int64 ordering (negative numbers should work correctly)
	key5, _ := EncodeKey(tableID, indexID, []any{int64(-100)}, pk1)
	key6, _ := EncodeKey(tableID, indexID, []any{int64(-50)}, pk2)
	if bytes.Compare(key5, key6) >= 0 {
		t.Error("Int64 encoding should handle negative numbers correctly")
	}

	// Int64 ordering (mixed)
	key7, _ := EncodeKey(tableID, indexID, []any{int64(-10)}, pk1)
	key8, _ := EncodeKey(tableID, indexID, []any{int64(10)}, pk2)
	if bytes.Compare(key7, key8) >= 0 {
		t.Error("Int64 encoding should order negative before positive")
	}

	// Float ordering
	key9, _ := EncodeKey(tableID, indexID, []any{1.5}, pk1)
	key10, _ := EncodeKey(tableID, indexID, []any{2.5}, pk2)
	if bytes.Compare(key9, key10) >= 0 {
		t.Error("Float encoding should produce ordered keys")
	}

	// Bool ordering: false < true
	key11, _ := EncodeKey(tableID, indexID, []any{false}, pk1)
	key12, _ := EncodeKey(tableID, indexID, []any{true}, pk2)
	if bytes.Compare(key11, key12) >= 0 {
		t.Error("Bool encoding should produce ordered keys")
	}
}

func TestEncodePrefix(t *testing.T) {
	tests := []struct {
		name    string
		tableID uint32
		indexID uint32
		values  []any
		wantErr bool
	}{
		{
			name:    "no values (full table scan)",
			tableID: 1,
			indexID: 1,
			values:  []any{},
			wantErr: false,
		},
		{
			name:    "single value",
			tableID: 1,
			indexID: 1,
			values:  []any{"test"},
			wantErr: false,
		},
		{
			name:    "multiple values (composite index)",
			tableID: 1,
			indexID: 1,
			values:  []any{"name", 25},
			wantErr: false,
		},
		{
			name:    "float supported",
			tableID: 1,
			indexID: 1,
			values:  []any{3.14},
			wantErr: false,
		},
		{
			name:    "unsupported type",
			tableID: 1,
			indexID: 1,
			values:  []any{struct{}{}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix, err := EncodePrefix(tt.tableID, tt.indexID, tt.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("EncodePrefix() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && prefix == nil {
				t.Error("EncodePrefix() returned nil prefix")
			}
		})
	}
}

func TestEncodePrefixVsKey(t *testing.T) {
	// Test that prefix correctly matches keys
	tableID := uint32(1)
	indexID := uint32(1)
	pk := []byte{1, 2, 3, 4}

	// Single value
	prefix, _ := EncodePrefix(tableID, indexID, []any{"test"})
	key, _ := EncodeKey(tableID, indexID, []any{"test"}, pk)

	if !bytes.HasPrefix(key, prefix) {
		t.Error("Key should have the prefix")
	}

	// Multiple values
	prefix2, _ := EncodePrefix(tableID, indexID, []any{"name", 25})
	key2, _ := EncodeKey(tableID, indexID, []any{"name", 25, "extra"}, pk)

	if !bytes.HasPrefix(key2, prefix2) {
		t.Error("Key should have the composite prefix")
	}
}

func TestEncodePrefixWithEmbeddedZeroBytes(t *testing.T) {
	tableID := uint32(3)
	indexID := uint32(9)
	pk := []byte{9, 9, 9, 9}

	stringVal := "a\x00b"
	stringPrefix, err := EncodePrefix(tableID, indexID, []any{stringVal})
	if err != nil {
		t.Fatalf("EncodePrefix string failed: %v", err)
	}
	stringKey, err := EncodeKey(tableID, indexID, []any{stringVal, 123}, pk)
	if err != nil {
		t.Fatalf("EncodeKey string failed: %v", err)
	}
	if !bytes.HasPrefix(stringKey, stringPrefix) {
		t.Fatal("string key should match prefix even with embedded zero bytes")
	}

	bytesVal := []byte{0x00, 0x01, 0x00, 0xFF}
	bytesPrefix, err := EncodePrefix(tableID, indexID, []any{bytesVal})
	if err != nil {
		t.Fatalf("EncodePrefix bytes failed: %v", err)
	}
	bytesKey, err := EncodeKey(tableID, indexID, []any{bytesVal, int64(7)}, pk)
	if err != nil {
		t.Fatalf("EncodeKey bytes failed: %v", err)
	}
	if !bytes.HasPrefix(bytesKey, bytesPrefix) {
		t.Fatal("bytes key should match prefix even with embedded zero bytes")
	}
}

func TestEncodeKeyTypeTagDistinguishesStringAndBytes(t *testing.T) {
	tableID := uint32(10)
	indexID := uint32(10)
	pk := []byte{1, 2, 3, 4}

	stringKey, err := EncodeKey(tableID, indexID, []any{"ab"}, pk)
	if err != nil {
		t.Fatalf("EncodeKey string failed: %v", err)
	}
	bytesKey, err := EncodeKey(tableID, indexID, []any{[]byte("ab")}, pk)
	if err != nil {
		t.Fatalf("EncodeKey bytes failed: %v", err)
	}

	if bytes.Equal(stringKey, bytesKey) {
		t.Fatal("string and bytes keys should differ by type tag")
	}

	stringTag, _, _ := decodeFirstTLVForTest(t, stringKey)
	bytesTag, _, _ := decodeFirstTLVForTest(t, bytesKey)
	if stringTag != indexTypeString {
		t.Fatalf("expected string tag %d, got %d", indexTypeString, stringTag)
	}
	if bytesTag != indexTypeBytes {
		t.Fatalf("expected bytes tag %d, got %d", indexTypeBytes, bytesTag)
	}
}

func TestEncodeValueInt64Negative(t *testing.T) {
	// Test that negative int64 values are encoded correctly for ordering
	buf := new(bytes.Buffer)

	// Encode -1
	err := encodeValue(buf, int64(-1))
	if err != nil {
		t.Fatalf("encodeValue failed: %v", err)
	}

	neg1Bytes := buf.Bytes()

	// Encode 0
	buf = new(bytes.Buffer)
	err = encodeValue(buf, int64(0))
	if err != nil {
		t.Fatalf("encodeValue failed: %v", err)
	}

	zeroBytes := buf.Bytes()

	// Encode 1
	buf = new(bytes.Buffer)
	err = encodeValue(buf, int64(1))
	if err != nil {
		t.Fatalf("encodeValue failed: %v", err)
	}

	pos1Bytes := buf.Bytes()

	// Check ordering: -1 < 0 < 1
	if bytes.Compare(neg1Bytes, zeroBytes) >= 0 {
		t.Error("Encoding of -1 should be less than 0")
	}
	if bytes.Compare(zeroBytes, pos1Bytes) >= 0 {
		t.Error("Encoding of 0 should be less than 1")
	}
}

func TestDecodeValueAt(t *testing.T) {
	ts := time.Unix(1700000000, 123456789).UTC()
	key, err := EncodeKey(7, 8, []any{
		int64(-42),
		3.5,
		true,
		ts,
		"a\x00b",
		[]byte{0x00, 0x01, 0xFF},
	}, []byte{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("EncodeKey failed: %v", err)
	}

	v0, err := DecodeValueAt(key, 0)
	if err != nil || v0.(int64) != -42 {
		t.Fatalf("DecodeValueAt[0] got %v err=%v", v0, err)
	}
	v1, err := DecodeValueAt(key, 1)
	if err != nil || v1.(float64) != 3.5 {
		t.Fatalf("DecodeValueAt[1] got %v err=%v", v1, err)
	}
	v2, err := DecodeValueAt(key, 2)
	if err != nil || v2.(bool) != true {
		t.Fatalf("DecodeValueAt[2] got %v err=%v", v2, err)
	}
	v3, err := DecodeValueAt(key, 3)
	if err != nil || !v3.(time.Time).Equal(ts) {
		t.Fatalf("DecodeValueAt[3] got %v err=%v", v3, err)
	}
	v4, err := DecodeValueAt(key, 4)
	if err != nil || v4.(string) != "a\x00b" {
		t.Fatalf("DecodeValueAt[4] got %v err=%v", v4, err)
	}
	v5, err := DecodeValueAt(key, 5)
	if err != nil || !bytes.Equal(v5.([]byte), []byte{0x00, 0x01, 0xFF}) {
		t.Fatalf("DecodeValueAt[5] got %v err=%v", v5, err)
	}
}
