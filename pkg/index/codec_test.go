package index

import (
	"bytes"
	"testing"
	"time"
)

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

	// String ordering
	key1, _ := EncodeKey(tableID, indexID, []any{"alice"}, pk1)
	key2, _ := EncodeKey(tableID, indexID, []any{"bob"}, pk2)
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
