package db

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/shinyes/yep_crdt/pkg/meta"
)

func normalizeColumnType(colType meta.ColumnType) meta.ColumnType {
	switch colType {
	case "":
		return meta.ColTypeString
	default:
		return colType
	}
}

func encodeLWWValueByColumnType(colType meta.ColumnType, value any) ([]byte, error) {
	switch normalizeColumnType(colType) {
	case meta.ColTypeString:
		switch v := value.(type) {
		case string:
			return []byte(v), nil
		case []byte:
			b := make([]byte, len(v))
			copy(b, v)
			return b, nil
		default:
			return []byte(fmt.Sprintf("%v", value)), nil
		}

	case meta.ColTypeBytes:
		switch v := value.(type) {
		case []byte:
			b := make([]byte, len(v))
			copy(b, v)
			return b, nil
		case string:
			return []byte(v), nil
		default:
			return nil, fmt.Errorf("expects []byte or string, got %T", value)
		}

	case meta.ColTypeInt:
		i, ok := parseInt64Like(value)
		if !ok {
			return nil, fmt.Errorf("expects int-compatible value, got %T", value)
		}
		return []byte(strconv.FormatInt(i, 10)), nil

	case meta.ColTypeFloat:
		f, ok := parseFloat64Like(value)
		if !ok {
			return nil, fmt.Errorf("expects float-compatible value, got %T", value)
		}
		return []byte(strconv.FormatFloat(f, 'g', -1, 64)), nil

	case meta.ColTypeBool:
		bv, ok := parseBoolLike(value)
		if !ok {
			return nil, fmt.Errorf("expects bool-compatible value, got %T", value)
		}
		return []byte(strconv.FormatBool(bv)), nil

	case meta.ColTypeTimestamp:
		ts, ok := parseTimestampLike(value)
		if !ok {
			return nil, fmt.Errorf("expects timestamp-compatible value, got %T", value)
		}
		return []byte(ts.UTC().Format(time.RFC3339Nano)), nil

	default:
		// Keep forward compatibility for unknown custom types.
		return []byte(fmt.Sprintf("%v", value)), nil
	}
}

func decodeLWWValueByColumnType(colType meta.ColumnType, raw any) (any, error) {
	switch normalizeColumnType(colType) {
	case meta.ColTypeString:
		switch v := raw.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			return fmt.Sprintf("%v", raw), nil
		}

	case meta.ColTypeBytes:
		switch v := raw.(type) {
		case []byte:
			b := make([]byte, len(v))
			copy(b, v)
			return b, nil
		case string:
			return []byte(v), nil
		default:
			return nil, fmt.Errorf("cannot decode %T as bytes", raw)
		}

	case meta.ColTypeInt:
		i, ok := parseInt64Like(raw)
		if !ok {
			return nil, fmt.Errorf("cannot decode %T as int", raw)
		}
		return i, nil

	case meta.ColTypeFloat:
		f, ok := parseFloat64Like(raw)
		if !ok {
			return nil, fmt.Errorf("cannot decode %T as float", raw)
		}
		return f, nil

	case meta.ColTypeBool:
		bv, ok := parseBoolLike(raw)
		if !ok {
			return nil, fmt.Errorf("cannot decode %T as bool", raw)
		}
		return bv, nil

	case meta.ColTypeTimestamp:
		ts, ok := parseTimestampLike(raw)
		if !ok {
			return nil, fmt.Errorf("cannot decode %T as timestamp", raw)
		}
		return ts, nil

	default:
		return raw, nil
	}
}

func normalizeValueByColumnType(colType meta.ColumnType, value any) (any, error) {
	switch normalizeColumnType(colType) {
	case meta.ColTypeString:
		switch v := value.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			return fmt.Sprintf("%v", value), nil
		}
	case meta.ColTypeBytes:
		switch v := value.(type) {
		case []byte:
			b := make([]byte, len(v))
			copy(b, v)
			return b, nil
		case string:
			return []byte(v), nil
		default:
			return nil, fmt.Errorf("expects []byte or string, got %T", value)
		}
	case meta.ColTypeInt:
		i, ok := parseInt64Like(value)
		if !ok {
			return nil, fmt.Errorf("expects int-compatible value, got %T", value)
		}
		return i, nil
	case meta.ColTypeFloat:
		f, ok := parseFloat64Like(value)
		if !ok {
			return nil, fmt.Errorf("expects float-compatible value, got %T", value)
		}
		return f, nil
	case meta.ColTypeBool:
		bv, ok := parseBoolLike(value)
		if !ok {
			return nil, fmt.Errorf("expects bool-compatible value, got %T", value)
		}
		return bv, nil
	case meta.ColTypeTimestamp:
		ts, ok := parseTimestampLike(value)
		if !ok {
			return nil, fmt.Errorf("expects timestamp-compatible value, got %T", value)
		}
		return ts.UTC(), nil
	default:
		return value, nil
	}
}

func parseInt64Like(v any) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case uint:
		if uint64(val) > math.MaxInt64 {
			return 0, false
		}
		return int64(val), true
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		if val > math.MaxInt64 {
			return 0, false
		}
		return int64(val), true
	case float32:
		if math.IsNaN(float64(val)) || math.IsInf(float64(val), 0) {
			return 0, false
		}
		return int64(val), true
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return 0, false
		}
		return int64(val), true
	case string:
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	case []byte:
		i, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}

func parseFloat64Like(v any) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		if math.IsNaN(float64(val)) || math.IsInf(float64(val), 0) {
			return 0, false
		}
		return float64(val), true
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return 0, false
		}
		return val, true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	case []byte:
		f, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func parseBoolLike(v any) (bool, bool) {
	switch val := v.(type) {
	case bool:
		return val, true
	case string:
		b, err := strconv.ParseBool(val)
		if err != nil {
			return false, false
		}
		return b, true
	case []byte:
		b, err := strconv.ParseBool(string(val))
		if err != nil {
			return false, false
		}
		return b, true
	default:
		return false, false
	}
}

func parseTimestampLike(v any) (time.Time, bool) {
	switch val := v.(type) {
	case time.Time:
		return val.UTC(), true
	case int:
		return timestampFromUnixAuto(int64(val)), true
	case int64:
		return timestampFromUnixAuto(val), true
	case uint64:
		if val > math.MaxInt64 {
			return time.Time{}, false
		}
		return timestampFromUnixAuto(int64(val)), true
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return time.Time{}, false
		}
		return timestampFromUnixAuto(int64(val)), true
	case string:
		if ts, ok := parseTimestampString(val); ok {
			return ts.UTC(), true
		}
		return time.Time{}, false
	case []byte:
		if ts, ok := parseTimestampString(string(val)); ok {
			return ts.UTC(), true
		}
		return time.Time{}, false
	default:
		return time.Time{}, false
	}
}

func parseTimestampString(text string) (time.Time, bool) {
	if text == "" {
		return time.Time{}, false
	}
	if ts, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return ts, true
	}
	if i, err := strconv.ParseInt(text, 10, 64); err == nil {
		return timestampFromUnixAuto(i), true
	}
	return time.Time{}, false
}

func timestampFromUnixAuto(v int64) time.Time {
	abs := v
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1_000_000_000_000_000_000:
		return time.Unix(0, v).UTC()
	case abs >= 1_000_000_000_000_000:
		return time.UnixMicro(v).UTC()
	case abs >= 1_000_000_000_000:
		return time.UnixMilli(v).UTC()
	default:
		return time.Unix(v, 0).UTC()
	}
}

func bytesFromAny(value any) ([]byte, bool) {
	switch v := value.(type) {
	case []byte:
		out := make([]byte, len(v))
		copy(out, v)
		return out, true
	case string:
		return []byte(v), true
	default:
		return nil, false
	}
}

func fallbackCompare(a, b any) int {
	return bytes.Compare([]byte(fmt.Sprintf("%v", a)), []byte(fmt.Sprintf("%v", b)))
}
