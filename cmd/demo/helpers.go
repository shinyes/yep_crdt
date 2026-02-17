package main

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/google/uuid"
	"github.com/shinyes/yep_crdt/pkg/db"
)

func listNotes(table *db.Table) error {
	rawRows, err := table.ScanRawRows()
	if err != nil {
		return err
	}

	type note struct {
		id  uuid.UUID
		row map[string]any
	}
	rows := make([]note, 0, len(rawRows))

	for _, raw := range rawRows {
		row, getErr := table.Get(raw.Key)
		if getErr != nil {
			continue
		}
		rows = append(rows, note{id: raw.Key, row: row})
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].id.String() < rows[j].id.String()
	})

	if len(rows) == 0 {
		fmt.Println("(empty)")
		return nil
	}

	for _, item := range rows {
		printNote(item.id, item.row)
	}
	return nil
}

func parseUUID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("invalid UUID: %w", err)
	}
	return id, nil
}

func parseCounterArgs(parts []string, usage string) (uuid.UUID, int, error) {
	if len(parts) < 2 {
		return uuid.UUID{}, 0, fmt.Errorf("usage: %s", usage)
	}
	id, err := parseUUID(parts[1])
	if err != nil {
		return uuid.UUID{}, 0, err
	}
	delta := 1
	if len(parts) >= 3 {
		delta, err = strconv.Atoi(parts[2])
		if err != nil {
			return uuid.UUID{}, 0, fmt.Errorf("invalid number: %w", err)
		}
		if delta <= 0 {
			return uuid.UUID{}, 0, fmt.Errorf("n must be > 0")
		}
	}
	return id, delta, nil
}

func printNote(id uuid.UUID, row map[string]any) {
	fmt.Printf("%s title=%q views=%d\n",
		id,
		asString(row["title"]),
		asInt64(row["views"]),
	)
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func asInt64(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case uint:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x)
	case float32:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		if i, err := strconv.ParseInt(x, 10, 64); err == nil {
			return i
		}
	case []byte:
		if i, err := strconv.ParseInt(string(x), 10, 64); err == nil {
			return i
		}
	}
	return 0
}
