package meta

import (
	"fmt"
	"slices"
	"strings"
)

var supportedColumnTypes = map[ColumnType]struct{}{
	ColTypeString:    {},
	ColTypeBytes:     {},
	ColTypeFloat:     {},
	ColTypeInt:       {},
	ColTypeBool:      {},
	ColTypeTimestamp: {},
}

// ValidateTableSchemaShape validates schema structure and rejects malformed input.
func ValidateTableSchemaShape(schema *TableSchema) error {
	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}
	if strings.TrimSpace(schema.Name) == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if _, err := normalizeColumns(schema.Columns); err != nil {
		return err
	}
	if _, err := normalizeIndexes(schema.Indexes); err != nil {
		return err
	}
	return nil
}

// ValidateTableSchemaShapeCompatibility checks schema shape equality by names.
// Column/index declaration order is ignored, but index column order is preserved.
func ValidateTableSchemaShapeCompatibility(existing *TableSchema, incoming *TableSchema) error {
	if err := ValidateTableSchemaShape(existing); err != nil {
		return fmt.Errorf("existing schema invalid: %w", err)
	}
	if err := ValidateTableSchemaShape(incoming); err != nil {
		return fmt.Errorf("incoming schema invalid: %w", err)
	}

	existingColumns, _ := normalizeColumns(existing.Columns)
	incomingColumns, _ := normalizeColumns(incoming.Columns)
	if len(existingColumns) != len(incomingColumns) {
		return fmt.Errorf("column count mismatch: existing=%d incoming=%d", len(existingColumns), len(incomingColumns))
	}
	for name, existingColumn := range existingColumns {
		incomingColumn, ok := incomingColumns[name]
		if !ok {
			return fmt.Errorf("missing column %q", name)
		}
		if existingColumn.Type != incomingColumn.Type || existingColumn.CrdtType != incomingColumn.CrdtType {
			return fmt.Errorf(
				"column %q mismatch: existing=(type=%s crdt=%s) incoming=(type=%s crdt=%s)",
				name,
				existingColumn.Type,
				existingColumn.CrdtType,
				incomingColumn.Type,
				incomingColumn.CrdtType,
			)
		}
	}

	existingIndexes, _ := normalizeIndexes(existing.Indexes)
	incomingIndexes, _ := normalizeIndexes(incoming.Indexes)
	if len(existingIndexes) != len(incomingIndexes) {
		return fmt.Errorf("index count mismatch: existing=%d incoming=%d", len(existingIndexes), len(incomingIndexes))
	}
	for name, existingIndex := range existingIndexes {
		incomingIndex, ok := incomingIndexes[name]
		if !ok {
			return fmt.Errorf("missing index %q", name)
		}
		if existingIndex.Unique != incomingIndex.Unique {
			return fmt.Errorf("index %q unique mismatch: existing=%t incoming=%t", name, existingIndex.Unique, incomingIndex.Unique)
		}
		if !slices.Equal(existingIndex.Columns, incomingIndex.Columns) {
			return fmt.Errorf("index %q columns mismatch: existing=%v incoming=%v", name, existingIndex.Columns, incomingIndex.Columns)
		}
	}

	return nil
}

func normalizeColumns(columns []ColumnSchema) (map[string]ColumnSchema, error) {
	normalized := make(map[string]ColumnSchema, len(columns))
	for _, column := range columns {
		name := strings.TrimSpace(column.Name)
		if name == "" {
			return nil, fmt.Errorf("column name cannot be empty")
		}
		if _, exists := normalized[name]; exists {
			return nil, fmt.Errorf("duplicate column name %q", name)
		}

		colType := ColumnType(strings.TrimSpace(string(column.Type)))
		if colType == "" {
			colType = ColTypeString
		}
		if _, ok := supportedColumnTypes[colType]; !ok {
			return nil, fmt.Errorf("unsupported column type %q for column %q", column.Type, name)
		}

		normalized[name] = ColumnSchema{
			Name:     name,
			Type:     colType,
			CrdtType: column.CrdtType,
		}
	}
	return normalized, nil
}

func normalizeIndexes(indexes []IndexSchema) (map[string]IndexSchema, error) {
	normalized := make(map[string]IndexSchema, len(indexes))
	for _, indexSchema := range indexes {
		name := strings.TrimSpace(indexSchema.Name)
		if name == "" {
			return nil, fmt.Errorf("index name cannot be empty")
		}
		if _, exists := normalized[name]; exists {
			return nil, fmt.Errorf("duplicate index name %q", name)
		}

		columns := make([]string, len(indexSchema.Columns))
		for i, column := range indexSchema.Columns {
			column = strings.TrimSpace(column)
			if column == "" {
				return nil, fmt.Errorf("index %q has empty column at position %d", name, i)
			}
			columns[i] = column
		}

		normalized[name] = IndexSchema{
			Name:    name,
			Columns: columns,
			Unique:  indexSchema.Unique,
		}
	}
	return normalized, nil
}
