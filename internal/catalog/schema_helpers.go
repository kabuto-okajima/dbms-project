package catalog

import (
	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

// ColumnOrdinal finds where a column sits inside the table's row layout.
func ColumnOrdinal(table *TableMetadata, columnName string) (int, error) {
	for i, column := range table.Columns {
		if column.Name == columnName {
			return i, nil
		}
	}

	return 0, shared.NewError(shared.ErrNotFound, "column %q does not exist on table %q", columnName, table.Name)
}

// ColumnByName returns both the column definition and its ordinal.
func ColumnByName(table *TableMetadata, columnName string) (shared.ColumnDefinition, int, error) {
	ordinal, err := ColumnOrdinal(table, columnName)
	if err != nil {
		return shared.ColumnDefinition{}, 0, err
	}

	return table.Columns[ordinal], ordinal, nil
}

// ValidateRowValues checks that one full-row payload matches the table schema.
func ValidateRowValues(table *TableMetadata, values []storage.Value) error {
	if len(values) != len(table.Columns) {
		return shared.NewError(
			shared.ErrInvalidDefinition,
			"row for table %q has %d values but schema requires %d",
			table.Name,
			len(values),
			len(table.Columns),
		)
	}

	for i, column := range table.Columns {
		if values[i].Type != column.Type {
			return shared.NewError(
				shared.ErrTypeMismatch,
				"value for column %q on table %q has type %q but schema requires %q",
				column.Name,
				table.Name,
				values[i].Type,
				column.Type,
			)
		}
	}

	return nil
}

// BuildRowForInsert converts one INSERT input into full schema order.
//
// If no explicit column list is provided, values are assumed to already be in
// schema order. For now, partial-column inserts are rejected because the engine
// does not support NULL/default filling yet.
func BuildRowForInsert(table *TableMetadata, columnNames []string, values []storage.Value) ([]storage.Value, error) {
	if len(columnNames) == 0 {
		if err := ValidateRowValues(table, values); err != nil {
			return nil, err
		}

		row := make([]storage.Value, len(values))
		copy(row, values)
		return row, nil
	}

	if len(columnNames) != len(values) {
		return nil, shared.NewError(
			shared.ErrInvalidDefinition,
			"insert into table %q specifies %d columns but %d values",
			table.Name,
			len(columnNames),
			len(values),
		)
	}
	if len(columnNames) != len(table.Columns) {
		return nil, shared.NewError(
			shared.ErrInvalidDefinition,
			"insert into table %q must provide all %d columns until defaults or NULLs are supported",
			table.Name,
			len(table.Columns),
		)
	}

	row := make([]storage.Value, len(table.Columns))
	seen := make(map[string]struct{}, len(columnNames))
	for i, columnName := range columnNames {
		if _, exists := seen[columnName]; exists {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "insert into table %q repeats column %q", table.Name, columnName)
		}
		seen[columnName] = struct{}{}

		_, ordinal, err := ColumnByName(table, columnName)
		if err != nil {
			return nil, err
		}
		row[ordinal] = values[i]
	}

	if err := ValidateRowValues(table, row); err != nil {
		return nil, err
	}

	return row, nil
}
