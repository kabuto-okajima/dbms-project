package statement

import (
	"dbms-project/internal/catalog"
	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

// InsertStatement is the minimal INSERT shape for VALUES-based execution.
type InsertStatement struct {
	TableName   string
	ColumnNames []string // optional, if empty values are assumed to be in schema order
	Values      []storage.Value
}

// InsertExecutor validates INSERT inputs and writes rows into table storage.
type InsertExecutor struct {
	Catalog *catalog.Manager
}

func NewInsertExecutor(manager *catalog.Manager) *InsertExecutor {
	return &InsertExecutor{Catalog: manager}
}

// Execute confirms the target table exists, validates the row shape, and stores it.
func (e *InsertExecutor) Execute(tx *storage.Tx, stmt InsertStatement) (Result, error) {
	if e.Catalog == nil {
		e.Catalog = catalog.NewManager()
	}

	table, err := e.Catalog.GetTable(tx, stmt.TableName)
	if err != nil {
		return Result{}, err
	}

	rowValues, err := catalog.BuildRowForInsert(table, stmt.ColumnNames, stmt.Values)
	if err != nil {
		return Result{}, err
	}
	if err := e.ensurePrimaryKeyUnique(tx, table, rowValues); err != nil {
		return Result{}, err
	}
	if err := e.ensureForeignKeysExist(tx, table, rowValues); err != nil {
		return Result{}, err
	}

	rid, err := e.Catalog.NextRID(tx, stmt.TableName)
	if err != nil {
		return Result{}, err
	}

	payload, err := storage.EncodeRow(storage.Row(rowValues))
	if err != nil {
		return Result{}, err
	}
	if err := tx.Put(table.TableBucket, storage.EncodeRID(rid), payload); err != nil {
		return Result{}, err
	}
	if err := e.updateIndexes(tx, table, rowValues, rid); err != nil {
		return Result{}, err
	}

	return Result{
		Message:      "1 row inserted",
		AffectedRows: 1,
	}, nil
}

func (e *InsertExecutor) updateIndexes(tx *storage.Tx, table *catalog.TableMetadata, values []storage.Value, rid storage.RID) error {
	// Get all indexes for the table
	indexes, err := e.Catalog.ListIndexesByTable(tx, table.Name)
	if err != nil {
		return err
	}

	for _, index := range indexes {
		columnOrdinal, err := catalog.ColumnOrdinal(table, index.ColumnName)
		if err != nil {
			return err
		}

		indexKey, err := storage.EncodeIndexKey(values[columnOrdinal])
		if err != nil {
			return err
		}

		existingData, err := tx.Get(index.IndexBucket, indexKey)
		if err != nil {
			return err
		}

		existingRIDs, err := storage.DecodeRIDList(existingData)
		if err != nil {
			return err
		}
		if index.IsUnique && len(existingRIDs) > 0 {
			return shared.NewError(shared.ErrConstraintViolation, "insert: duplicate key for unique index %q", index.Name)
		}

		existingRIDs = append(existingRIDs, rid)
		payload, err := storage.EncodeRIDList(existingRIDs)
		if err != nil {
			return err
		}
		if err := tx.Put(index.IndexBucket, indexKey, payload); err != nil {
			return err
		}
	}

	return nil
}

// ensurePrimaryKeyUnique rejects an INSERT when the table declares a primary
// key and an existing row already has the same PK value.
func (e *InsertExecutor) ensurePrimaryKeyUnique(tx *storage.Tx, table *catalog.TableMetadata, values []storage.Value) error {
	if len(table.PrimaryKey) == 0 {
		return nil
	}

	pkOrdinals := make([]int, len(table.PrimaryKey))
	for i, columnName := range table.PrimaryKey {
		ordinal, err := catalog.ColumnOrdinal(table, columnName)
		if err != nil {
			return err
		}
		pkOrdinals[i] = ordinal
	}

	return tx.ForEach(table.TableBucket, func(_ []byte, payload []byte) error {
		row, err := storage.DecodeRow(payload)
		if err != nil {
			return err
		}

		matches := true
		for _, ordinal := range pkOrdinals {
			if ordinal >= len(row) || row[ordinal] != values[ordinal] {
				matches = false
				break
			}
		}
		if matches {
			return shared.NewError(shared.ErrConstraintViolation, "insert: duplicate primary key on table %q", table.Name)
		}

		return nil
	})
}

// ensureForeignKeysExist checks FK constraints only when the table schema
// declares foreign keys. Each child FK value must match an existing parent-row
// value in the referenced table/column before the insert is allowed.
func (e *InsertExecutor) ensureForeignKeysExist(tx *storage.Tx, table *catalog.TableMetadata, values []storage.Value) error {
	for _, fk := range table.ForeignKeys {
		childOrdinal, err := catalog.ColumnOrdinal(table, fk.ColumnName)
		if err != nil {
			return err
		}

		parentTable, err := e.Catalog.GetTable(tx, fk.RefTable)
		if err != nil {
			return err
		}
		parentOrdinal, err := catalog.ColumnOrdinal(parentTable, fk.RefColumn)
		if err != nil {
			return err
		}

		foundMatch := false
		if err := tx.ForEach(parentTable.TableBucket, func(_ []byte, payload []byte) error {
			row, err := storage.DecodeRow(payload)
			if err != nil {
				return err
			}
			if parentOrdinal < len(row) && row[parentOrdinal] == values[childOrdinal] {
				foundMatch = true
			}
			return nil
		}); err != nil {
			return err
		}

		if !foundMatch {
			return shared.NewError(
				shared.ErrConstraintViolation,
				"insert: foreign key %q on table %q references missing value in %q.%q",
				fk.ColumnName,
				table.Name,
				fk.RefTable,
				fk.RefColumn,
			)
		}
	}

	return nil
}
