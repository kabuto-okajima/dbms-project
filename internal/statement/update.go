package statement

import (
	"fmt"

	"dbms-project/internal/catalog"
	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

// UpdateStatement is the minimal UPDATE shape for one-column assignment.
//
// The first version supports:
// UPDATE table SET column = value
type UpdateStatement struct {
	TableName  string
	ColumnName string
	Value      storage.Value
	Where      Expression
	Matcher    RowPredicate
}

// UpdateExecutor will rewrite rows and maintain constraints/indexes.
type UpdateExecutor struct {
	Catalog *catalog.Manager
}

type rowToUpdate struct {
	RID storage.RID
	Old storage.Row
	New storage.Row
}

func NewUpdateExecutor(manager *catalog.Manager) *UpdateExecutor {
	return &UpdateExecutor{Catalog: manager}
}

// Execute rewrites all matching rows in the target table for one-column assignment.
func (e *UpdateExecutor) Execute(tx *storage.Tx, stmt UpdateStatement) (Result, error) {
	if e.Catalog == nil {
		e.Catalog = catalog.NewManager()
	}
	if err := e.Catalog.Bootstrap(tx); err != nil {
		return Result{}, err
	}

	// Load the target table and resolve which column is being updated.
	table, err := e.Catalog.GetTable(tx, stmt.TableName)
	if err != nil {
		return Result{}, err
	}

	columnOrdinal, err := catalog.ColumnOrdinal(table, stmt.ColumnName)
	if err != nil {
		return Result{}, err
	}

	rowsToUpdate := make([]rowToUpdate, 0)
	if err := tx.ForEach(table.TableBucket, func(key, payload []byte) error {
		// Decode one stored row so we can rewrite the target column.
		row, err := storage.DecodeRow(payload)
		if err != nil {
			return err
		}
		if stmt.Matcher != nil {
			match, err := stmt.Matcher(row)
			if err != nil {
				return err
			}
			if !match {
				return nil
			}
		}

		if columnOrdinal >= len(row) {
			return shared.NewError(shared.ErrInvalidDefinition, "update: row is missing column %q on table %q", stmt.ColumnName, table.Name)
		}

		// Copy the row, apply the new value, and validate the updated shape.
		updatedRow := make(storage.Row, len(row))
		copy(updatedRow, row)
		updatedRow[columnOrdinal] = stmt.Value

		if err := catalog.ValidateRowValues(table, updatedRow); err != nil {
			return err
		}

		rowsToUpdate = append(rowsToUpdate, rowToUpdate{
			RID: storage.DecodeRID(key),
			Old: row,
			New: updatedRow,
		})
		return nil
	}); err != nil {
		return Result{}, err
	}

	if err := e.ensureUpdateAllowed(tx, table, stmt, rowsToUpdate); err != nil {
		return Result{}, err
	}
	if err := e.updateIndexes(tx, table, rowsToUpdate); err != nil {
		return Result{}, err
	}

	for _, row := range rowsToUpdate {
		// Persist the rewritten row back under the same RID key.
		encodedRow, err := storage.EncodeRow(row.New)
		if err != nil {
			return Result{}, err
		}
		if err := tx.Put(table.TableBucket, storage.EncodeRID(row.RID), encodedRow); err != nil {
			return Result{}, err
		}
	}

	return Result{
		Message:      fmt.Sprintf("%d rows updated", len(rowsToUpdate)),
		AffectedRows: len(rowsToUpdate),
	}, nil
}

func (e *UpdateExecutor) ensureUpdateAllowed(tx *storage.Tx, table *catalog.TableMetadata, stmt UpdateStatement, rows []rowToUpdate) error {
	if err := e.ensurePrimaryKeyUnique(tx, table, stmt, rows); err != nil {
		return err
	}
	if err := e.ensureForeignKeysExist(tx, table, stmt, rows); err != nil {
		return err
	}
	if err := e.ensureParentRestrict(tx, table, stmt, rows); err != nil {
		return err
	}
	return nil
}

func (e *UpdateExecutor) ensurePrimaryKeyUnique(tx *storage.Tx, table *catalog.TableMetadata, stmt UpdateStatement, rows []rowToUpdate) error {
	if len(table.PrimaryKey) == 0 || table.PrimaryKey[0] != stmt.ColumnName {
		return nil
	}

	pkOrdinal, err := catalog.ColumnOrdinal(table, table.PrimaryKey[0])
	if err != nil {
		return err
	}

	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		// Get the index key for the new primary key value being updated.
		// e.g., key = 20 for a primary key on column "id" when updating a row to have id=20.
		key, err := storage.EncodeIndexKey(row.New[pkOrdinal])
		if err != nil {
			return err
		}
		mapKey := string(key)
		if _, exists := seen[mapKey]; exists {
			return shared.NewError(shared.ErrConstraintViolation, "update: duplicate primary key on table %q", table.Name)
		}
		seen[mapKey] = struct{}{}
	}

	return nil
}

func (e *UpdateExecutor) ensureForeignKeysExist(tx *storage.Tx, table *catalog.TableMetadata, stmt UpdateStatement, rows []rowToUpdate) error {
	for _, fk := range table.ForeignKeys {
		if fk.ColumnName != stmt.ColumnName {
			continue
		}

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

		for _, row := range rows {
			foundMatch := false
			if err := tx.ForEach(parentTable.TableBucket, func(_ []byte, payload []byte) error {
				parentRow, err := storage.DecodeRow(payload)
				if err != nil {
					return err
				}
				if parentOrdinal < len(parentRow) && parentRow[parentOrdinal] == row.New[childOrdinal] {
					foundMatch = true
				}
				return nil
			}); err != nil {
				return err
			}

			if !foundMatch {
				return shared.NewError(
					shared.ErrConstraintViolation,
					"update: foreign key %q on table %q references missing value in %q.%q",
					fk.ColumnName,
					table.Name,
					fk.RefTable,
					fk.RefColumn,
				)
			}
		}
	}

	return nil
}

func (e *UpdateExecutor) ensureParentRestrict(tx *storage.Tx, table *catalog.TableMetadata, stmt UpdateStatement, rows []rowToUpdate) error {
	referencingFKs, err := e.Catalog.ListReferencingForeignKeyMetadata(tx, table.Name)
	if err != nil {
		return err
	}

	for _, fk := range referencingFKs {
		if fk.RefColumn != stmt.ColumnName {
			continue
		}

		childTable, err := e.Catalog.GetTable(tx, fk.TableName)
		if err != nil {
			return err
		}
		childOrdinal, err := catalog.ColumnOrdinal(childTable, fk.ColumnName)
		if err != nil {
			return err
		}
		parentOrdinal, err := catalog.ColumnOrdinal(table, fk.RefColumn)
		if err != nil {
			return err
		}

		for _, row := range rows {
			if row.Old[parentOrdinal] == row.New[parentOrdinal] {
				continue
			}

			foundReference := false
			if err := tx.ForEach(childTable.TableBucket, func(_ []byte, payload []byte) error {
				childRow, err := storage.DecodeRow(payload)
				if err != nil {
					return err
				}
				if childOrdinal < len(childRow) && childRow[childOrdinal] == row.Old[parentOrdinal] {
					foundReference = true
				}
				return nil
			}); err != nil {
				return err
			}

			if foundReference {
				return shared.NewError(
					shared.ErrConstraintViolation,
					"update: table %q is still referenced by foreign key %q on table %q",
					table.Name,
					fk.ColumnName,
					fk.TableName,
				)
			}
		}
	}

	return nil
}

func (e *UpdateExecutor) updateIndexes(tx *storage.Tx, table *catalog.TableMetadata, rows []rowToUpdate) error {
	indexes, err := e.Catalog.ListIndexesByTable(tx, table.Name)
	if err != nil {
		return err
	}

	for _, row := range rows {
		for _, index := range indexes {
			columnOrdinal, err := catalog.ColumnOrdinal(table, index.ColumnName)
			if err != nil {
				return err
			}
			if columnOrdinal >= len(row.Old) || columnOrdinal >= len(row.New) {
				return shared.NewError(shared.ErrInvalidDefinition, "update: row is missing indexed column %q on table %q", index.ColumnName, table.Name)
			}

			oldKey, err := storage.EncodeIndexKey(row.Old[columnOrdinal])
			if err != nil {
				return err
			}
			newKey, err := storage.EncodeIndexKey(row.New[columnOrdinal])
			if err != nil {
				return err
			}

			// If the indexed value did not change, there is nothing to do for this index.
			if string(oldKey) == string(newKey) {
				continue
			}

			if err := e.removeIndexRID(tx, index.IndexBucket, oldKey, row.RID); err != nil {
				return err
			}
			if err := e.addIndexRID(tx, index, newKey, row.RID); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *UpdateExecutor) removeIndexRID(tx *storage.Tx, bucketName string, indexKey []byte, rid storage.RID) error {
	existingData, err := tx.Get(bucketName, indexKey)
	if err != nil {
		return err
	}

	existingRIDs, err := storage.DecodeRIDList(existingData)
	if err != nil {
		return err
	}

	filtered := make([]storage.RID, 0, len(existingRIDs))
	for _, existingRID := range existingRIDs {
		if existingRID != rid {
			filtered = append(filtered, existingRID)
		}
	}

	if len(filtered) == 0 {
		return tx.Delete(bucketName, indexKey)
	}

	payload, err := storage.EncodeRIDList(filtered)
	if err != nil {
		return err
	}
	return tx.Put(bucketName, indexKey, payload)
}

func (e *UpdateExecutor) addIndexRID(tx *storage.Tx, index catalog.IndexMetadata, indexKey []byte, rid storage.RID) error {
	existingData, err := tx.Get(index.IndexBucket, indexKey)
	if err != nil {
		return err
	}

	existingRIDs, err := storage.DecodeRIDList(existingData)
	if err != nil {
		return err
	}
	if index.IsUnique && len(existingRIDs) > 0 {
		return shared.NewError(shared.ErrConstraintViolation, "update: duplicate key for unique index %q", index.Name)
	}

	existingRIDs = append(existingRIDs, rid)
	payload, err := storage.EncodeRIDList(existingRIDs)
	if err != nil {
		return err
	}
	return tx.Put(index.IndexBucket, indexKey, payload)
}
