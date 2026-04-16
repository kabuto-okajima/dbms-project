package statement

import (
	"fmt"

	"dbms-project/internal/catalog"
	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

// DeleteStatement is the minimal DELETE shape for full-table deletion.
type DeleteStatement struct {
	TableName string
}

// DeleteExecutor removes rows from table storage and keeps indexes in sync.
type DeleteExecutor struct {
	Catalog *catalog.Manager
}

type rowToDelete struct {
	RID storage.RID
	Row storage.Row
}

func NewDeleteExecutor(manager *catalog.Manager) *DeleteExecutor {
	return &DeleteExecutor{Catalog: manager}
}

// Execute deletes every row in the target table.
//
// WHERE support can layer on top of this later.
func (e *DeleteExecutor) Execute(tx *storage.Tx, stmt DeleteStatement) (Result, error) {
	if e.Catalog == nil {
		e.Catalog = catalog.NewManager()
	}
	if err := e.Catalog.Bootstrap(tx); err != nil {
		return Result{}, err
	}

	table, err := e.Catalog.GetTable(tx, stmt.TableName)
	if err != nil {
		return Result{}, err
	}

	rowsToDelete := make([]rowToDelete, 0)
	if err := tx.ForEach(table.TableBucket, func(key, payload []byte) error {
		row, err := storage.DecodeRow(payload)
		if err != nil {
			return err
		}

		rowsToDelete = append(rowsToDelete, rowToDelete{
			RID: storage.DecodeRID(key),
			Row: row,
		})
		return nil
	}); err != nil {
		return Result{}, err
	}

	if err := e.ensureDeleteAllowed(tx, table, rowsToDelete); err != nil {
		return Result{}, err
	}
	if err := e.removeIndexEntries(tx, table, rowsToDelete); err != nil {
		return Result{}, err
	}

	for _, row := range rowsToDelete {
		if err := tx.Delete(table.TableBucket, storage.EncodeRID(row.RID)); err != nil {
			return Result{}, err
		}
	}

	return Result{
		Message:      fmt.Sprintf("%d rows deleted", len(rowsToDelete)),
		AffectedRows: len(rowsToDelete),
	}, nil
}

func (e *DeleteExecutor) removeIndexEntries(tx *storage.Tx, table *catalog.TableMetadata, rows []rowToDelete) error {
	indexes, err := e.Catalog.ListIndexesByTable(tx, table.Name)
	if err != nil {
		return err
	}

	for _, row := range rows {
		for _, index := range indexes {
			// Find which column in the row feeds this index.
			columnOrdinal, err := catalog.ColumnOrdinal(table, index.ColumnName)
			if err != nil {
				return err
			}

			if columnOrdinal >= len(row.Row) {
				return shared.NewError(shared.ErrInvalidDefinition, "delete: row is missing indexed column %q on table %q", index.ColumnName, table.Name)
			}

			// Compute the index key for the value being removed.
			// e.g., indexKey = 20 for an index on column "age" when deleting a row with age=20.
			indexKey, err := storage.EncodeIndexKey(row.Row[columnOrdinal])
			if err != nil {
				return err
			}

			// e.g., existingRIDs = [RID(1), RID(2), RID(3)] for index key 20 if three rows have age=20, and we're deleting the row with RID(2).
			existingData, err := tx.Get(index.IndexBucket, indexKey)
			if err != nil {
				return err
			}

			existingRIDs, err := storage.DecodeRIDList(existingData)
			if err != nil {
				return err
			}

			// Remove only this row's RID from the index entry.
			// e.g., filtered = [RID(1), RID(3)] after removing RID(2) from existingRIDs.
			filtered := make([]storage.RID, 0, len(existingRIDs))
			for _, existingRID := range existingRIDs {
				if existingRID != row.RID {
					filtered = append(filtered, existingRID)
				}
			}

			// Delete the key completely if no rows remain under it.
			if len(filtered) == 0 {
				if err := tx.Delete(index.IndexBucket, indexKey); err != nil {
					return err
				}
				continue
			}

			// Otherwise, write back the shortened RID list.
			payload, err := storage.EncodeRIDList(filtered)
			if err != nil {
				return err
			}
			if err := tx.Put(index.IndexBucket, indexKey, payload); err != nil {
				return err
			}
		}
	}

	return nil
}

// ensureDeleteAllowed checks if any of the rows to be deleted are still referenced by foreign keys in other tables.
func (e *DeleteExecutor) ensureDeleteAllowed(tx *storage.Tx, table *catalog.TableMetadata, rows []rowToDelete) error {
	if len(rows) == 0 {
		return nil
	}

	// Find every child-table FK that points at this parent table.
	referencingFKs, err := e.Catalog.ListReferencingForeignKeyMetadata(tx, table.Name)
	if err != nil {
		return err
	}

	for _, fk := range referencingFKs {
		// Resolve the referenced parent column and the child FK column.
		parentOrdinal, err := catalog.ColumnOrdinal(table, fk.RefColumn)
		if err != nil {
			return err
		}

		childTable, err := e.Catalog.GetTable(tx, fk.TableName)
		if err != nil {
			return err
		}
		childOrdinal, err := catalog.ColumnOrdinal(childTable, fk.ColumnName)
		if err != nil {
			return err
		}

		for _, row := range rows {
			if parentOrdinal >= len(row.Row) {
				return shared.NewError(shared.ErrInvalidDefinition, "delete: row is missing referenced column %q on table %q", fk.RefColumn, table.Name)
			}

			// Compare this parent key value against every row in the child table.
			parentValue := row.Row[parentOrdinal]
			foundReference := false
			if err := tx.ForEach(childTable.TableBucket, func(_ []byte, payload []byte) error {
				childRow, err := storage.DecodeRow(payload)
				if err != nil {
					return err
				}
				if childOrdinal < len(childRow) && childRow[childOrdinal] == parentValue {
					foundReference = true
				}
				return nil
			}); err != nil {
				return err
			}

			// RESTRICT means the delete must stop if any child row still points here.
			if foundReference {
				return shared.NewError(
					shared.ErrConstraintViolation,
					"delete: table %q is still referenced by foreign key %q on table %q",
					table.Name,
					fk.ColumnName,
					fk.TableName,
				)
			}
		}
	}

	return nil
}
