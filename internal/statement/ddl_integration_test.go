package statement

import (
	"errors"
	"path/filepath"
	"testing"

	"dbms-project/internal/catalog"
	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

// TestDDLStatementLifecycle exercises create/drop table and index through the statement layer.
func TestDDLStatementLifecycle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	createIndex := NewCreateIndexExecutor(manager)
	dropIndex := NewDropIndexExecutor(manager)
	dropTable := NewDropTableExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		result, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		})
		if err != nil {
			return err
		}
		if result.Message != "table students created" {
			t.Fatalf("unexpected create-table result: %+v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		result, err := createIndex.Execute(tx, shared.IndexDefinition{
			Name:       "idx_students_name",
			TableName:  "students",
			ColumnName: "name",
		})
		if err != nil {
			return err
		}
		if result.Message != "index idx_students_name created" {
			t.Fatalf("unexpected create-index result: %+v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		if _, err := manager.GetTable(tx, "students"); err != nil {
			return err
		}
		if _, err := manager.GetIndex(tx, "idx_students_name"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		result, err := dropIndex.Execute(tx, "idx_students_name")
		if err != nil {
			return err
		}
		if result.Message != "index idx_students_name dropped" {
			t.Fatalf("unexpected drop-index result: %+v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		result, err := dropTable.Execute(tx, "students")
		if err != nil {
			return err
		}
		if result.Message != "table students dropped" {
			t.Fatalf("unexpected drop-table result: %+v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		_, err := manager.GetTable(tx, "students")
		if err == nil {
			t.Fatal("expected not-found error for dropped table, got nil")
		}
		if !errors.Is(err, shared.ErrNotFound) {
			t.Fatalf("expected not-found error, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestDDLStatementRejectsReferencedDrop confirms FK restrictions still hold through the statement layer.
func TestDDLStatementRejectsReferencedDrop(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	dropTable := NewDropTableExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name:       "departments",
			Columns:    []shared.ColumnDefinition{{Name: "id", Type: shared.TypeInteger}},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "dept_id", Type: shared.TypeInteger},
			},
			ForeignKeys: []shared.ForeignKeyDefinition{{ColumnName: "dept_id", RefTable: "departments", RefColumn: "id"}},
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		_, err := dropTable.Execute(tx, "departments")
		if err == nil {
			t.Fatal("expected constraint-violation error, got nil")
		}
		if !errors.Is(err, shared.ErrConstraintViolation) {
			t.Fatalf("expected constraint-violation error, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestInsertStatementValidation covers INSERT validation and row persistence.
func TestInsertStatementValidation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	insert := NewInsertExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		_, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("missing table", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			_, err := insert.Execute(tx, InsertStatement{
				TableName: "missing",
				Values: []storage.Value{
					storage.NewIntegerValue(1),
				},
			})
			if err == nil {
				t.Fatal("expected not-found error, got nil")
			}
			if !errors.Is(err, shared.ErrNotFound) {
				t.Fatalf("expected not-found error, got %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("wrong value count", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			_, err := insert.Execute(tx, InsertStatement{
				TableName: "students",
				Values: []storage.Value{
					storage.NewIntegerValue(1), // only id provided, name missing
				},
			})
			if err == nil {
				t.Fatal("expected invalid-definition error, got nil")
			}
			if !errors.Is(err, shared.ErrInvalidDefinition) {
				t.Fatalf("expected invalid-definition error, got %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("wrong value type", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			_, err := insert.Execute(tx, InsertStatement{
				TableName: "students",
				Values: []storage.Value{
					storage.NewStringValue("wrong"), // id should be integer, not string
					storage.NewStringValue("Ada"),
				},
			})
			if err == nil {
				t.Fatal("expected type-mismatch error, got nil")
			}
			if !errors.Is(err, shared.ErrTypeMismatch) {
				t.Fatalf("expected type-mismatch error, got %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("valid inserts persist rows with distinct rids", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			result, err := insert.Execute(tx, InsertStatement{
				TableName: "students",
				Values: []storage.Value{
					storage.NewIntegerValue(1),
					storage.NewStringValue("Ada"),
				},
			})
			if err != nil {
				return err
			}
			if result.Message != "1 row inserted" || result.AffectedRows != 1 {
				t.Fatalf("unexpected first insert result: %+v", result)
			}

			result, err = insert.Execute(tx, InsertStatement{
				TableName: "students",
				Values: []storage.Value{
					storage.NewIntegerValue(2),
					storage.NewStringValue("Grace"),
				},
			})
			if err != nil {
				return err
			}
			if result.Message != "1 row inserted" || result.AffectedRows != 1 {
				t.Fatalf("unexpected second insert result: %+v", result)
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		err = store.View(func(tx *storage.Tx) error {
			table, err := manager.GetTable(tx, "students")
			if err != nil {
				return err
			}

			rowsByRID := map[storage.RID]storage.Row{}
			if err := tx.ForEach(table.TableBucket, func(key, value []byte) error {
				row, err := storage.DecodeRow(value)
				if err != nil {
					return err
				}
				rowsByRID[storage.DecodeRID(key)] = row
				return nil
			}); err != nil {
				return err
			}

			if len(rowsByRID) != 2 {
				t.Fatalf("expected 2 persisted rows, found %d", len(rowsByRID))
			}

			row1, ok := rowsByRID[1]
			if !ok {
				t.Fatal("expected row with RID 1")
			}
			if len(row1) != 2 || row1[0] != storage.NewIntegerValue(1) || row1[1] != storage.NewStringValue("Ada") {
				t.Fatalf("unexpected row for RID 1: %+v", row1)
			}

			row2, ok := rowsByRID[2]
			if !ok {
				t.Fatal("expected row with RID 2")
			}
			if len(row2) != 2 || row2[0] != storage.NewIntegerValue(2) || row2[1] != storage.NewStringValue("Grace") {
				t.Fatalf("unexpected row for RID 2: %+v", row2)
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("column list is reordered into schema order", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			result, err := insert.Execute(tx, InsertStatement{
				TableName:   "students",
				ColumnNames: []string{"name", "id"},
				Values: []storage.Value{
					storage.NewStringValue("Linus"),
					storage.NewIntegerValue(3),
				},
			})
			if err != nil {
				return err
			}
			if result.Message != "1 row inserted" || result.AffectedRows != 1 {
				t.Fatalf("unexpected insert result: %+v", result)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		err = store.View(func(tx *storage.Tx) error {
			table, err := manager.GetTable(tx, "students")
			if err != nil {
				return err
			}

			data, err := tx.Get(table.TableBucket, storage.EncodeRID(3))
			if err != nil {
				return err
			}
			row, err := storage.DecodeRow(data)
			if err != nil {
				return err
			}
			if len(row) != 2 || row[0] != storage.NewIntegerValue(3) || row[1] != storage.NewStringValue("Linus") {
				t.Fatalf("unexpected reordered row: %+v", row)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("column list rejects unknown column", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			_, err := insert.Execute(tx, InsertStatement{
				TableName:   "students",
				ColumnNames: []string{"nickname", "id"},
				Values: []storage.Value{
					storage.NewStringValue("Ada"),
					storage.NewIntegerValue(4),
				},
			})
			if err == nil {
				t.Fatal("expected not-found error, got nil")
			}
			if !errors.Is(err, shared.ErrNotFound) {
				t.Fatalf("expected not-found error, got %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("column list rejects duplicate column", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			_, err := insert.Execute(tx, InsertStatement{
				TableName:   "students",
				ColumnNames: []string{"id", "id"},
				Values: []storage.Value{
					storage.NewIntegerValue(4),
					storage.NewIntegerValue(5),
				},
			})
			if err == nil {
				t.Fatal("expected invalid-definition error, got nil")
			}
			if !errors.Is(err, shared.ErrInvalidDefinition) {
				t.Fatalf("expected invalid-definition error, got %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("column list rejects partial insert for now", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			_, err := insert.Execute(tx, InsertStatement{
				TableName:   "students",
				ColumnNames: []string{"name"},
				Values: []storage.Value{
					storage.NewStringValue("OnlyName"),
				},
			})
			if err == nil {
				t.Fatal("expected invalid-definition error, got nil")
			}
			if !errors.Is(err, shared.ErrInvalidDefinition) {
				t.Fatalf("expected invalid-definition error, got %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

// TestInsertMaintainsIndexes confirms INSERT keeps secondary index buckets in sync.
func TestInsertMaintainsIndexes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	createIndex := NewCreateIndexExecutor(manager)
	insert := NewInsertExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
		}); err != nil {
			return err
		}
		_, err := createIndex.Execute(tx, shared.IndexDefinition{
			Name:       "idx_students_name",
			TableName:  "students",
			ColumnName: "name",
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewStringValue("Ada"),
			},
		}); err != nil {
			return err
		}
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(2),
				storage.NewStringValue("Ada"),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		indexMeta, err := manager.GetIndex(tx, "idx_students_name")
		if err != nil {
			return err
		}

		adaKey, err := storage.EncodeIndexKey(storage.NewStringValue("Ada"))
		if err != nil {
			return err
		}

		data, err := tx.Get(indexMeta.IndexBucket, adaKey)
		if err != nil {
			return err
		}

		rids, err := storage.DecodeRIDList(data)
		if err != nil {
			return err
		}
		if len(rids) != 2 || rids[0] != 1 || rids[1] != 2 {
			t.Fatalf("unexpected RID list for Ada: %+v", rids)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestInsertRejectsDuplicateUniqueIndex confirms unique secondary indexes block duplicate INSERT keys.
func TestInsertRejectsDuplicateUniqueIndex(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	createIndex := NewCreateIndexExecutor(manager)
	insert := NewInsertExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "email", Type: shared.TypeString},
			},
		}); err != nil {
			return err
		}
		_, err := createIndex.Execute(tx, shared.IndexDefinition{
			Name:       "idx_students_email",
			TableName:  "students",
			ColumnName: "email",
			IsUnique:   true,
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewStringValue("ada@example.com"),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(2),
				storage.NewStringValue("ada@example.com"),
			},
		})
		if err == nil {
			t.Fatal("expected constraint-violation error, got nil")
		}
		if !errors.Is(err, shared.ErrConstraintViolation) {
			t.Fatalf("expected constraint-violation error, got %v", err)
		}
		return err
	})
	if !errors.Is(err, shared.ErrConstraintViolation) {
		t.Fatalf("expected transaction to return constraint-violation error, got %v", err)
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rowCount := 0
		if err := tx.ForEach(table.TableBucket, func(_, _ []byte) error {
			rowCount++
			return nil
		}); err != nil {
			return err
		}
		if rowCount != 1 {
			t.Fatalf("expected 1 persisted row after duplicate rejection, found %d", rowCount)
		}

		indexMeta, err := manager.GetIndex(tx, "idx_students_email")
		if err != nil {
			return err
		}
		emailKey, err := storage.EncodeIndexKey(storage.NewStringValue("ada@example.com"))
		if err != nil {
			return err
		}
		data, err := tx.Get(indexMeta.IndexBucket, emailKey)
		if err != nil {
			return err
		}

		rids, err := storage.DecodeRIDList(data)
		if err != nil {
			return err
		}
		if len(rids) != 1 || rids[0] != 1 {
			t.Fatalf("unexpected RID list after duplicate rejection: %+v", rids)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestInsertRejectsDuplicatePrimaryKey confirms declared primary keys stay unique on INSERT.
func TestInsertRejectsDuplicatePrimaryKey(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	insert := NewInsertExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		_, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewStringValue("Ada"),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewStringValue("Grace"),
			},
		})
		if err == nil {
			t.Fatal("expected constraint-violation error, got nil")
		}
		if !errors.Is(err, shared.ErrConstraintViolation) {
			t.Fatalf("expected constraint-violation error, got %v", err)
		}
		return err
	})
	if !errors.Is(err, shared.ErrConstraintViolation) {
		t.Fatalf("expected transaction to return constraint-violation error, got %v", err)
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rowsByRID := map[storage.RID]storage.Row{}
		if err := tx.ForEach(table.TableBucket, func(key, value []byte) error {
			row, err := storage.DecodeRow(value)
			if err != nil {
				return err
			}
			rowsByRID[storage.DecodeRID(key)] = row
			return nil
		}); err != nil {
			return err
		}

		if len(rowsByRID) != 1 {
			t.Fatalf("expected 1 persisted row after duplicate PK rejection, found %d", len(rowsByRID))
		}

		row1, ok := rowsByRID[1]
		if !ok {
			t.Fatal("expected original row with RID 1")
		}
		if len(row1) != 2 || row1[0] != storage.NewIntegerValue(1) || row1[1] != storage.NewStringValue("Ada") {
			t.Fatalf("unexpected row after duplicate PK rejection: %+v", row1)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestInsertChecksForeignKeyExistence confirms child rows must reference an existing parent row.
func TestInsertChecksForeignKeyExistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	insert := NewInsertExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "departments",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		_, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "dept_id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
			ForeignKeys: []shared.ForeignKeyDefinition{
				{ColumnName: "dept_id", RefTable: "departments", RefColumn: "id"},
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "departments",
			Values: []storage.Value{
				storage.NewIntegerValue(10),
				storage.NewStringValue("Engineering"),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("matching parent row allows insert", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			result, err := insert.Execute(tx, InsertStatement{
				TableName: "students",
				Values: []storage.Value{
					storage.NewIntegerValue(1),
					storage.NewIntegerValue(10),
					storage.NewStringValue("Ada"),
				},
			})
			if err != nil {
				return err
			}
			if result.Message != "1 row inserted" || result.AffectedRows != 1 {
				t.Fatalf("unexpected insert result: %+v", result)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("missing parent row rejects insert and leaves no extra row", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			_, err := insert.Execute(tx, InsertStatement{
				TableName: "students",
				Values: []storage.Value{
					storage.NewIntegerValue(2),
					storage.NewIntegerValue(999),
					storage.NewStringValue("Grace"),
				},
			})
			if err == nil {
				t.Fatal("expected constraint-violation error, got nil")
			}
			if !errors.Is(err, shared.ErrConstraintViolation) {
				t.Fatalf("expected constraint-violation error, got %v", err)
			}
			return err
		})
		if !errors.Is(err, shared.ErrConstraintViolation) {
			t.Fatalf("expected transaction to return constraint-violation error, got %v", err)
		}

		err = store.View(func(tx *storage.Tx) error {
			table, err := manager.GetTable(tx, "students")
			if err != nil {
				return err
			}

			rowCount := 0
			if err := tx.ForEach(table.TableBucket, func(_, _ []byte) error {
				rowCount++
				return nil
			}); err != nil {
				return err
			}
			if rowCount != 1 {
				t.Fatalf("expected 1 persisted student row after FK rejection, found %d", rowCount)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

// TestDeleteStatementFullTable covers the first DELETE step: remove all rows without WHERE.
func TestDeleteStatementFullTable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	createIndex := NewCreateIndexExecutor(manager)
	insert := NewInsertExecutor(manager)
	deleteExec := NewDeleteExecutor(manager)

	t.Run("missing table", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			_, err := deleteExec.Execute(tx, DeleteStatement{TableName: "missing"})
			if err == nil {
				t.Fatal("expected not-found error, got nil")
			}
			if !errors.Is(err, shared.ErrNotFound) {
				t.Fatalf("expected not-found error, got %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		_, err := createIndex.Execute(tx, shared.IndexDefinition{
			Name:       "idx_students_name",
			TableName:  "students",
			ColumnName: "name",
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("empty table delete succeeds", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			result, err := deleteExec.Execute(tx, DeleteStatement{TableName: "students"})
			if err != nil {
				return err
			}
			if result.AffectedRows != 0 || result.Message != "0 rows deleted" {
				t.Fatalf("unexpected delete result: %+v", result)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewStringValue("Ada"),
			},
		}); err != nil {
			return err
		}
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(2),
				storage.NewStringValue("Grace"),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("full-table delete removes rows and clears index entries", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			result, err := deleteExec.Execute(tx, DeleteStatement{TableName: "students"})
			if err != nil {
				return err
			}
			if result.AffectedRows != 2 || result.Message != "2 rows deleted" {
				t.Fatalf("unexpected delete result: %+v", result)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		err = store.View(func(tx *storage.Tx) error {
			table, err := manager.GetTable(tx, "students")
			if err != nil {
				return err
			}

			rowCount := 0
			if err := tx.ForEach(table.TableBucket, func(_, _ []byte) error {
				rowCount++
				return nil
			}); err != nil {
				return err
			}
			if rowCount != 0 {
				t.Fatalf("expected no rows after delete, found %d", rowCount)
			}

			indexMeta, err := manager.GetIndex(tx, "idx_students_name")
			if err != nil {
				return err
			}

			indexEntryCount := 0
			if err := tx.ForEach(indexMeta.IndexBucket, func(_, _ []byte) error {
				indexEntryCount++
				return nil
			}); err != nil {
				return err
			}
			if indexEntryCount != 0 {
				t.Fatalf("expected no index entries after delete, found %d", indexEntryCount)
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

// TestDeleteStatementRestrictRejectsReferencedParent confirms FK RESTRICT blocks parent deletion.
func TestDeleteStatementRestrictRejectsReferencedParent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	insert := NewInsertExecutor(manager)
	deleteExec := NewDeleteExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "departments",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		_, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "dept_id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
			ForeignKeys: []shared.ForeignKeyDefinition{
				{ColumnName: "dept_id", RefTable: "departments", RefColumn: "id"},
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := insert.Execute(tx, InsertStatement{
			TableName: "departments",
			Values: []storage.Value{
				storage.NewIntegerValue(10),
				storage.NewStringValue("Engineering"),
			},
		}); err != nil {
			return err
		}
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewIntegerValue(10),
				storage.NewStringValue("Ada"),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		_, err := deleteExec.Execute(tx, DeleteStatement{TableName: "departments"})
		if err == nil {
			t.Fatal("expected constraint-violation error, got nil")
		}
		if !errors.Is(err, shared.ErrConstraintViolation) {
			t.Fatalf("expected constraint-violation error, got %v", err)
		}
		return err
	})
	if !errors.Is(err, shared.ErrConstraintViolation) {
		t.Fatalf("expected transaction to return constraint-violation error, got %v", err)
	}

	err = store.View(func(tx *storage.Tx) error {
		departments, err := manager.GetTable(tx, "departments")
		if err != nil {
			return err
		}

		rowCount := 0
		if err := tx.ForEach(departments.TableBucket, func(_, _ []byte) error {
			rowCount++
			return nil
		}); err != nil {
			return err
		}
		if rowCount != 1 {
			t.Fatalf("expected parent row to remain after RESTRICT failure, found %d rows", rowCount)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestUpdateStatementFullTable covers the first UPDATE step: one-column full-table rewrite without WHERE.
func TestUpdateStatementFullTable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	insert := NewInsertExecutor(manager)
	updateExec := NewUpdateExecutor(manager)

	t.Run("missing table", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			_, err := updateExec.Execute(tx, UpdateStatement{
				TableName:  "missing",
				ColumnName: "name",
				Value:      storage.NewStringValue("Ada"),
			})
			if err == nil {
				t.Fatal("expected not-found error, got nil")
			}
			if !errors.Is(err, shared.ErrNotFound) {
				t.Fatalf("expected not-found error, got %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	err = store.Update(func(tx *storage.Tx) error {
		_, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("missing column", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			_, err := updateExec.Execute(tx, UpdateStatement{
				TableName:  "students",
				ColumnName: "nickname",
				Value:      storage.NewStringValue("Ada"),
			})
			if err == nil {
				t.Fatal("expected not-found error, got nil")
			}
			if !errors.Is(err, shared.ErrNotFound) {
				t.Fatalf("expected not-found error, got %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewStringValue("Ada"),
			},
		}); err != nil {
			return err
		}
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(2),
				storage.NewStringValue("Grace"),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("wrong value type", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			_, err := updateExec.Execute(tx, UpdateStatement{
				TableName:  "students",
				ColumnName: "id",
				Value:      storage.NewStringValue("wrong"),
			})
			if err == nil {
				t.Fatal("expected type-mismatch error, got nil")
			}
			if !errors.Is(err, shared.ErrTypeMismatch) {
				t.Fatalf("expected type-mismatch error, got %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("successful full-table update rewrites all rows", func(t *testing.T) {
		err := store.Update(func(tx *storage.Tx) error {
			result, err := updateExec.Execute(tx, UpdateStatement{
				TableName:  "students",
				ColumnName: "name",
				Value:      storage.NewStringValue("Updated"),
			})
			if err != nil {
				return err
			}
			if result.AffectedRows != 2 || result.Message != "2 rows updated" {
				t.Fatalf("unexpected update result: %+v", result)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		err = store.View(func(tx *storage.Tx) error {
			table, err := manager.GetTable(tx, "students")
			if err != nil {
				return err
			}

			rowsByRID := map[storage.RID]storage.Row{}
			if err := tx.ForEach(table.TableBucket, func(key, value []byte) error {
				row, err := storage.DecodeRow(value)
				if err != nil {
					return err
				}

				// fmt.Printf("RID %d: %+v\n", storage.DecodeRID(key), row)

				rowsByRID[storage.DecodeRID(key)] = row
				return nil
			}); err != nil {
				return err
			}

			if len(rowsByRID) != 2 {
				t.Fatalf("expected 2 rows after update, found %d", len(rowsByRID))
			}
			if row := rowsByRID[1]; len(row) != 2 || row[0] != storage.NewIntegerValue(1) || row[1] != storage.NewStringValue("Updated") {
				t.Fatalf("unexpected row for RID 1 after update: %+v", row)
			}
			if row := rowsByRID[2]; len(row) != 2 || row[0] != storage.NewIntegerValue(2) || row[1] != storage.NewStringValue("Updated") {
				t.Fatalf("unexpected row for RID 2 after update: %+v", row)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

// TestUpdateMaintainsIndexes confirms UPDATE moves index entries to the new key.
func TestUpdateMaintainsIndexes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	createIndex := NewCreateIndexExecutor(manager)
	insert := NewInsertExecutor(manager)
	updateExec := NewUpdateExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
		}); err != nil {
			return err
		}
		_, err := createIndex.Execute(tx, shared.IndexDefinition{
			Name:       "idx_students_name",
			TableName:  "students",
			ColumnName: "name",
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewStringValue("Ada"),
			},
		}); err != nil {
			return err
		}
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(2),
				storage.NewStringValue("Grace"),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		_, err := updateExec.Execute(tx, UpdateStatement{
			TableName:  "students",
			ColumnName: "name",
			Value:      storage.NewStringValue("Updated"),
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		indexMeta, err := manager.GetIndex(tx, "idx_students_name")
		if err != nil {
			return err
		}

		oldAdaKey, err := storage.EncodeIndexKey(storage.NewStringValue("Ada"))
		if err != nil {
			return err
		}
		oldGraceKey, err := storage.EncodeIndexKey(storage.NewStringValue("Grace"))
		if err != nil {
			return err
		}
		newKey, err := storage.EncodeIndexKey(storage.NewStringValue("Updated"))
		if err != nil {
			return err
		}

		oldAdaData, err := tx.Get(indexMeta.IndexBucket, oldAdaKey)
		if err != nil {
			return err
		}
		oldGraceData, err := tx.Get(indexMeta.IndexBucket, oldGraceKey)
		if err != nil {
			return err
		}
		if oldAdaData != nil || oldGraceData != nil {
			t.Fatalf("expected old index keys to be removed, got Ada=%v Grace=%v", oldAdaData, oldGraceData)
		}

		newData, err := tx.Get(indexMeta.IndexBucket, newKey)
		if err != nil {
			return err
		}
		rids, err := storage.DecodeRIDList(newData)
		if err != nil {
			return err
		}
		if len(rids) != 2 || rids[0] != 1 || rids[1] != 2 {
			t.Fatalf("unexpected RID list for updated key: %+v", rids)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestUpdatePredicateUpdatesOnlyMatchingRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	createIndex := NewCreateIndexExecutor(manager)
	insert := NewInsertExecutor(manager)
	updateExec := NewUpdateExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		_, err := createIndex.Execute(tx, shared.IndexDefinition{
			Name:       "idx_students_name",
			TableName:  "students",
			ColumnName: "name",
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		for _, row := range []storage.Row{
			{storage.NewIntegerValue(1), storage.NewStringValue("Ada")},
			{storage.NewIntegerValue(2), storage.NewStringValue("Grace")},
			{storage.NewIntegerValue(3), storage.NewStringValue("Ada")},
		} {
			if _, err := insert.Execute(tx, InsertStatement{
				TableName: "students",
				Values:    row,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		result, err := updateExec.Execute(tx, UpdateStatement{
			TableName:  "students",
			ColumnName: "name",
			Value:      storage.NewStringValue("Updated"),
			Matcher: func(row storage.Row) (bool, error) {
				return len(row) >= 2 && row[1] == storage.NewStringValue("Ada"), nil
			},
		})
		if err != nil {
			return err
		}
		if result.AffectedRows != 2 || result.Message != "2 rows updated" {
			t.Fatalf("unexpected update result: %+v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rowsByRID := map[storage.RID]storage.Row{}
		if err := tx.ForEach(table.TableBucket, func(key, value []byte) error {
			row, err := storage.DecodeRow(value)
			if err != nil {
				return err
			}
			rowsByRID[storage.DecodeRID(key)] = row
			return nil
		}); err != nil {
			return err
		}

		if row := rowsByRID[1]; len(row) != 2 || row[1] != storage.NewStringValue("Updated") {
			t.Fatalf("unexpected row 1 after selective update: %+v", row)
		}
		if row := rowsByRID[2]; len(row) != 2 || row[1] != storage.NewStringValue("Grace") {
			t.Fatalf("unexpected row 2 after selective update: %+v", row)
		}
		if row := rowsByRID[3]; len(row) != 2 || row[1] != storage.NewStringValue("Updated") {
			t.Fatalf("unexpected row 3 after selective update: %+v", row)
		}

		indexMeta, err := manager.GetIndex(tx, "idx_students_name")
		if err != nil {
			return err
		}
		adaKey, err := storage.EncodeIndexKey(storage.NewStringValue("Ada"))
		if err != nil {
			return err
		}
		adaData, err := tx.Get(indexMeta.IndexBucket, adaKey)
		if err != nil {
			return err
		}
		if adaData != nil {
			t.Fatalf("expected Ada index entry to be gone, found %v", adaData)
		}

		graceKey, err := storage.EncodeIndexKey(storage.NewStringValue("Grace"))
		if err != nil {
			return err
		}
		graceData, err := tx.Get(indexMeta.IndexBucket, graceKey)
		if err != nil {
			return err
		}
		graceRIDs, err := storage.DecodeRIDList(graceData)
		if err != nil {
			return err
		}
		if len(graceRIDs) != 1 || graceRIDs[0] != 2 {
			t.Fatalf("unexpected Grace RID list after selective update: %+v", graceRIDs)
		}

		updatedKey, err := storage.EncodeIndexKey(storage.NewStringValue("Updated"))
		if err != nil {
			return err
		}
		updatedData, err := tx.Get(indexMeta.IndexBucket, updatedKey)
		if err != nil {
			return err
		}
		updatedRIDs, err := storage.DecodeRIDList(updatedData)
		if err != nil {
			return err
		}
		if len(updatedRIDs) != 2 || updatedRIDs[0] != 1 || updatedRIDs[1] != 3 {
			t.Fatalf("unexpected Updated RID list after selective update: %+v", updatedRIDs)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestUpdatePredicateRestrictOnlyChecksMatchedRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	insert := NewInsertExecutor(manager)
	updateExec := NewUpdateExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "departments",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		_, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "dept_id", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
			ForeignKeys: []shared.ForeignKeyDefinition{
				{ColumnName: "dept_id", RefTable: "departments", RefColumn: "id"},
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		for _, row := range []storage.Row{
			{storage.NewIntegerValue(10), storage.NewStringValue("Engineering")},
			{storage.NewIntegerValue(20), storage.NewStringValue("Math")},
			{storage.NewIntegerValue(30), storage.NewStringValue("Physics")},
		} {
			if _, err := insert.Execute(tx, InsertStatement{
				TableName: "departments",
				Values:    row,
			}); err != nil {
				return err
			}
		}
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewIntegerValue(10),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		result, err := updateExec.Execute(tx, UpdateStatement{
			TableName:  "departments",
			ColumnName: "id",
			Value:      storage.NewIntegerValue(40),
			Matcher: func(row storage.Row) (bool, error) {
				return len(row) >= 1 && row[0] == storage.NewIntegerValue(20), nil
			},
		})
		if err != nil {
			return err
		}
		if result.AffectedRows != 1 || result.Message != "1 rows updated" {
			t.Fatalf("unexpected update result: %+v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "departments")
		if err != nil {
			return err
		}
		ids := make([]storage.Value, 0)
		if err := tx.ForEach(table.TableBucket, func(_ []byte, payload []byte) error {
			row, err := storage.DecodeRow(payload)
			if err != nil {
				return err
			}
			ids = append(ids, row[0])
			return nil
		}); err != nil {
			return err
		}
		if len(ids) != 3 {
			t.Fatalf("expected 3 department rows after selective update, found %d", len(ids))
		}
		found10, found30, found40 := false, false, false
		for _, id := range ids {
			switch id {
			case storage.NewIntegerValue(10):
				found10 = true
			case storage.NewIntegerValue(30):
				found30 = true
			case storage.NewIntegerValue(40):
				found40 = true
			}
		}
		if !found10 || !found30 || !found40 {
			t.Fatalf("unexpected department ids after selective update: %+v", ids)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestUpdateRejectsDuplicateUniqueIndex confirms unique secondary indexes still reject duplicate UPDATE keys.
func TestUpdateRejectsDuplicateUniqueIndex(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	createIndex := NewCreateIndexExecutor(manager)
	insert := NewInsertExecutor(manager)
	updateExec := NewUpdateExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "email", Type: shared.TypeString},
			},
		}); err != nil {
			return err
		}
		_, err := createIndex.Execute(tx, shared.IndexDefinition{
			Name:       "idx_students_email",
			TableName:  "students",
			ColumnName: "email",
			IsUnique:   true,
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewStringValue("ada@example.com"),
			},
		}); err != nil {
			return err
		}
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(2),
				storage.NewStringValue("grace@example.com"),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		_, err := updateExec.Execute(tx, UpdateStatement{
			TableName:  "students",
			ColumnName: "email",
			Value:      storage.NewStringValue("ada@example.com"),
		})
		if err == nil {
			t.Fatal("expected constraint-violation error, got nil")
		}
		if !errors.Is(err, shared.ErrConstraintViolation) {
			t.Fatalf("expected constraint-violation error, got %v", err)
		}
		return err
	})
	if !errors.Is(err, shared.ErrConstraintViolation) {
		t.Fatalf("expected transaction to return constraint-violation error, got %v", err)
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rowsByRID := map[storage.RID]storage.Row{}
		if err := tx.ForEach(table.TableBucket, func(key, value []byte) error {
			row, err := storage.DecodeRow(value)
			if err != nil {
				return err
			}
			rowsByRID[storage.DecodeRID(key)] = row
			return nil
		}); err != nil {
			return err
		}

		if row := rowsByRID[1]; len(row) != 2 || row[1] != storage.NewStringValue("ada@example.com") {
			t.Fatalf("unexpected row 1 after rejected update: %+v", row)
		}
		if row := rowsByRID[2]; len(row) != 2 || row[1] != storage.NewStringValue("grace@example.com") {
			t.Fatalf("unexpected row 2 after rejected update: %+v", row)
		}

		indexMeta, err := manager.GetIndex(tx, "idx_students_email")
		if err != nil {
			return err
		}
		adaKey, err := storage.EncodeIndexKey(storage.NewStringValue("ada@example.com"))
		if err != nil {
			return err
		}
		graceKey, err := storage.EncodeIndexKey(storage.NewStringValue("grace@example.com"))
		if err != nil {
			return err
		}

		adaData, err := tx.Get(indexMeta.IndexBucket, adaKey)
		if err != nil {
			return err
		}
		graceData, err := tx.Get(indexMeta.IndexBucket, graceKey)
		if err != nil {
			return err
		}
		adaRIDs, err := storage.DecodeRIDList(adaData)
		if err != nil {
			return err
		}
		graceRIDs, err := storage.DecodeRIDList(graceData)
		if err != nil {
			return err
		}
		if len(adaRIDs) != 1 || adaRIDs[0] != 1 {
			t.Fatalf("unexpected ada RID list after rejected update: %+v", adaRIDs)
		}
		if len(graceRIDs) != 1 || graceRIDs[0] != 2 {
			t.Fatalf("unexpected grace RID list after rejected update: %+v", graceRIDs)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestUpdateRejectsDuplicatePrimaryKey confirms UPDATE preserves PK uniqueness when the PK column changes.
func TestUpdateRejectsDuplicatePrimaryKey(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	insert := NewInsertExecutor(manager)
	updateExec := NewUpdateExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		_, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewStringValue("Ada"),
			},
		}); err != nil {
			return err
		}
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(2),
				storage.NewStringValue("Grace"),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		_, err := updateExec.Execute(tx, UpdateStatement{
			TableName:  "students",
			ColumnName: "id",
			Value:      storage.NewIntegerValue(1),
		})
		if err == nil {
			t.Fatal("expected constraint-violation error, got nil")
		}
		if !errors.Is(err, shared.ErrConstraintViolation) {
			t.Fatalf("expected constraint-violation error, got %v", err)
		}
		return err
	})
	if !errors.Is(err, shared.ErrConstraintViolation) {
		t.Fatalf("expected transaction to return constraint-violation error, got %v", err)
	}
}

func TestDeleteStatementPredicateDeletesOnlyMatchingRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	createIndex := NewCreateIndexExecutor(manager)
	insert := NewInsertExecutor(manager)
	deleteExec := NewDeleteExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		_, err := createIndex.Execute(tx, shared.IndexDefinition{
			Name:       "idx_students_name",
			TableName:  "students",
			ColumnName: "name",
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		rows := []storage.Row{
			{storage.NewIntegerValue(1), storage.NewStringValue("Ada")},
			{storage.NewIntegerValue(2), storage.NewStringValue("Grace")},
			{storage.NewIntegerValue(3), storage.NewStringValue("Ada")},
		}
		for _, row := range rows {
			if _, err := insert.Execute(tx, InsertStatement{
				TableName: "students",
				Values:    row,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		result, err := deleteExec.Execute(tx, DeleteStatement{
			// "DELETE FROM students WHERE name = 'Ada'"
			TableName: "students",
			Matcher: func(row storage.Row) (bool, error) {
				return len(row) >= 2 && row[1] == storage.NewStringValue("Ada"), nil
			},
		})
		if err != nil {
			return err
		}
		if result.AffectedRows != 2 || result.Message != "2 rows deleted" {
			t.Fatalf("unexpected delete result: %+v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		rows := make([]storage.Row, 0)
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}
		if err := tx.ForEach(table.TableBucket, func(_ []byte, payload []byte) error {
			row, err := storage.DecodeRow(payload)
			if err != nil {
				return err
			}
			rows = append(rows, row)
			return nil
		}); err != nil {
			return err
		}
		if len(rows) != 1 {
			t.Fatalf("expected 1 remaining row, found %d", len(rows))
		}
		if got := rows[0]; len(got) != 2 || got[0] != storage.NewIntegerValue(2) || got[1] != storage.NewStringValue("Grace") {
			t.Fatalf("unexpected remaining row: %+v", got)
		}

		indexMeta, err := manager.GetIndex(tx, "idx_students_name")
		if err != nil {
			return err
		}

		adaKey, err := storage.EncodeIndexKey(storage.NewStringValue("Ada"))
		if err != nil {
			return err
		}
		adaData, err := tx.Get(indexMeta.IndexBucket, adaKey)
		if err != nil {
			return err
		}
		if adaData != nil {
			t.Fatalf("expected Ada index entry to be gone, found %v", adaData)
		}

		graceKey, err := storage.EncodeIndexKey(storage.NewStringValue("Grace"))
		if err != nil {
			return err
		}
		graceData, err := tx.Get(indexMeta.IndexBucket, graceKey)
		if err != nil {
			return err
		}
		graceRIDs, err := storage.DecodeRIDList(graceData)
		if err != nil {
			return err
		}
		if len(graceRIDs) != 1 {
			t.Fatalf("expected one Grace RID, got %+v", graceRIDs)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteStatementPredicateRestrictOnlyChecksMatchedRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	insert := NewInsertExecutor(manager)
	deleteExec := NewDeleteExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "departments",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		_, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "dept_id", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
			ForeignKeys: []shared.ForeignKeyDefinition{
				{ColumnName: "dept_id", RefTable: "departments", RefColumn: "id"},
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		for _, row := range []storage.Row{
			{storage.NewIntegerValue(10), storage.NewStringValue("Engineering")},
			{storage.NewIntegerValue(20), storage.NewStringValue("Math")},
		} {
			if _, err := insert.Execute(tx, InsertStatement{
				TableName: "departments",
				Values:    row,
			}); err != nil {
				return err
			}
		}
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewIntegerValue(10),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		result, err := deleteExec.Execute(tx, DeleteStatement{
			TableName: "departments",
			Matcher: func(row storage.Row) (bool, error) {
				return len(row) >= 1 && row[0] == storage.NewIntegerValue(20), nil
			},
		})
		if err != nil {
			return err
		}
		if result.AffectedRows != 1 || result.Message != "1 rows deleted" {
			t.Fatalf("unexpected delete result: %+v", result)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "departments")
		if err != nil {
			return err
		}
		ids := make([]storage.Value, 0)
		if err := tx.ForEach(table.TableBucket, func(_ []byte, payload []byte) error {
			row, err := storage.DecodeRow(payload)
			if err != nil {
				return err
			}
			ids = append(ids, row[0])
			return nil
		}); err != nil {
			return err
		}
		if len(ids) != 1 || ids[0] != storage.NewIntegerValue(10) {
			t.Fatalf("unexpected department ids after delete: %+v", ids)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestUpdateChecksForeignKeyExistence confirms UPDATE rejects child FK values that do not exist in the parent table.
func TestUpdateChecksForeignKeyExistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	insert := NewInsertExecutor(manager)
	updateExec := NewUpdateExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "departments",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		_, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "dept_id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			ForeignKeys: []shared.ForeignKeyDefinition{
				{ColumnName: "dept_id", RefTable: "departments", RefColumn: "id"},
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := insert.Execute(tx, InsertStatement{
			TableName: "departments",
			Values: []storage.Value{
				storage.NewIntegerValue(10),
				storage.NewStringValue("Engineering"),
			},
		}); err != nil {
			return err
		}
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewIntegerValue(10),
				storage.NewStringValue("Ada"),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Attempt to update the student's dept_id to a non-existent department, which should violate the FK constraint.
	err = store.Update(func(tx *storage.Tx) error {
		_, err := updateExec.Execute(tx, UpdateStatement{
			TableName:  "students",
			ColumnName: "dept_id",
			Value:      storage.NewIntegerValue(999),
		})
		if err == nil {
			t.Fatal("expected constraint-violation error, got nil")
		}
		if !errors.Is(err, shared.ErrConstraintViolation) {
			t.Fatalf("expected constraint-violation error, got %v", err)
		}
		return err
	})
	if !errors.Is(err, shared.ErrConstraintViolation) {
		t.Fatalf("expected transaction to return constraint-violation error, got %v", err)
	}
}

// TestUpdateRestrictRejectsReferencedParentKeyChange confirms parent-key updates obey RESTRICT.
func TestUpdateRestrictRejectsReferencedParentKeyChange(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	createTable := NewCreateTableExecutor(manager)
	insert := NewInsertExecutor(manager)
	updateExec := NewUpdateExecutor(manager)

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "departments",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		_, err := createTable.Execute(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "dept_id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			ForeignKeys: []shared.ForeignKeyDefinition{
				{ColumnName: "dept_id", RefTable: "departments", RefColumn: "id"},
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		if _, err := insert.Execute(tx, InsertStatement{
			TableName: "departments",
			Values: []storage.Value{
				storage.NewIntegerValue(10),
				storage.NewStringValue("Engineering"),
			},
		}); err != nil {
			return err
		}
		_, err := insert.Execute(tx, InsertStatement{
			TableName: "students",
			Values: []storage.Value{
				storage.NewIntegerValue(1),
				storage.NewIntegerValue(10),
				storage.NewStringValue("Ada"),
			},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Attempt to update the department's id to a non-existent value, which should violate the FK constraint on students.dept_id.
	err = store.Update(func(tx *storage.Tx) error {
		_, err := updateExec.Execute(tx, UpdateStatement{
			TableName:  "departments",
			ColumnName: "id",
			Value:      storage.NewIntegerValue(20),
		})
		if err == nil {
			t.Fatal("expected constraint-violation error, got nil")
		}
		if !errors.Is(err, shared.ErrConstraintViolation) {
			t.Fatalf("expected constraint-violation error, got %v", err)
		}
		return err
	})
	if !errors.Is(err, shared.ErrConstraintViolation) {
		t.Fatalf("expected transaction to return constraint-violation error, got %v", err)
	}
}
