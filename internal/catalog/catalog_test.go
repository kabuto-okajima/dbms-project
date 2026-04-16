package catalog

import (
	"errors"
	"path/filepath"
	"testing"

	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

// TestCatalogTableLifecycle covers create, read, RID allocation, and drop for one table.
func TestCatalogTableLifecycle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}
		if table.Name != "students" {
			t.Fatalf("got table name %q", table.Name)
		}
		if len(table.Columns) != 2 {
			t.Fatalf("got %d columns", len(table.Columns))
		}

		// fmt.Printf("table metadata: %+v\n", table)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		rid1, err := manager.NextRID(tx, "students")
		if err != nil {
			return err
		}
		rid2, err := manager.NextRID(tx, "students")
		if err != nil {
			return err
		}
		if rid1 != 1 || rid2 != 2 {
			t.Fatalf("got rids %d and %d", rid1, rid2)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		return manager.DropTable(tx, "students")
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCreateTableValidation rejects malformed table definitions before catalog writes happen.
func TestCreateTableValidation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	tests := []struct {
		name string
		def  shared.TableDefinition
	}{
		{
			name: "empty table name",
			def: shared.TableDefinition{
				Columns: []shared.ColumnDefinition{{Name: "id", Type: shared.TypeInteger}},
			},
		},
		{
			name: "no columns",
			def:  shared.TableDefinition{Name: "students"},
		},
		{
			name: "duplicate column names",
			def: shared.TableDefinition{
				Name: "students",
				Columns: []shared.ColumnDefinition{
					{Name: "id", Type: shared.TypeInteger},
					{Name: "id", Type: shared.TypeString},
				},
			},
		},
		{
			name: "unsupported type",
			def: shared.TableDefinition{
				Name:    "students",
				Columns: []shared.ColumnDefinition{{Name: "id", Type: shared.DataType("float")}},
			},
		},
		{
			name: "missing primary key column",
			def: shared.TableDefinition{
				Name:       "students",
				Columns:    []shared.ColumnDefinition{{Name: "id", Type: shared.TypeInteger}},
				PrimaryKey: []string{"student_id"},
			},
		},
		{
			name: "composite primary key unsupported",
			def: shared.TableDefinition{
				Name: "students",
				Columns: []shared.ColumnDefinition{
					{Name: "id", Type: shared.TypeInteger},
					{Name: "dept_id", Type: shared.TypeInteger},
				},
				PrimaryKey: []string{"id", "dept_id"},
			},
		},
		{
			name: "missing foreign key source column",
			def: shared.TableDefinition{
				Name:        "students",
				Columns:     []shared.ColumnDefinition{{Name: "id", Type: shared.TypeInteger}},
				ForeignKeys: []shared.ForeignKeyDefinition{{ColumnName: "dept_id", RefTable: "departments", RefColumn: "id"}},
			},
		},
		{
			name: "missing referenced table",
			def: shared.TableDefinition{
				Name: "students",
				Columns: []shared.ColumnDefinition{
					{Name: "id", Type: shared.TypeInteger},
					{Name: "dept_id", Type: shared.TypeInteger},
				},
				ForeignKeys: []shared.ForeignKeyDefinition{{ColumnName: "dept_id", RefTable: "departments", RefColumn: "id"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Update(func(tx *storage.Tx) error {
				return manager.CreateTable(tx, tt.def)
			})
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

// TestCreateTableForeignKeyTargetValidation rejects foreign keys that point at a missing target column.
func TestCreateTableForeignKeyTargetValidation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "departments",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "dept_id", Type: shared.TypeInteger},
			},
			ForeignKeys: []shared.ForeignKeyDefinition{{ColumnName: "dept_id", RefTable: "departments", RefColumn: "dept_id"}},
		})
	})
	if err == nil {
		t.Fatal("expected referenced-column validation error, got nil")
	}
	if !errors.Is(err, shared.ErrNotFound) {
		t.Fatalf("expected not-found error, got %v", err)
	}
}

// TestCreateTableDuplicateName rejects creating the same table name twice.
func TestCreateTableDuplicateName(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()
	definition := shared.TableDefinition{
		Name: "students",
		Columns: []shared.ColumnDefinition{
			{Name: "id", Type: shared.TypeInteger},
		},
	}

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, definition)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, definition)
	})
	if err == nil {
		t.Fatal("expected duplicate-table error, got nil")
	}
	if !errors.Is(err, shared.ErrAlreadyExists) {
		t.Fatalf("expected already-exists error, got %v", err)
	}
}

// TestGetTableAfterDrop confirms a dropped table can no longer be loaded from the catalog.
func TestGetTableAfterDrop(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
			},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		return manager.DropTable(tx, "students")
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		_, err := manager.GetTable(tx, "students")
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
}

// TestValidateRowValues confirms DML runtime rows align with schema arity and types.
func TestValidateRowValues(t *testing.T) {
	table := &TableMetadata{
		Name: "students",
		Columns: []shared.ColumnDefinition{
			{Name: "id", Type: shared.TypeInteger},
			{Name: "name", Type: shared.TypeString},
		},
	}

	err := ValidateRowValues(table, []storage.Value{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Ada"),
	})
	if err != nil {
		t.Fatalf("expected valid row, got %v", err)
	}

	err = ValidateRowValues(table, []storage.Value{
		storage.NewIntegerValue(1),
	})
	if err == nil {
		t.Fatal("expected row-length error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}

	err = ValidateRowValues(table, []storage.Value{
		storage.NewStringValue("wrong"), // should be integer
		storage.NewStringValue("Ada"),
	})
	if err == nil {
		t.Fatal("expected type-mismatch error, got nil")
	}
	if !errors.Is(err, shared.ErrTypeMismatch) {
		t.Fatalf("expected type-mismatch error, got %v", err)
	}
}

// TestGetTableReturnsPKAndFKMetadata checks that GetTable reconstructs key metadata correctly.
func TestGetTableReturnsPKAndFKMetadata(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "departments",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
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
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}
		if len(table.PrimaryKey) != 1 || table.PrimaryKey[0] != "id" {
			t.Fatalf("unexpected primary key metadata: %+v", table.PrimaryKey)
		}
		if len(table.ForeignKeys) != 1 {
			t.Fatalf("unexpected foreign key count: %d", len(table.ForeignKeys))
		}
		fk := table.ForeignKeys[0]
		if fk.ColumnName != "dept_id" || fk.RefTable != "departments" || fk.RefColumn != "id" {
			t.Fatalf("unexpected foreign key metadata: %+v", fk)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestIndexMetadataHelpers checks that index metadata can be loaded and filtered by table.
func TestIndexMetadataHelpers(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	err = store.Update(func(tx *storage.Tx) error {
		if err := manager.Bootstrap(tx); err != nil {
			return err
		}
		return putJSON(tx, IndexesBucket, []byte("idx_students_name"), IndexRecord{
			IndexName:   "idx_students_name",
			TableName:   "students",
			ColumnName:  "name",
			IsUnique:    false,
			IndexBucket: indexBucketName("idx_students_name"),
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		indexMeta, err := manager.GetIndex(tx, "idx_students_name")
		if err != nil {
			return err
		}
		if indexMeta.TableName != "students" || indexMeta.ColumnName != "name" {
			t.Fatalf("unexpected index metadata: %+v", indexMeta)
		}

		indexes, err := manager.ListIndexesByTable(tx, "students")
		if err != nil {
			return err
		}
		if len(indexes) != 1 {
			t.Fatalf("expected 1 index, got %d", len(indexes))
		}
		if indexes[0].Name != "idx_students_name" {
			t.Fatalf("unexpected index list: %+v", indexes)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestGetIndexMissing confirms missing index lookups return a not-found error.
func TestGetIndexMissing(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	err = store.Update(func(tx *storage.Tx) error {
		return manager.Bootstrap(tx)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		_, err := manager.GetIndex(tx, "missing_index")
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
}

// TestCreateIndexMetadataWrite checks that CreateIndex creates the bucket and catalog metadata.
func TestCreateIndexMetadataWrite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateIndex(tx, shared.IndexDefinition{
			Name:       "idx_students_name",
			TableName:  "students",
			ColumnName: "name",
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		indexMeta, err := manager.GetIndex(tx, "idx_students_name")
		if err != nil {
			return err
		}
		if indexMeta.IndexBucket != indexBucketName("idx_students_name") {
			t.Fatalf("unexpected index bucket: %s", indexMeta.IndexBucket)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCreateIndexValidation rejects bad or duplicate index definitions.
func TestCreateIndexValidation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		def     shared.IndexDefinition
		errKind error
	}{
		{
			name:    "empty index name",
			def:     shared.IndexDefinition{TableName: "students", ColumnName: "name"},
			errKind: shared.ErrInvalidDefinition,
		},
		{
			name:    "missing table",
			def:     shared.IndexDefinition{Name: "idx_missing", TableName: "missing", ColumnName: "name"},
			errKind: shared.ErrNotFound,
		},
		{
			name:    "missing column",
			def:     shared.IndexDefinition{Name: "idx_students_age", TableName: "students", ColumnName: "age"},
			errKind: shared.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Update(func(tx *storage.Tx) error {
				return manager.CreateIndex(tx, tt.def)
			})
			if err == nil {
				t.Fatal("expected create-index error, got nil")
			}
			if !errors.Is(err, tt.errKind) {
				t.Fatalf("expected %v, got %v", tt.errKind, err)
			}
		})
	}

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateIndex(tx, shared.IndexDefinition{
			Name:       "idx_students_name",
			TableName:  "students",
			ColumnName: "name",
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateIndex(tx, shared.IndexDefinition{
			Name:       "idx_students_name_2",
			TableName:  "students",
			ColumnName: "name",
		})
	})
	if err == nil {
		t.Fatal("expected duplicate-column index error, got nil")
	}
	if !errors.Is(err, shared.ErrAlreadyExists) {
		t.Fatalf("expected already-exists error, got %v", err)
	}
}

// TestCreateIndexBuildsEntriesFromExistingRows checks that existing table rows populate the new index bucket.
func TestCreateIndexBuildsEntriesFromExistingRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	err = store.Update(func(tx *storage.Tx) error {
		if err := manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
		}); err != nil {
			return err
		}

		row1, err := storage.EncodeRow(storage.Row{storage.NewIntegerValue(1), storage.NewStringValue("Alice")})
		if err != nil {
			return err
		}
		row2, err := storage.EncodeRow(storage.Row{storage.NewIntegerValue(2), storage.NewStringValue("Alice")})
		if err != nil {
			return err
		}
		row3, err := storage.EncodeRow(storage.Row{storage.NewIntegerValue(3), storage.NewStringValue("Bob")})
		if err != nil {
			return err
		}

		if err := tx.Put(tableBucketName("students"), storage.EncodeRID(1), row1); err != nil {
			return err
		}
		if err := tx.Put(tableBucketName("students"), storage.EncodeRID(2), row2); err != nil {
			return err
		}
		if err := tx.Put(tableBucketName("students"), storage.EncodeRID(3), row3); err != nil {
			return err
		}

		return manager.CreateIndex(tx, shared.IndexDefinition{
			Name:       "idx_students_name",
			TableName:  "students",
			ColumnName: "name",
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		aliceKey, err := storage.EncodeIndexKey(storage.NewStringValue("Alice"))
		if err != nil {
			return err
		}
		bobKey, err := storage.EncodeIndexKey(storage.NewStringValue("Bob"))
		if err != nil {
			return err
		}

		aliceData, err := tx.Get(indexBucketName("idx_students_name"), aliceKey)
		if err != nil {
			return err
		}
		bobData, err := tx.Get(indexBucketName("idx_students_name"), bobKey)
		if err != nil {
			return err
		}
		aliceRIDs, err := storage.DecodeRIDList(aliceData)
		if err != nil {
			return err
		}
		bobRIDs, err := storage.DecodeRIDList(bobData)
		if err != nil {
			return err
		}
		if len(aliceRIDs) != 2 || aliceRIDs[0] != 1 || aliceRIDs[1] != 2 {
			t.Fatalf("unexpected Alice RID list: %+v", aliceRIDs)
		}
		if len(bobRIDs) != 1 || bobRIDs[0] != 3 {
			t.Fatalf("unexpected Bob RID list: %+v", bobRIDs)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestDropIndexRemovesBucketAndMetadata checks that dropping an index removes both its bucket and catalog row.
func TestDropIndexRemovesBucketAndMetadata(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	err = store.Update(func(tx *storage.Tx) error {
		if err := manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
		}); err != nil {
			return err
		}
		return manager.CreateIndex(tx, shared.IndexDefinition{
			Name:       "idx_students_name",
			TableName:  "students",
			ColumnName: "name",
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		return manager.DropIndex(tx, "idx_students_name")
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		_, err := manager.GetIndex(tx, "idx_students_name")
		if err == nil {
			t.Fatal("expected not-found error, got nil")
		}
		if !errors.Is(err, shared.ErrNotFound) {
			t.Fatalf("expected not-found error, got %v", err)
		}

		_, err = tx.Get(indexBucketName("idx_students_name"), []byte("anything"))
		if !errors.Is(err, storage.ErrBucketNotFound) {
			t.Fatalf("expected missing bucket error, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestDropTableRejectsReferencedTable confirms a referenced parent table cannot be dropped.
func TestDropTableRejectsReferencedTable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	err = store.Update(func(tx *storage.Tx) error {
		if err := manager.CreateTable(tx, shared.TableDefinition{
			Name:       "departments",
			Columns:    []shared.ColumnDefinition{{Name: "id", Type: shared.TypeInteger}},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "dept_id", Type: shared.TypeInteger},
			},
			ForeignKeys: []shared.ForeignKeyDefinition{{ColumnName: "dept_id", RefTable: "departments", RefColumn: "id"}},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		return manager.DropTable(tx, "departments")
	})
	if err == nil {
		t.Fatal("expected constraint-violation error, got nil")
	}
	if !errors.Is(err, shared.ErrConstraintViolation) {
		t.Fatalf("expected constraint-violation error, got %v", err)
	}
}

// TestDropTableRemovesIndexes confirms dropping a table also removes its index storage and metadata.
func TestDropTableRemovesIndexes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := NewManager()

	err = store.Update(func(tx *storage.Tx) error {
		if err := manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
		}); err != nil {
			return err
		}
		return manager.CreateIndex(tx, shared.IndexDefinition{
			Name:       "idx_students_name",
			TableName:  "students",
			ColumnName: "name",
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Update(func(tx *storage.Tx) error {
		return manager.DropTable(tx, "students")
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		_, err := manager.GetIndex(tx, "idx_students_name")
		if err == nil {
			t.Fatal("expected not-found error, got nil")
		}
		if !errors.Is(err, shared.ErrNotFound) {
			t.Fatalf("expected not-found error, got %v", err)
		}

		_, err = tx.Get(indexBucketName("idx_students_name"), []byte("anything"))
		if !errors.Is(err, storage.ErrBucketNotFound) {
			t.Fatalf("expected missing bucket error, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
