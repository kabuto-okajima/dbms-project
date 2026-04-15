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
