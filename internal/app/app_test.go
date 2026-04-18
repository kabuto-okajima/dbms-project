package app

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"dbms-project/internal/catalog"
	"dbms-project/internal/parser"
	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

// TestNewBootstrapsCatalog confirms app startup opens the DB and creates catalog buckets.
func TestNewBootstrapsCatalog(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	app, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	if app.Store == nil {
		t.Fatal("expected app store to be initialized")
	}
	if app.Catalog == nil {
		t.Fatal("expected app catalog manager to be initialized")
	}

	err = app.Store.View(func(tx *storage.Tx) error {
		for _, bucket := range []string{
			catalog.TablesBucket,
			catalog.ColumnsBucket,
			catalog.PrimaryKeysBucket,
			catalog.ForeignKeysBucket,
			catalog.IndexesBucket,
		} {
			if _, err := tx.Get(bucket, []byte("missing")); err != nil && err != storage.ErrBucketNotFound {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestResultRepresentsSelectOutput(t *testing.T) {
	result := Result{
		Kind: parser.StatementSelect,
		Columns: []ResultColumn{
			{Name: "id", Type: shared.TypeInteger},
			{Name: "name", Type: shared.TypeString},
		},
		Rows: []storage.Row{
			{
				storage.NewIntegerValue(1),
				storage.NewStringValue("Ada"),
			},
		},
		Elapsed: 5 * time.Millisecond,
	}

	if !result.IsSelect() {
		t.Fatal("expected select result")
	}
	if !result.HasRows() {
		t.Fatal("expected tabular result")
	}
	if got := len(result.Columns); got != 2 {
		t.Fatalf("expected 2 columns, got %d", got)
	}
	if got := len(result.Rows); got != 1 {
		t.Fatalf("expected 1 row, got %d", got)
	}
}

func TestResultRepresentsStatusOutput(t *testing.T) {
	result := Result{
		Kind:         parser.StatementInsert,
		Message:      "1 row inserted",
		AffectedRows: 1,
		Elapsed:      2 * time.Millisecond,
	}

	if result.IsSelect() {
		t.Fatal("did not expect select result")
	}
	if result.HasRows() {
		t.Fatal("did not expect tabular result")
	}
	if result.Message != "1 row inserted" {
		t.Fatalf("unexpected message %q", result.Message)
	}
	if result.AffectedRows != 1 {
		t.Fatalf("unexpected affected row count %d", result.AffectedRows)
	}
}

func TestExecuteSQLClassifiesParsedStatement(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	app, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	if _, err := app.ExecuteSQL("create table students (id int primary key, name varchar(255), age int)"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("insert into students (id, name, age) values (1, 'Alice', 19)"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("insert into students (id, name, age) values (2, 'Bob', 20)"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("insert into students (id, name, age) values (3, 'Carol', 20)"); err != nil {
		t.Fatal(err)
	}

	result, err := app.ExecuteSQL("select name from students where age = 20 order by name desc")
	if err != nil {
		t.Fatal(err)
	}

	// fmt.Printf("result: %+v\n", result)

	if result.Kind != parser.StatementSelect {
		t.Fatalf("expected select kind, got %q", result.Kind)
	}
	if !result.IsSelect() {
		t.Fatal("expected select result")
	}
	if !result.HasRows() {
		t.Fatal("expected tabular select output")
	}
	if result.Message != "" {
		t.Fatalf("expected no status message for select result, got %q", result.Message)
	}
	if len(result.Columns) != 1 || result.Columns[0].Name != "name" || result.Columns[0].Type != shared.TypeString {
		t.Fatalf("unexpected select columns: %+v", result.Columns)
	}
	wantRows := []storage.Row{
		{storage.NewStringValue("Carol")},
		{storage.NewStringValue("Bob")},
	}
	if !reflect.DeepEqual(result.Rows, wantRows) {
		t.Fatalf("unexpected select rows: got %+v want %+v", result.Rows, wantRows)
	}
	if result.Elapsed < 0 {
		t.Fatalf("expected non-negative elapsed time, got %v", result.Elapsed)
	}
}

func TestExecuteSQLReturnsParseError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	app, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	result, err := app.ExecuteSQL("SELECT FROM")
	if err == nil {
		t.Fatal("expected parse error, got nil")
	}
	if result.Kind != parser.StatementUnknown {
		t.Fatalf("expected unknown kind on parse error, got %q", result.Kind)
	}
	if result.Elapsed < 0 {
		t.Fatalf("expected non-negative elapsed time, got %v", result.Elapsed)
	}
}

func TestExecuteSQLReturnsSelectSemanticError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	app, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	if _, err := app.ExecuteSQL("create table students (id int primary key, name varchar(255))"); err != nil {
		t.Fatal(err)
	}

	result, err := app.ExecuteSQL("select missing from students")
	if err == nil {
		t.Fatal("expected select semantic error, got nil")
	}
	if result.Kind != parser.StatementSelect {
		t.Fatalf("expected select kind on semantic error, got %q", result.Kind)
	}
	if !errors.Is(err, shared.ErrNotFound) && !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected catalog/binding style error, got %v", err)
	}
}

func TestExecuteSQLRunsDDLLifecycle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	app, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	result, err := app.ExecuteSQL("create table students (id int primary key, name varchar(255))")
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != parser.StatementCreateTable {
		t.Fatalf("expected create-table kind, got %q", result.Kind)
	}
	if result.Message != "table students created" {
		t.Fatalf("unexpected create-table message %q", result.Message)
	}

	result, err = app.ExecuteSQL("create index idx_students_name on students (name)")
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != parser.StatementCreateIndex {
		t.Fatalf("expected create-index kind, got %q", result.Kind)
	}
	if result.Message != "index idx_students_name created" {
		t.Fatalf("unexpected create-index message %q", result.Message)
	}

	err = app.Store.View(func(tx *storage.Tx) error {
		if _, err := app.Catalog.GetTable(tx, "students"); err != nil {
			return err
		}
		if _, err := app.Catalog.GetIndex(tx, "idx_students_name"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err = app.ExecuteSQL("drop index idx_students_name on students")
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != parser.StatementDropIndex {
		t.Fatalf("expected drop-index kind, got %q", result.Kind)
	}
	if result.Message != "index idx_students_name dropped" {
		t.Fatalf("unexpected drop-index message %q", result.Message)
	}

	result, err = app.ExecuteSQL("drop table students")
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != parser.StatementDropTable {
		t.Fatalf("expected drop-table kind, got %q", result.Kind)
	}
	if result.Message != "table students dropped" {
		t.Fatalf("unexpected drop-table message %q", result.Message)
	}

	err = app.Store.View(func(tx *storage.Tx) error {
		_, err := app.Catalog.GetTable(tx, "students")
		if err == nil {
			t.Fatal("expected dropped table lookup to fail")
		}
		if !errors.Is(err, shared.ErrNotFound) {
			t.Fatalf("expected not-found error after drop, got %v", err)
		}
		_, err = app.Catalog.GetIndex(tx, "idx_students_name")
		if err == nil {
			t.Fatal("expected dropped index lookup to fail")
		}
		if !errors.Is(err, shared.ErrNotFound) {
			t.Fatalf("expected not-found error after index drop, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecuteSQLDDLFailureRollsBack(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	app, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	if _, err := app.ExecuteSQL("create table students (id int primary key, name varchar(255))"); err != nil {
		t.Fatal(err)
	}

	result, err := app.ExecuteSQL("create table students (id int primary key)")
	if err == nil {
		t.Fatal("expected duplicate create-table error, got nil")
	}
	if !errors.Is(err, shared.ErrAlreadyExists) {
		t.Fatalf("expected already-exists error, got %v", err)
	}
	if result.Kind != parser.StatementCreateTable {
		t.Fatalf("expected create-table kind on failure, got %q", result.Kind)
	}

	err = app.Store.View(func(tx *storage.Tx) error {
		table, err := app.Catalog.GetTable(tx, "students")
		if err != nil {
			return err
		}
		if len(table.Columns) != 2 {
			t.Fatalf("expected original table definition to remain, got %d columns", len(table.Columns))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecuteSQLRunsWriteDMLLifecycle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	app, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	if _, err := app.ExecuteSQL("create table students (id int primary key, name varchar(255))"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("create index idx_students_name on students (name)"); err != nil {
		t.Fatal(err)
	}

	result, err := app.ExecuteSQL("insert into students (id, name) values (1, 'Ada')")
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != parser.StatementInsert {
		t.Fatalf("expected insert kind, got %q", result.Kind)
	}
	if result.Message != "1 row inserted" || result.AffectedRows != 1 {
		t.Fatalf("unexpected insert result: %+v", result)
	}

	rows, err := loadTableRows(app, "students")
	if err != nil {
		t.Fatal(err)
	}
	wantRows := []storage.Row{{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Ada"),
	}}
	if !reflect.DeepEqual(rows, wantRows) {
		t.Fatalf("unexpected rows after insert: got %+v want %+v", rows, wantRows)
	}

	result, err = app.ExecuteSQL("update students set name = 'Grace'")
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != parser.StatementUpdate {
		t.Fatalf("expected update kind, got %q", result.Kind)
	}
	if result.Message != "1 rows updated" || result.AffectedRows != 1 {
		t.Fatalf("unexpected update result: %+v", result)
	}

	rows, err = loadTableRows(app, "students")
	if err != nil {
		t.Fatal(err)
	}
	wantRows = []storage.Row{{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Grace"),
	}}
	if !reflect.DeepEqual(rows, wantRows) {
		t.Fatalf("unexpected rows after update: got %+v want %+v", rows, wantRows)
	}

	result, err = app.ExecuteSQL("delete from students")
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != parser.StatementDelete {
		t.Fatalf("expected delete kind, got %q", result.Kind)
	}
	if result.Message != "1 rows deleted" || result.AffectedRows != 1 {
		t.Fatalf("unexpected delete result: %+v", result)
	}

	rows, err = loadTableRows(app, "students")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected no rows after delete, got %+v", rows)
	}
}

func TestExecuteSQLDeleteWithWhereRemovesOnlyMatchingRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	app, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	if _, err := app.ExecuteSQL("create table students (id int primary key, name varchar(255))"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("create index idx_students_name on students (name)"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("insert into students (id, name) values (1, 'Ada')"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("insert into students (id, name) values (2, 'Grace')"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("insert into students (id, name) values (3, 'Ada')"); err != nil {
		t.Fatal(err)
	}

	result, err := app.ExecuteSQL("delete from students where name = 'Ada'")
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != parser.StatementDelete {
		t.Fatalf("expected delete kind, got %q", result.Kind)
	}
	if result.Message != "2 rows deleted" || result.AffectedRows != 2 {
		t.Fatalf("unexpected delete result: %+v", result)
	}

	rows, err := loadTableRows(app, "students")
	if err != nil {
		t.Fatal(err)
	}
	wantRows := []storage.Row{{
		storage.NewIntegerValue(2),
		storage.NewStringValue("Grace"),
	}}
	if !reflect.DeepEqual(rows, wantRows) {
		t.Fatalf("unexpected rows after predicate delete: got %+v want %+v", rows, wantRows)
	}
}

func TestExecuteSQLUpdateWithWhereRewritesOnlyMatchingRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	app, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	if _, err := app.ExecuteSQL("create table students (id int primary key, name varchar(255))"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("create index idx_students_name on students (name)"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("insert into students (id, name) values (1, 'Ada')"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("insert into students (id, name) values (2, 'Grace')"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("insert into students (id, name) values (3, 'Ada')"); err != nil {
		t.Fatal(err)
	}

	result, err := app.ExecuteSQL("update students set name = 'Updated' where name = 'Ada'")
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != parser.StatementUpdate {
		t.Fatalf("expected update kind, got %q", result.Kind)
	}
	if result.Message != "2 rows updated" || result.AffectedRows != 2 {
		t.Fatalf("unexpected update result: %+v", result)
	}

	rows, err := loadTableRows(app, "students")
	if err != nil {
		t.Fatal(err)
	}
	wantRows := []storage.Row{
		{storage.NewIntegerValue(1), storage.NewStringValue("Updated")},
		{storage.NewIntegerValue(2), storage.NewStringValue("Grace")},
		{storage.NewIntegerValue(3), storage.NewStringValue("Updated")},
	}
	if !reflect.DeepEqual(rows, wantRows) {
		t.Fatalf("unexpected rows after predicate update: got %+v want %+v", rows, wantRows)
	}
}

func TestExecuteSQLWriteDMLFailureRollsBack(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	app, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	if _, err := app.ExecuteSQL("create table students (id int primary key, name varchar(255))"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ExecuteSQL("insert into students (id, name) values (1, 'Ada')"); err != nil {
		t.Fatal(err)
	}

	result, err := app.ExecuteSQL("insert into students (id, name) values (1, 'Grace')")
	if err == nil {
		t.Fatal("expected duplicate primary-key insert error, got nil")
	}
	if !errors.Is(err, shared.ErrConstraintViolation) {
		t.Fatalf("expected constraint-violation error, got %v", err)
	}
	if result.Kind != parser.StatementInsert {
		t.Fatalf("expected insert kind on failure, got %q", result.Kind)
	}

	rows, err := loadTableRows(app, "students")
	if err != nil {
		t.Fatal(err)
	}
	wantRows := []storage.Row{{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Ada"),
	}}
	if !reflect.DeepEqual(rows, wantRows) {
		t.Fatalf("unexpected rows after failed insert rollback: got %+v want %+v", rows, wantRows)
	}
}

func loadTableRows(app *App, tableName string) ([]storage.Row, error) {
	rows := make([]storage.Row, 0)
	err := app.Store.View(func(tx *storage.Tx) error {
		table, err := app.Catalog.GetTable(tx, tableName)
		if err != nil {
			return err
		}

		return tx.ForEach(table.TableBucket, func(_ []byte, payload []byte) error {
			row, err := storage.DecodeRow(payload)
			if err != nil {
				return err
			}
			rows = append(rows, row)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return rows, nil
}
