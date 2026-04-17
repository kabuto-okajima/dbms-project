package binder

import (
	"errors"
	"path/filepath"
	"testing"

	"dbms-project/internal/catalog"
	"dbms-project/internal/shared"
	"dbms-project/internal/statement"
	"dbms-project/internal/storage"
)

func TestBindSelectResolvesSingleTableColumns(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
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
		b := New(manager)
		bound, err := b.BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{
					Expr: statement.ColumnRef{ColumnName: "id"},
				},
			},
			From: []statement.TableRef{
				{Name: "students", Alias: "s"},
			},
		})
		if err != nil {
			return err
		}

		if len(bound.From) != 1 || len(bound.SelectItems) != 1 {
			t.Fatalf("unexpected bound select shape: %+v", bound)
		}

		columnExpr, ok := bound.SelectItems[0].Expr.(BoundColumnRef)
		if !ok {
			t.Fatalf("expected bound column expression, got %T", bound.SelectItems[0].Expr)
		}
		if bound.From[0].Alias != "s" {
			t.Fatalf("expected alias s, got %q", bound.From[0].Alias)
		}
		if columnExpr.TableName != "students" || columnExpr.ColumnName != "id" || columnExpr.Type != shared.TypeInteger {
			t.Fatalf("unexpected bound column: %+v", columnExpr)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBindSelectBindsSingleTableExpressions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
				{Name: "age", Type: shared.TypeInteger},
				{Name: "dept_id", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		bound, err := New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.ColumnRef{TableName: "s", ColumnName: "dept_id"}, Alias: "dept"},
				{Expr: statement.AggregateExpr{Function: statement.AggCount, Arg: statement.StarExpr{}}},
			},
			From: []statement.TableRef{
				{Name: "students", Alias: "s"},
			},
			Where: statement.LogicalExpr{
				Operator: statement.OpAnd,
				Terms: []statement.Expression{
					statement.ComparisonExpr{
						Left:     statement.ColumnRef{ColumnName: "age"},
						Operator: statement.OpGreaterThan,
						Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(20)},
					},
					statement.ComparisonExpr{
						Left:     statement.ColumnRef{ColumnName: "dept_id"},
						Operator: statement.OpEqual,
						Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(1)},
					},
				},
			},
			GroupBy: []statement.Expression{
				statement.ColumnRef{ColumnName: "dept_id"},
			},
			Having: statement.ComparisonExpr{
				Left:     statement.AggregateExpr{Function: statement.AggCount, Arg: statement.StarExpr{}},
				Operator: statement.OpGreaterThan,
				Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(1)},
			},
			OrderBy: []statement.OrderByTerm{
				{Expr: statement.ColumnRef{ColumnName: "dept_id"}, Desc: true},
			},
		})
		if err != nil {
			return err
		}

		if bound.SelectItems[0].Alias != "dept" || len(bound.GroupBy) != 1 || len(bound.OrderBy) != 1 {
			t.Fatalf("unexpected bound select clauses: %+v", bound)
		}
		if _, ok := bound.Where.(BoundLogicalExpr); !ok {
			t.Fatalf("expected bound logical WHERE expression, got %T", bound.Where)
		}
		if _, ok := bound.GroupBy[0].(BoundColumnRef); !ok {
			t.Fatalf("expected bound group-by column, got %T", bound.GroupBy[0])
		}
		havingExpr, ok := bound.Having.(BoundComparisonExpr)
		if !ok {
			t.Fatalf("expected bound HAVING comparison, got %T", bound.Having)
		}
		if _, ok := havingExpr.Left.(BoundAggregateExpr); !ok {
			t.Fatalf("expected bound aggregate in HAVING, got %T", havingExpr.Left)
		}
		if _, ok := bound.OrderBy[0].Expr.(BoundColumnRef); !ok || !bound.OrderBy[0].Desc {
			t.Fatalf("unexpected order-by binding: %+v", bound.OrderBy[0])
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBindSelectResolvesAliasesInHavingAndOrderBy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "dept_id", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		bound, err := New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.ColumnRef{ColumnName: "dept_id"}, Alias: "dept"},
				{Expr: statement.AggregateExpr{Function: statement.AggCount, Arg: statement.StarExpr{}}, Alias: "total"},
			},
			From: []statement.TableRef{{Name: "students"}},
			GroupBy: []statement.Expression{
				statement.ColumnRef{ColumnName: "dept_id"},
			},
			Having: statement.ComparisonExpr{
				Left:     statement.ColumnRef{ColumnName: "total"},
				Operator: statement.OpGreaterThan,
				Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(1)},
			},
			OrderBy: []statement.OrderByTerm{
				{Expr: statement.ColumnRef{ColumnName: "dept"}, Desc: false},
			},
		})
		if err != nil {
			return err
		}

		havingExpr, ok := bound.Having.(BoundComparisonExpr)
		if !ok {
			t.Fatalf("expected bound HAVING comparison, got %T", bound.Having)
		}
		if _, ok := havingExpr.Left.(BoundAggregateExpr); !ok {
			t.Fatalf("expected HAVING alias to resolve to aggregate, got %T", havingExpr.Left)
		}

		if _, ok := bound.OrderBy[0].Expr.(BoundColumnRef); !ok {
			t.Fatalf("expected ORDER BY alias to resolve to grouping column, got %T", bound.OrderBy[0].Expr)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBindSelectRejectsTypeMismatch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
				{Name: "age", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		_, err := New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{{Expr: statement.StarExpr{}}},
			From:        []statement.TableRef{{Name: "students"}},
			Where: statement.ComparisonExpr{
				Left:     statement.ColumnRef{ColumnName: "age"},
				Operator: statement.OpEqual,
				Right:    statement.LiteralExpr{Value: storage.NewStringValue("Ada")},
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
}

func TestBindSelectRejectsInvalidAggregateUsage(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
				{Name: "dept_id", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		stmt statement.SelectStatement
		want error
	}{
		{
			name: "non grouped select column",
			stmt: statement.SelectStatement{
				SelectItems: []statement.SelectItem{
					{Expr: statement.ColumnRef{ColumnName: "name"}},
					{Expr: statement.AggregateExpr{Function: statement.AggCount, Arg: statement.StarExpr{}}},
				},
				From: []statement.TableRef{{Name: "students"}},
				GroupBy: []statement.Expression{
					statement.ColumnRef{ColumnName: "dept_id"},
				},
			},
			want: shared.ErrInvalidDefinition,
		},
		{
			name: "having non grouping column",
			stmt: statement.SelectStatement{
				SelectItems: []statement.SelectItem{
					{Expr: statement.ColumnRef{ColumnName: "dept_id"}},
					{Expr: statement.AggregateExpr{Function: statement.AggCount, Arg: statement.StarExpr{}}},
				},
				From: []statement.TableRef{{Name: "students"}},
				GroupBy: []statement.Expression{
					statement.ColumnRef{ColumnName: "dept_id"},
				},
				Having: statement.ComparisonExpr{
					Left:     statement.ColumnRef{ColumnName: "name"},
					Operator: statement.OpEqual,
					Right:    statement.LiteralExpr{Value: storage.NewStringValue("Bob")},
				},
			},
			want: shared.ErrInvalidDefinition,
		},
		{
			name: "sum requires integer column",
			stmt: statement.SelectStatement{
				SelectItems: []statement.SelectItem{
					{Expr: statement.AggregateExpr{Function: statement.AggSum, Arg: statement.ColumnRef{ColumnName: "name"}}},
				},
				From: []statement.TableRef{{Name: "students"}},
			},
			want: shared.ErrTypeMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.View(func(tx *storage.Tx) error {
				_, err := New(manager).BindSelect(tx, tt.stmt)
				if err == nil {
					t.Fatal("expected aggregate-legality error, got nil")
				}
				if !errors.Is(err, tt.want) {
					t.Fatalf("expected %v, got %v", tt.want, err)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBindSelectRejectsResolutionErrors(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	err = store.Update(func(tx *storage.Tx) error {
		if err := manager.CreateTable(tx, shared.TableDefinition{
			Name:       "students",
			Columns:    []shared.ColumnDefinition{{Name: "id", Type: shared.TypeInteger}},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		return manager.CreateTable(tx, shared.TableDefinition{
			Name:       "departments",
			Columns:    []shared.ColumnDefinition{{Name: "id", Type: shared.TypeInteger}},
			PrimaryKey: []string{"id"},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("missing table", func(t *testing.T) {
		err := store.View(func(tx *storage.Tx) error {
			_, err := New(manager).BindSelect(tx, statement.SelectStatement{
				SelectItems: []statement.SelectItem{{Expr: statement.StarExpr{}}},
				From:        []statement.TableRef{{Name: "missing"}},
			})
			if err == nil {
				t.Fatal("expected missing-table error, got nil")
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

	t.Run("duplicate alias", func(t *testing.T) {
		err := store.View(func(tx *storage.Tx) error {
			_, err := New(manager).BindSelect(tx, statement.SelectStatement{
				SelectItems: []statement.SelectItem{{Expr: statement.StarExpr{}}},
				From: []statement.TableRef{
					{Name: "students", Alias: "t"},
					{Name: "departments", Alias: "t"},
				},
			})
			if err == nil {
				t.Fatal("expected duplicate-alias error, got nil")
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

	t.Run("unknown column", func(t *testing.T) {
		err := store.View(func(tx *storage.Tx) error {
			_, err := New(manager).BindSelect(tx, statement.SelectStatement{
				SelectItems: []statement.SelectItem{{Expr: statement.ColumnRef{ColumnName: "missing_column"}}},
				From:        []statement.TableRef{{Name: "students"}},
			})
			if err == nil {
				t.Fatal("expected missing-column error, got nil")
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

	t.Run("wrong qualifier", func(t *testing.T) {
		err := store.View(func(tx *storage.Tx) error {
			_, err := New(manager).BindSelect(tx, statement.SelectStatement{
				SelectItems: []statement.SelectItem{{Expr: statement.ColumnRef{TableName: "d", ColumnName: "id"}}},
				From:        []statement.TableRef{{Name: "students", Alias: "s"}},
			})
			if err == nil {
				t.Fatal("expected unknown-table error, got nil")
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

	t.Run("duplicate select alias", func(t *testing.T) {
		err := store.View(func(tx *storage.Tx) error {
			_, err := New(manager).BindSelect(tx, statement.SelectStatement{
				SelectItems: []statement.SelectItem{
					{Expr: statement.ColumnRef{ColumnName: "id"}, Alias: "x"},
					{Expr: statement.ColumnRef{ColumnName: "id"}, Alias: "x"},
				},
				From: []statement.TableRef{{Name: "students"}},
			})
			if err == nil {
				t.Fatal("expected duplicate-alias error, got nil")
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
