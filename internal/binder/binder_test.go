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

func TestBindSelectResolvesTwoTableJoinColumns(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	err = store.Update(func(tx *storage.Tx) error {
		if err := manager.CreateTable(tx, shared.TableDefinition{
			Name: "students",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "dept_id", Type: shared.TypeInteger},
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
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

	err = store.View(func(tx *storage.Tx) error {
		bound, err := New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.ColumnRef{TableName: "s", ColumnName: "name"}, Alias: "student_name"},
				{Expr: statement.ColumnRef{TableName: "d", ColumnName: "name"}, Alias: "department_name"},
			},
			From: []statement.TableRef{
				{Name: "students", Alias: "s"},
				{Name: "departments", Alias: "d"},
			},
			Join: &statement.JoinClause{
				Type: statement.JoinInner,
				On: statement.ComparisonExpr{
					Left:     statement.ColumnRef{TableName: "s", ColumnName: "dept_id"},
					Operator: statement.OpEqual,
					Right:    statement.ColumnRef{TableName: "d", ColumnName: "id"},
				},
			},
			Where: statement.ComparisonExpr{
				Left:     statement.ColumnRef{TableName: "d", ColumnName: "id"},
				Operator: statement.OpGreaterThan,
				Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(10)},
			},
		})
		if err != nil {
			return err
		}

		if len(bound.From) != 2 || bound.Join == nil {
			t.Fatalf("unexpected bound two-table shape: %+v", bound)
		}

		leftSelect := bound.SelectItems[0].Expr.(BoundColumnRef)
		rightSelect := bound.SelectItems[1].Expr.(BoundColumnRef)
		if leftSelect.TableName != "students" || leftSelect.ColumnName != "name" || leftSelect.Ordinal != 2 {
			t.Fatalf("unexpected left selected column: %+v", leftSelect)
		}
		if rightSelect.TableName != "departments" || rightSelect.ColumnName != "name" || rightSelect.Ordinal != 4 {
			t.Fatalf("unexpected right selected column: %+v", rightSelect)
		}

		onExpr, ok := bound.Join.On.(BoundComparisonExpr)
		if !ok {
			t.Fatalf("expected bound ON comparison, got %T", bound.Join.On)
		}
		leftOn := onExpr.Left.(BoundColumnRef)
		rightOn := onExpr.Right.(BoundColumnRef)
		if leftOn.Ordinal != 1 || rightOn.Ordinal != 3 {
			t.Fatalf("unexpected ON ordinals: left=%+v right=%+v", leftOn, rightOn)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBindSelectSupportsAliasBasedSelfJoin(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	manager := catalog.NewManager()
	err = store.Update(func(tx *storage.Tx) error {
		return manager.CreateTable(tx, shared.TableDefinition{
			Name: "t",
			Columns: []shared.ColumnDefinition{
				{Name: "id", Type: shared.TypeInteger},
				{Name: "b", Type: shared.TypeInteger},
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
				{Expr: statement.ColumnRef{TableName: "r", ColumnName: "id"}},
				{Expr: statement.ColumnRef{TableName: "s", ColumnName: "id"}},
			},
			From: []statement.TableRef{
				{Name: "t", Alias: "r"},
				{Name: "t", Alias: "s"},
			},
			Join: &statement.JoinClause{
				Type: statement.JoinInner,
				On: statement.ComparisonExpr{
					Left:     statement.ColumnRef{TableName: "r", ColumnName: "b"},
					Operator: statement.OpEqual,
					Right:    statement.ColumnRef{TableName: "s", ColumnName: "b"},
				},
			},
		})
		if err != nil {
			return err
		}

		if len(bound.From) != 2 || bound.Join == nil {
			t.Fatalf("unexpected bound self-join shape: %+v", bound)
		}

		left := bound.SelectItems[0].Expr.(BoundColumnRef)
		right := bound.SelectItems[1].Expr.(BoundColumnRef)
		if left.Ordinal != 0 || right.Ordinal != 2 {
			t.Fatalf("unexpected self-join select ordinals: left=%+v right=%+v", left, right)
		}

		onExpr, ok := bound.Join.On.(BoundComparisonExpr)
		if !ok {
			t.Fatalf("expected bound ON comparison, got %T", bound.Join.On)
		}
		leftOn := onExpr.Left.(BoundColumnRef)
		rightOn := onExpr.Right.(BoundColumnRef)
		if leftOn.Ordinal != 1 || rightOn.Ordinal != 3 {
			t.Fatalf("unexpected self-join ON ordinals: left=%+v right=%+v", leftOn, rightOn)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBindSelectAliasHidesBaseTableName(t *testing.T) {
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
			},
			PrimaryKey: []string{"id"},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		_, err := New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.ColumnRef{TableName: "students", ColumnName: "id"}},
			},
			From: []statement.TableRef{
				{Name: "students", Alias: "s"},
			},
		})
		if err == nil {
			t.Fatal("expected base-table qualifier to be hidden by alias, got nil")
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

func TestBindTablePredicate(t *testing.T) {
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
		expr statement.Expression
		want error
		check func(t *testing.T, expr BoundExpression)
	}{
		{
			name: "nil predicate",
			expr: nil,
			check: func(t *testing.T, expr BoundExpression) {
				if expr != nil {
					t.Fatalf("expected nil predicate, got %T", expr)
				}
			},
		},
		{
			name: "comparison predicate",
			expr: statement.ComparisonExpr{
				Left:     statement.ColumnRef{ColumnName: "id"},
				Operator: statement.OpEqual,
				Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(1)},
			},
			check: func(t *testing.T, expr BoundExpression) {
				comparison, ok := expr.(BoundComparisonExpr)
				if !ok {
					t.Fatalf("expected comparison predicate, got %T", expr)
				}
				column, ok := comparison.Left.(BoundColumnRef)
				if !ok {
					t.Fatalf("expected bound column on left side, got %T", comparison.Left)
				}
				if column.TableName != "students" || column.ColumnName != "id" || column.Ordinal != 0 {
					t.Fatalf("unexpected bound column: %+v", column)
				}
			},
		},
		{
			name: "logical predicate",
			expr: statement.LogicalExpr{
				Operator: statement.OpAnd,
				Terms: []statement.Expression{
					statement.ComparisonExpr{
						Left:     statement.ColumnRef{ColumnName: "id"},
						Operator: statement.OpEqual,
						Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(1)},
					},
					statement.ComparisonExpr{
						Left:     statement.ColumnRef{ColumnName: "dept_id"},
						Operator: statement.OpEqual,
						Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(2)},
					},
				},
			},
			check: func(t *testing.T, expr BoundExpression) {
				logical, ok := expr.(BoundLogicalExpr)
				if !ok {
					t.Fatalf("expected logical predicate, got %T", expr)
				}
				if logical.Operator != statement.OpAnd || len(logical.Terms) != 2 {
					t.Fatalf("unexpected logical predicate: %+v", logical)
				}
			},
		},
		{
			name: "unknown column",
			expr: statement.ComparisonExpr{
				Left:     statement.ColumnRef{ColumnName: "missing"},
				Operator: statement.OpEqual,
				Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(1)},
			},
			want: shared.ErrNotFound,
		},
		{
			name: "type mismatch",
			expr: statement.ComparisonExpr{
				Left:     statement.ColumnRef{ColumnName: "id"},
				Operator: statement.OpEqual,
				Right:    statement.LiteralExpr{Value: storage.NewStringValue("Ada")},
			},
			want: shared.ErrTypeMismatch,
		},
		{
			name: "aggregate is rejected",
			expr: statement.ComparisonExpr{
				Left: statement.AggregateExpr{
					Function: statement.AggCount,
					Arg:      statement.StarExpr{},
				},
				Operator: statement.OpGreaterThan,
				Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(1)},
			},
			want: shared.ErrInvalidDefinition,
		},
		{
			name: "scalar expression is rejected",
			expr: statement.ColumnRef{ColumnName: "id"},
			want: shared.ErrInvalidDefinition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.View(func(tx *storage.Tx) error {
				bound, err := New(manager).BindTablePredicate(tx, "students", tt.expr)
				if tt.want != nil {
					if err == nil {
						t.Fatal("expected bind error, got nil")
					}
					if !errors.Is(err, tt.want) {
						t.Fatalf("expected %v, got %v", tt.want, err)
					}
					return nil
				}

				if err != nil {
					return err
				}
				if tt.check != nil {
					tt.check(t, bound)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
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

	t.Run("ambiguous two-table column", func(t *testing.T) {
		err := store.View(func(tx *storage.Tx) error {
			_, err := New(manager).BindSelect(tx, statement.SelectStatement{
				SelectItems: []statement.SelectItem{
					{Expr: statement.ColumnRef{ColumnName: "id"}},
				},
				From: []statement.TableRef{
					{Name: "students", Alias: "s"},
					{Name: "departments", Alias: "d"},
				},
				Join: &statement.JoinClause{
					Type: statement.JoinInner,
					On: statement.ComparisonExpr{
						Left:     statement.ColumnRef{TableName: "s", ColumnName: "id"},
						Operator: statement.OpEqual,
						Right:    statement.ColumnRef{TableName: "d", ColumnName: "id"},
					},
				},
			})
			if err == nil {
				t.Fatal("expected ambiguous-column error, got nil")
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

	t.Run("join requires on predicate", func(t *testing.T) {
		err := store.View(func(tx *storage.Tx) error {
			_, err := New(manager).BindSelect(tx, statement.SelectStatement{
				SelectItems: []statement.SelectItem{{Expr: statement.StarExpr{}}},
				From: []statement.TableRef{
					{Name: "students", Alias: "s"},
					{Name: "departments", Alias: "d"},
				},
				Join: &statement.JoinClause{
					Type: statement.JoinInner,
				},
			})
			if err == nil {
				t.Fatal("expected missing-join-predicate error, got nil")
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

	t.Run("join requires equality predicate", func(t *testing.T) {
		err := store.View(func(tx *storage.Tx) error {
			_, err := New(manager).BindSelect(tx, statement.SelectStatement{
				SelectItems: []statement.SelectItem{{Expr: statement.StarExpr{}}},
				From: []statement.TableRef{
					{Name: "students", Alias: "s"},
					{Name: "departments", Alias: "d"},
				},
				Join: &statement.JoinClause{
					Type: statement.JoinInner,
					On: statement.ComparisonExpr{
						Left:     statement.ColumnRef{TableName: "s", ColumnName: "id"},
						Operator: statement.OpGreaterThan,
						Right:    statement.ColumnRef{TableName: "d", ColumnName: "id"},
					},
				},
			})
			if err == nil {
				t.Fatal("expected non-equi-join error, got nil")
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

	t.Run("join keys must be column refs", func(t *testing.T) {
		err := store.View(func(tx *storage.Tx) error {
			_, err := New(manager).BindSelect(tx, statement.SelectStatement{
				SelectItems: []statement.SelectItem{{Expr: statement.StarExpr{}}},
				From: []statement.TableRef{
					{Name: "students", Alias: "s"},
					{Name: "departments", Alias: "d"},
				},
				Join: &statement.JoinClause{
					Type: statement.JoinInner,
					On: statement.ComparisonExpr{
						Left:     statement.ColumnRef{TableName: "s", ColumnName: "id"},
						Operator: statement.OpEqual,
						Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(1)},
					},
				},
			})
			if err == nil {
				t.Fatal("expected non-column-join-key error, got nil")
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

	t.Run("join keys must come from different tables", func(t *testing.T) {
		err := store.View(func(tx *storage.Tx) error {
			_, err := New(manager).BindSelect(tx, statement.SelectStatement{
				SelectItems: []statement.SelectItem{{Expr: statement.StarExpr{}}},
				From: []statement.TableRef{
					{Name: "students", Alias: "s"},
					{Name: "departments", Alias: "d"},
				},
				Join: &statement.JoinClause{
					Type: statement.JoinInner,
					On: statement.ComparisonExpr{
						Left:     statement.ColumnRef{TableName: "s", ColumnName: "id"},
						Operator: statement.OpEqual,
						Right:    statement.ColumnRef{TableName: "s", ColumnName: "id"},
					},
				},
			})
			if err == nil {
				t.Fatal("expected same-table-join-key error, got nil")
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
