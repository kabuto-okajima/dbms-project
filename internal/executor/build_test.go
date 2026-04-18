package executor

import (
	"path/filepath"
	"testing"

	"dbms-project/internal/binder"
	"dbms-project/internal/catalog"
	"dbms-project/internal/planner"
	"dbms-project/internal/shared"
	"dbms-project/internal/statement"
	"dbms-project/internal/storage"
)

func TestBuildPhysicalPlanChoosesIndexScanForIndexedPredicate(t *testing.T) {
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
				{Name: "age", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		return manager.CreateIndex(tx, shared.IndexDefinition{
			Name:       "idx_students_age",
			TableName:  "students",
			ColumnName: "age",
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		bound, err := binder.New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.StarExpr{}},
			},
			From: []statement.TableRef{
				{Name: "students"},
			},
			Where: statement.ComparisonExpr{
				Left:     statement.ColumnRef{ColumnName: "age"},
				Operator: statement.OpEqual,
				Right: statement.LiteralExpr{
					Value: storage.NewIntegerValue(20),
				},
			},
		})
		if err != nil {
			return err
		}

		logical, err := planner.BuildLogicalSelect(bound)
		if err != nil {
			return err
		}

		physical, err := BuildPhysicalPlan(tx, logical)
		if err != nil {
			return err
		}

		project, ok := physical.(PhysicalProject)
		if !ok {
			t.Fatalf("expected project root, got %T", physical)
		}
		indexScan, ok := project.Input.(PhysicalIndexScan)
		if !ok {
			t.Fatalf("expected project input to be index scan, got %T", project.Input)
		}
		if indexScan.Index.Name != "idx_students_age" {
			t.Fatalf("expected idx_students_age, got %+v", indexScan.Index)
		}
		if indexScan.Operator != statement.OpEqual || indexScan.Value != storage.NewIntegerValue(20) {
			t.Fatalf("unexpected index scan details: %+v", indexScan)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuildPhysicalPlanLowersLogicalJoinToNestedLoopJoin(t *testing.T) {
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
		bound, err := binder.New(manager).BindSelect(tx, statement.SelectStatement{
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

		logical, err := planner.BuildLogicalSelect(bound)
		if err != nil {
			return err
		}

		physical, err := BuildPhysicalPlan(tx, logical)
		if err != nil {
			return err
		}

		project, ok := physical.(PhysicalProject)
		if !ok {
			t.Fatalf("expected project root, got %T", physical)
		}
		filter, ok := project.Input.(PhysicalFilter)
		if !ok {
			t.Fatalf("expected project input to be filter, got %T", project.Input)
		}
		join, ok := filter.Input.(PhysicalNestedLoopJoin)
		if !ok {
			t.Fatalf("expected filter input to be nested-loop join, got %T", filter.Input)
		}
		if join.Predicate == nil {
			t.Fatal("expected join predicate to be populated")
		}

		leftScan, ok := join.Left.(PhysicalTableScan)
		if !ok {
			t.Fatalf("expected join left input to be table scan, got %T", join.Left)
		}
		rightScan, ok := join.Right.(PhysicalTableScan)
		if !ok {
			t.Fatalf("expected join right input to be table scan, got %T", join.Right)
		}
		if leftScan.Table.Name != "students" || rightScan.Table.Name != "departments" {
			t.Fatalf("unexpected join inputs: left=%q right=%q", leftScan.Table.Name, rightScan.Table.Name)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuildPhysicalPlanFallsBackToTableScanWithoutIndex(t *testing.T) {
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
				{Name: "age", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		bound, err := binder.New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.StarExpr{}},
			},
			From: []statement.TableRef{
				{Name: "students"},
			},
			Where: statement.ComparisonExpr{
				Left:     statement.ColumnRef{ColumnName: "age"},
				Operator: statement.OpEqual,
				Right: statement.LiteralExpr{
					Value: storage.NewIntegerValue(20),
				},
			},
		})
		if err != nil {
			return err
		}

		logical, err := planner.BuildLogicalSelect(bound)
		if err != nil {
			return err
		}

		physical, err := BuildPhysicalPlan(tx, logical)
		if err != nil {
			return err
		}

		project := physical.(PhysicalProject)
		filter, ok := project.Input.(PhysicalFilter)
		if !ok {
			t.Fatalf("expected project input to be filter, got %T", project.Input)
		}
		if _, ok := filter.Input.(PhysicalTableScan); !ok {
			t.Fatalf("expected filter input to be table scan, got %T", filter.Input)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuildPhysicalPlanChoosesIndexScanWithResidualAndPredicate(t *testing.T) {
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
				{Name: "age", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		return manager.CreateIndex(tx, shared.IndexDefinition{
			Name:       "idx_students_age",
			TableName:  "students",
			ColumnName: "age",
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		bound, err := binder.New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.StarExpr{}},
			},
			From: []statement.TableRef{
				{Name: "students"},
			},
			Where: statement.LogicalExpr{
				Operator: statement.OpAnd,
				Terms: []statement.Expression{
					statement.ComparisonExpr{
						Left:     statement.ColumnRef{ColumnName: "age"},
						Operator: statement.OpEqual,
						Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(20)},
					},
					statement.ComparisonExpr{
						Left:     statement.ColumnRef{ColumnName: "id"},
						Operator: statement.OpGreaterThan,
						Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(2)},
					},
				},
			},
		})
		if err != nil {
			return err
		}

		logical, err := planner.BuildLogicalSelect(bound)
		if err != nil {
			return err
		}

		physical, err := BuildPhysicalPlan(tx, logical)
		if err != nil {
			return err
		}

		project := physical.(PhysicalProject)
		indexScan, ok := project.Input.(PhysicalIndexScan)
		if !ok {
			t.Fatalf("expected project input to be index scan, got %T", project.Input)
		}
		if indexScan.Residual == nil {
			t.Fatal("expected residual predicate to remain on index scan")
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuildPhysicalPlanPrefersFirstCanonicalIndexTerm(t *testing.T) {
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
				{Name: "age", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}
		if err := manager.CreateIndex(tx, shared.IndexDefinition{
			Name:       "idx_students_age",
			TableName:  "students",
			ColumnName: "age",
		}); err != nil {
			return err
		}
		return manager.CreateIndex(tx, shared.IndexDefinition{
			Name:       "idx_students_id",
			TableName:  "students",
			ColumnName: "id",
			IsUnique:   true,
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		bound, err := binder.New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.StarExpr{}},
			},
			From: []statement.TableRef{
				{Name: "students"},
			},
			Where: statement.LogicalExpr{
				Operator: statement.OpAnd,
				Terms: []statement.Expression{
					statement.ComparisonExpr{
						Left:     statement.ColumnRef{ColumnName: "id"},
						Operator: statement.OpGreaterThan,
						Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(2)},
					},
					statement.ComparisonExpr{
						Left:     statement.ColumnRef{ColumnName: "age"},
						Operator: statement.OpEqual,
						Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(20)},
					},
				},
			},
		})
		if err != nil {
			return err
		}

		logical, err := planner.BuildLogicalSelect(bound)
		if err != nil {
			return err
		}
		optimized := planner.Optimize(logical)

		physical, err := BuildPhysicalPlan(tx, optimized)
		if err != nil {
			return err
		}

		project := physical.(PhysicalProject)
		indexScan, ok := project.Input.(PhysicalIndexScan)
		if !ok {
			t.Fatalf("expected project input to be index scan, got %T", project.Input)
		}
		if indexScan.Index.Name != "idx_students_age" {
			t.Fatalf("expected canonical equality term to choose idx_students_age, got %+v", indexScan.Index)
		}
		if indexScan.Operator != statement.OpEqual || indexScan.Value != storage.NewIntegerValue(20) {
			t.Fatalf("unexpected chosen index term: %+v", indexScan)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
