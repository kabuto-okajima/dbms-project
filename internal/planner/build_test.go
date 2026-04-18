package planner

import (
	"path/filepath"
	"testing"

	"dbms-project/internal/binder"
	"dbms-project/internal/catalog"
	"dbms-project/internal/shared"
	"dbms-project/internal/statement"
	"dbms-project/internal/storage"
)

func TestBuildLogicalSelectPlansSingleTableScan(t *testing.T) {
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
		bound, err := binder.New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.StarExpr{}},
			},
			From: []statement.TableRef{
				{Name: "students"},
			},
		})
		if err != nil {
			return err
		}

		plan, err := BuildLogicalSelect(bound)
		if err != nil {
			return err
		}

		project, ok := plan.(LogicalProject)
		if !ok {
			t.Fatalf("expected project-root logical plan, got %T", plan)
		}
		if len(project.Outputs) != 1 || !project.Outputs[0].ExpandsStar {
			t.Fatalf("unexpected project outputs: %+v", project.Outputs)
		}

		scan, ok := project.Input.(LogicalScan)
		if !ok {
			t.Fatalf("expected project input to be scan, got %T", project.Input)
		}
		if scan.Table.Name != "students" {
			t.Fatalf("expected scan of students, got %q", scan.Table.Name)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuildLogicalSelectPlansFilterOverScan(t *testing.T) {
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
				Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(20)},
			},
		})
		if err != nil {
			return err
		}

		plan, err := BuildLogicalSelect(bound)
		if err != nil {
			return err
		}

		project, ok := plan.(LogicalProject)
		if !ok {
			t.Fatalf("expected project-root logical plan, got %T", plan)
		}
		if len(project.Outputs) != 1 || !project.Outputs[0].ExpandsStar {
			t.Fatalf("unexpected project outputs: %+v", project.Outputs)
		}

		filter, ok := project.Input.(LogicalFilter)
		if !ok {
			t.Fatalf("expected project input to be filter, got %T", project.Input)
		}
		if filter.Predicate == nil {
			t.Fatal("expected filter predicate to be populated")
		}

		scan, ok := filter.Input.(LogicalScan)
		if !ok {
			t.Fatalf("expected filter input to be scan, got %T", filter.Input)
		}
		if scan.Table.Name != "students" {
			t.Fatalf("expected scan of students, got %q", scan.Table.Name)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuildLogicalSelectPlansFilterOverJoin(t *testing.T) {
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

		plan, err := BuildLogicalSelect(bound)
		if err != nil {
			return err
		}

		project, ok := plan.(LogicalProject)
		if !ok {
			t.Fatalf("expected project-root logical plan, got %T", plan)
		}
		if len(project.Outputs) != 2 {
			t.Fatalf("expected 2 project outputs, got %+v", project.Outputs)
		}

		filter, ok := project.Input.(LogicalFilter)
		if !ok {
			t.Fatalf("expected project input to be filter, got %T", project.Input)
		}

		join, ok := filter.Input.(LogicalJoin)
		if !ok {
			t.Fatalf("expected filter input to be join, got %T", filter.Input)
		}
		if join.Predicate == nil {
			t.Fatal("expected join predicate to be populated")
		}

		leftScan, ok := join.Left.(LogicalScan)
		if !ok {
			t.Fatalf("expected join left input to be scan, got %T", join.Left)
		}
		rightScan, ok := join.Right.(LogicalScan)
		if !ok {
			t.Fatalf("expected join right input to be scan, got %T", join.Right)
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

func TestBuildLogicalSelectPlansScalarProjection(t *testing.T) {
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
		bound, err := binder.New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.ColumnRef{ColumnName: "name"}, Alias: "student_name"},
			},
			From: []statement.TableRef{
				{Name: "students"},
			},
		})
		if err != nil {
			return err
		}

		plan, err := BuildLogicalSelect(bound)
		if err != nil {
			return err
		}

		project, ok := plan.(LogicalProject)
		if !ok {
			t.Fatalf("expected project-root logical plan, got %T", plan)
		}
		if len(project.Outputs) != 1 {
			t.Fatalf("expected 1 project output, got %d", len(project.Outputs))
		}
		if project.Outputs[0].Name != "student_name" || project.Outputs[0].Type != shared.TypeString || project.Outputs[0].ExpandsStar {
			t.Fatalf("unexpected project output: %+v", project.Outputs[0])
		}
		if _, ok := project.Input.(LogicalScan); !ok {
			t.Fatalf("expected project input to be scan, got %T", project.Input)
		}

		// fmt.Printf("Logical project output: %+v\n", project.Outputs[0])
		// -> Logical project output: {Name:student_name Expr:{TableName:students ColumnName:name Ordinal:1 Type:string} Type:string ExpandsStar:false}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuildLogicalSelectPlansAggregateOverScan(t *testing.T) {
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
		bound, err := binder.New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.ColumnRef{ColumnName: "dept_id"}},
				{Expr: statement.AggregateExpr{Function: statement.AggCount, Arg: statement.StarExpr{}}},
			},
			From: []statement.TableRef{
				{Name: "students"},
			},
			GroupBy: []statement.Expression{
				statement.ColumnRef{ColumnName: "dept_id"},
			},
		})
		if err != nil {
			return err
		}

		plan, err := BuildLogicalSelect(bound)
		if err != nil {
			return err
		}

		project, ok := plan.(LogicalProject)
		if !ok {
			t.Fatalf("expected project-root logical plan, got %T", plan)
		}
		if len(project.Outputs) != 2 || project.Outputs[1].Name != "COUNT(*)" {
			t.Fatalf("unexpected project outputs: %+v", project.Outputs)
		}

		aggregate, ok := project.Input.(LogicalAggregate)
		if !ok {
			t.Fatalf("expected project input to be aggregate, got %T", project.Input)
		}
		if len(aggregate.GroupBy) != 1 || len(aggregate.Aggregates) != 1 {
			t.Fatalf("unexpected aggregate node contents: %+v", aggregate)
		}
		if _, ok := aggregate.Input.(LogicalScan); !ok {
			t.Fatalf("expected aggregate input to be scan, got %T", aggregate.Input)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuildLogicalSelectPlansHavingOverAggregate(t *testing.T) {
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
		bound, err := binder.New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.ColumnRef{ColumnName: "dept_id"}},
				{Expr: statement.AggregateExpr{Function: statement.AggCount, Arg: statement.StarExpr{}}},
			},
			From: []statement.TableRef{
				{Name: "students"},
			},
			GroupBy: []statement.Expression{
				statement.ColumnRef{ColumnName: "dept_id"},
			},
			Having: statement.ComparisonExpr{
				Left:     statement.AggregateExpr{Function: statement.AggCount, Arg: statement.StarExpr{}},
				Operator: statement.OpGreaterThan,
				Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(1)},
			},
		})
		if err != nil {
			return err
		}

		plan, err := BuildLogicalSelect(bound)
		if err != nil {
			return err
		}

		project, ok := plan.(LogicalProject)
		if !ok {
			t.Fatalf("expected project-root logical plan, got %T", plan)
		}

		havingFilter, ok := project.Input.(LogicalFilter)
		if !ok {
			t.Fatalf("expected project input to be HAVING filter, got %T", project.Input)
		}
		if havingFilter.Predicate == nil {
			t.Fatal("expected HAVING predicate to be populated")
		}

		aggregate, ok := havingFilter.Input.(LogicalAggregate)
		if !ok {
			t.Fatalf("expected HAVING filter input to be aggregate, got %T", havingFilter.Input)
		}
		if len(aggregate.GroupBy) != 1 || len(aggregate.Aggregates) != 1 {
			t.Fatalf("unexpected aggregate under HAVING: %+v", aggregate)
		}
		if _, ok := aggregate.Input.(LogicalScan); !ok {
			t.Fatalf("expected aggregate input to be scan, got %T", aggregate.Input)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuildLogicalSelectPlansSortBeforeProject(t *testing.T) {
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
		bound, err := binder.New(manager).BindSelect(tx, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.ColumnRef{ColumnName: "dept_id"}},
			},
			From: []statement.TableRef{
				{Name: "students"},
			},
			OrderBy: []statement.OrderByTerm{
				{Expr: statement.ColumnRef{ColumnName: "dept_id"}, Desc: true},
			},
		})
		if err != nil {
			return err
		}

		plan, err := BuildLogicalSelect(bound)
		if err != nil {
			return err
		}

		project, ok := plan.(LogicalProject)
		if !ok {
			t.Fatalf("expected project-root logical plan, got %T", plan)
		}

		sort, ok := project.Input.(LogicalSort)
		if !ok {
			t.Fatalf("expected project input to be sort, got %T", project.Input)
		}
		if len(sort.OrderBy) != 1 || !sort.OrderBy[0].Desc || sort.OrderBy[0].Type != shared.TypeInteger {
			t.Fatalf("unexpected sort keys: %+v", sort.OrderBy)
		}

		if _, ok := sort.Input.(LogicalScan); !ok {
			t.Fatalf("expected sort input to be scan, got %T", sort.Input)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
