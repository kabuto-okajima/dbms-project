package executor

import (
	"errors"
	"path/filepath"
	"testing"

	"dbms-project/internal/binder"
	"dbms-project/internal/catalog"
	"dbms-project/internal/planner"
	"dbms-project/internal/shared"
	"dbms-project/internal/statement"
	"dbms-project/internal/storage"
)

func TestEvaluateScalarReadsBoundColumnByOrdinal(t *testing.T) {
	row := RuntimeRow{
		Values: storage.Row{
			storage.NewIntegerValue(1),
			storage.NewStringValue("Alice"),
		},
	}

	value, err := EvaluateScalar(binder.BoundColumnRef{
		TableName:  "students",
		ColumnName: "name",
		Ordinal:    1,
		Type:       shared.TypeString,
	}, row)
	if err != nil {
		t.Fatal(err)
	}
	if value != storage.NewStringValue("Alice") {
		t.Fatalf("expected Alice, got %+v", value)
	}
}

func TestEvaluateScalarReturnsLiteral(t *testing.T) {
	value, err := EvaluateScalar(binder.BoundLiteralExpr{
		Value: storage.NewIntegerValue(20),
	}, RuntimeRow{})
	if err != nil {
		t.Fatal(err)
	}
	if value != storage.NewIntegerValue(20) {
		t.Fatalf("expected 20, got %+v", value)
	}
}

func TestEvaluateScalarRejectsAggregateWithoutContext(t *testing.T) {
	_, err := EvaluateScalar(binder.BoundAggregateExpr{
		Function: statement.AggCount,
		Arg:      binder.BoundStarExpr{},
	}, RuntimeRow{})
	if err == nil {
		t.Fatal("expected aggregate evaluation without context to fail")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid definition error, got %v", err)
	}
}

func TestEvaluatePredicateComparisonSupportsIntegersAndStrings(t *testing.T) {
	row := RuntimeRow{
		Values: storage.Row{
			storage.NewIntegerValue(20),
			storage.NewStringValue("Bob"),
		},
	}

	intOK, err := EvaluatePredicate(binder.BoundComparisonExpr{
		Left: binder.BoundColumnRef{
			TableName:  "students",
			ColumnName: "age",
			Ordinal:    0,
			Type:       shared.TypeInteger,
		},
		Operator: statement.OpGreaterThanOrEqual,
		Right: binder.BoundLiteralExpr{
			Value: storage.NewIntegerValue(20),
		},
	}, row)
	if err != nil {
		t.Fatal(err)
	}
	if !intOK {
		t.Fatal("expected integer comparison to pass")
	}

	stringOK, err := EvaluatePredicate(binder.BoundComparisonExpr{
		Left: binder.BoundColumnRef{
			TableName:  "students",
			ColumnName: "name",
			Ordinal:    1,
			Type:       shared.TypeString,
		},
		Operator: statement.OpEqual,
		Right: binder.BoundLiteralExpr{
			Value: storage.NewStringValue("Bob"),
		},
	}, row)
	if err != nil {
		t.Fatal(err)
	}
	if !stringOK {
		t.Fatal("expected string comparison to pass")
	}
}

func TestEvaluatePredicateLogicalAndOr(t *testing.T) {
	row := RuntimeRow{
		Values: storage.Row{
			storage.NewIntegerValue(20),
			storage.NewStringValue("Bob"),
		},
	}

	andOK, err := EvaluatePredicate(binder.BoundLogicalExpr{
		Operator: statement.OpAnd,
		Terms: []binder.BoundExpression{
			binder.BoundComparisonExpr{
				Left: binder.BoundColumnRef{
					TableName:  "students",
					ColumnName: "age",
					Ordinal:    0,
					Type:       shared.TypeInteger,
				},
				Operator: statement.OpEqual,
				Right: binder.BoundLiteralExpr{
					Value: storage.NewIntegerValue(20),
				},
			},
			binder.BoundComparisonExpr{
				Left: binder.BoundColumnRef{
					TableName:  "students",
					ColumnName: "name",
					Ordinal:    1,
					Type:       shared.TypeString,
				},
				Operator: statement.OpEqual,
				Right: binder.BoundLiteralExpr{
					Value: storage.NewStringValue("Bob"),
				},
			},
		},
	}, row)
	if err != nil {
		t.Fatal(err)
	}
	if !andOK {
		t.Fatal("expected AND predicate to pass")
	}

	orOK, err := EvaluatePredicate(binder.BoundLogicalExpr{
		Operator: statement.OpOr,
		Terms: []binder.BoundExpression{
			binder.BoundComparisonExpr{
				Left: binder.BoundColumnRef{
					TableName:  "students",
					ColumnName: "age",
					Ordinal:    0,
					Type:       shared.TypeInteger,
				},
				Operator: statement.OpLessThan,
				Right: binder.BoundLiteralExpr{
					Value: storage.NewIntegerValue(18),
				},
			},
			binder.BoundComparisonExpr{
				Left: binder.BoundColumnRef{
					TableName:  "students",
					ColumnName: "name",
					Ordinal:    1,
					Type:       shared.TypeString,
				},
				Operator: statement.OpEqual,
				Right: binder.BoundLiteralExpr{
					Value: storage.NewStringValue("Bob"),
				},
			},
		},
	}, row)
	if err != nil {
		t.Fatal(err)
	}
	if !orOK {
		t.Fatal("expected OR predicate to pass")
	}
}

func TestEvaluatePredicateRejectsTypeMismatch(t *testing.T) {
	row := RuntimeRow{
		Values: storage.Row{
			storage.NewIntegerValue(20),
		},
	}

	_, err := EvaluatePredicate(binder.BoundComparisonExpr{
		Left: binder.BoundColumnRef{
			TableName:  "students",
			ColumnName: "age",
			Ordinal:    0,
			Type:       shared.TypeInteger,
		},
		Operator: statement.OpEqual,
		Right: binder.BoundLiteralExpr{
			Value: storage.NewStringValue("20"),
		},
	}, row)
	if err == nil {
		t.Fatal("expected type mismatch to fail")
	}
	if !errors.Is(err, shared.ErrTypeMismatch) {
		t.Fatalf("expected type mismatch error, got %v", err)
	}
}

func TestPhysicalTableScanExecuteReturnsSchemaAndRows(t *testing.T) {
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
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		row1, err := storage.EncodeRow(storage.Row{
			storage.NewIntegerValue(1),
			storage.NewStringValue("Alice"),
		})
		if err != nil {
			return err
		}
		row2, err := storage.EncodeRow(storage.Row{
			storage.NewIntegerValue(2),
			storage.NewStringValue("Bob"),
		})
		if err != nil {
			return err
		}

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}
		if err := tx.Put(table.TableBucket, storage.EncodeRID(1), row1); err != nil {
			return err
		}
		if err := tx.Put(table.TableBucket, storage.EncodeRID(2), row2); err != nil {
			return err
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

		result, err := PhysicalTableScan{
			Table: binder.BoundTable{
				Name:     "students",
				Metadata: table,
			},
		}.Execute(tx)
		if err != nil {
			return err
		}

		if len(result.Schema) != 2 {
			t.Fatalf("expected 2 columns, got %d", len(result.Schema))
		}
		if result.Schema[0].Name != "id" || result.Schema[0].Type != shared.TypeInteger {
			t.Fatalf("unexpected first schema column: %+v", result.Schema[0])
		}
		if result.Schema[1].Name != "name" || result.Schema[1].Type != shared.TypeString {
			t.Fatalf("unexpected second schema column: %+v", result.Schema[1])
		}

		if len(result.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(result.Rows))
		}
		if result.Rows[0].Values[0] != storage.NewIntegerValue(1) || result.Rows[0].Values[1] != storage.NewStringValue("Alice") {
			t.Fatalf("unexpected first row: %+v", result.Rows[0])
		}
		if result.Rows[1].Values[0] != storage.NewIntegerValue(2) || result.Rows[1].Values[1] != storage.NewStringValue("Bob") {
			t.Fatalf("unexpected second row: %+v", result.Rows[1])
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalTableScanExecuteReturnsSchemaForEmptyTable(t *testing.T) {
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
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		result, err := PhysicalTableScan{
			Table: binder.BoundTable{
				Name:     "students",
				Metadata: table,
			},
		}.Execute(tx)
		if err != nil {
			return err
		}

		if len(result.Schema) != 2 {
			t.Fatalf("expected 2 schema columns, got %d", len(result.Schema))
		}
		if len(result.Rows) != 0 {
			t.Fatalf("expected no rows, got %d", len(result.Rows))
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalIndexScanExecuteMatchesEquality(t *testing.T) {
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

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}
		indexMeta, err := manager.GetIndex(tx, "idx_students_age")
		if err != nil {
			return err
		}

		rows := []storage.Row{
			{storage.NewIntegerValue(1), storage.NewIntegerValue(19)},
			{storage.NewIntegerValue(2), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(3), storage.NewIntegerValue(20)},
		}
		insertExec := statement.NewInsertExecutor(manager)
		for _, row := range rows {
			if _, err := insertExec.Execute(tx, statement.InsertStatement{
				TableName: "students",
				Values:    row,
			}); err != nil {
				return err
			}
		}

		result, err := PhysicalIndexScan{
			Table:    binder.BoundTable{Name: "students", Metadata: table},
			Index:    *indexMeta,
			Column:   binder.BoundColumnRef{TableName: "students", ColumnName: "age", Ordinal: 1, Type: shared.TypeInteger},
			Operator: statement.OpEqual,
			Value:    storage.NewIntegerValue(20),
		}.Execute(tx)
		if err != nil {
			return err
		}

		if len(result.Rows) != 2 {
			t.Fatalf("expected 2 indexed rows, got %d", len(result.Rows))
		}
		if result.Rows[0].Values[0] != storage.NewIntegerValue(2) || result.Rows[1].Values[0] != storage.NewIntegerValue(3) {
			t.Fatalf("unexpected equality index-scan rows: %+v", result.Rows)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalIndexScanExecuteMatchesRange(t *testing.T) {
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

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}
		indexMeta, err := manager.GetIndex(tx, "idx_students_age")
		if err != nil {
			return err
		}

		insertExec := statement.NewInsertExecutor(manager)
		for _, row := range []storage.Row{
			{storage.NewIntegerValue(1), storage.NewIntegerValue(18)},
			{storage.NewIntegerValue(2), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(3), storage.NewIntegerValue(21)},
		} {
			if _, err := insertExec.Execute(tx, statement.InsertStatement{
				TableName: "students",
				Values:    row,
			}); err != nil {
				return err
			}
		}

		result, err := ExecutePlan(tx, PhysicalIndexScan{
			Table:    binder.BoundTable{Name: "students", Metadata: table},
			Index:    *indexMeta,
			Column:   binder.BoundColumnRef{TableName: "students", ColumnName: "age", Ordinal: 1, Type: shared.TypeInteger},
			Operator: statement.OpGreaterThanOrEqual,
			Value:    storage.NewIntegerValue(20),
		})
		if err != nil {
			return err
		}

		if len(result.Rows) != 2 {
			t.Fatalf("expected 2 range-index rows, got %d", len(result.Rows))
		}
		if result.Rows[0].Values[1] != storage.NewIntegerValue(20) || result.Rows[1].Values[1] != storage.NewIntegerValue(21) {
			t.Fatalf("unexpected range index-scan rows: %+v", result.Rows)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecutePlanUsesIndexScanResidualForAndPredicate(t *testing.T) {
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

		insertExec := statement.NewInsertExecutor(manager)
		for _, row := range []storage.Row{
			{storage.NewIntegerValue(1), storage.NewIntegerValue(19)},
			{storage.NewIntegerValue(2), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(3), storage.NewIntegerValue(20)},
		} {
			if _, err := insertExec.Execute(tx, statement.InsertStatement{
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

		result, err := ExecutePlan(tx, physical)
		if err != nil {
			return err
		}

		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row after residual filtering, got %d", len(result.Rows))
		}
		if result.Rows[0].Values[0] != storage.NewIntegerValue(3) || result.Rows[0].Values[1] != storage.NewIntegerValue(20) {
			t.Fatalf("unexpected residual-filtered rows: %+v", result.Rows)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalFilterExecuteKeepsMatchingRows(t *testing.T) {
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

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		row1, err := storage.EncodeRow(storage.Row{
			storage.NewIntegerValue(1),
			storage.NewIntegerValue(19),
		})
		if err != nil {
			return err
		}
		row2, err := storage.EncodeRow(storage.Row{
			storage.NewIntegerValue(2),
			storage.NewIntegerValue(20),
		})
		if err != nil {
			return err
		}
		row3, err := storage.EncodeRow(storage.Row{
			storage.NewIntegerValue(3),
			storage.NewIntegerValue(20),
		})
		if err != nil {
			return err
		}

		if err := tx.Put(table.TableBucket, storage.EncodeRID(1), row1); err != nil {
			return err
		}
		if err := tx.Put(table.TableBucket, storage.EncodeRID(2), row2); err != nil {
			return err
		}
		if err := tx.Put(table.TableBucket, storage.EncodeRID(3), row3); err != nil {
			return err
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

		result, err := PhysicalFilter{
			Input: PhysicalTableScan{
				Table: binder.BoundTable{
					Name:     "students",
					Metadata: table,
				},
			},
			Predicate: binder.BoundComparisonExpr{
				Left: binder.BoundColumnRef{
					TableName:  "students",
					ColumnName: "age",
					Ordinal:    1,
					Type:       shared.TypeInteger,
				},
				Operator: statement.OpEqual,
				Right: binder.BoundLiteralExpr{
					Value: storage.NewIntegerValue(20),
				},
			},
		}.Execute(tx)
		if err != nil {
			return err
		}

		if len(result.Schema) != 2 {
			t.Fatalf("expected 2 schema columns, got %d", len(result.Schema))
		}
		if len(result.Rows) != 2 {
			t.Fatalf("expected 2 matching rows, got %d", len(result.Rows))
		}
		if result.Rows[0].Values[0] != storage.NewIntegerValue(2) || result.Rows[1].Values[0] != storage.NewIntegerValue(3) {
			t.Fatalf("unexpected filtered rows: %+v", result.Rows)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalFilterExecuteReturnsEmptyRowsWhenNothingMatches(t *testing.T) {
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

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		row, err := storage.EncodeRow(storage.Row{
			storage.NewIntegerValue(1),
			storage.NewIntegerValue(19),
		})
		if err != nil {
			return err
		}
		return tx.Put(table.TableBucket, storage.EncodeRID(1), row)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		result, err := PhysicalFilter{
			Input: PhysicalTableScan{
				Table: binder.BoundTable{
					Name:     "students",
					Metadata: table,
				},
			},
			Predicate: binder.BoundComparisonExpr{
				Left: binder.BoundColumnRef{
					TableName:  "students",
					ColumnName: "age",
					Ordinal:    1,
					Type:       shared.TypeInteger,
				},
				Operator: statement.OpGreaterThan,
				Right: binder.BoundLiteralExpr{
					Value: storage.NewIntegerValue(30),
				},
			},
		}.Execute(tx)
		if err != nil {
			return err
		}

		if len(result.Schema) != 2 {
			t.Fatalf("expected 2 schema columns, got %d", len(result.Schema))
		}
		if len(result.Rows) != 0 {
			t.Fatalf("expected no matching rows, got %d", len(result.Rows))
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalProjectExecuteShapesFilteredOutput(t *testing.T) {
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
				{Name: "name", Type: shared.TypeString},
				{Name: "age", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rows := []storage.Row{
			{storage.NewIntegerValue(1), storage.NewStringValue("Alice"), storage.NewIntegerValue(19)},
			{storage.NewIntegerValue(2), storage.NewStringValue("Bob"), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(3), storage.NewStringValue("Carol"), storage.NewIntegerValue(20)},
		}
		for i, row := range rows {
			payload, err := storage.EncodeRow(row)
			if err != nil {
				return err
			}
			if err := tx.Put(table.TableBucket, storage.EncodeRID(storage.RID(i+1)), payload); err != nil {
				return err
			}
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

		result, err := ExecutePlan(tx, PhysicalProject{
			Input: PhysicalFilter{
				Input: PhysicalTableScan{
					Table: binder.BoundTable{
						Name:     "students",
						Metadata: table,
					},
				},
				Predicate: binder.BoundComparisonExpr{
					Left: binder.BoundColumnRef{
						TableName:  "students",
						ColumnName: "age",
						Ordinal:    2,
						Type:       shared.TypeInteger,
					},
					Operator: statement.OpEqual,
					Right: binder.BoundLiteralExpr{
						Value: storage.NewIntegerValue(20),
					},
				},
			},
			Outputs: []PhysicalOutput{
				{
					Name: "student_name",
					Expr: binder.BoundColumnRef{
						TableName:  "students",
						ColumnName: "name",
						Ordinal:    1,
						Type:       shared.TypeString,
					},
					Type: shared.TypeString,
				},
			},
		})
		if err != nil {
			return err
		}

		if len(result.Schema) != 1 {
			t.Fatalf("expected 1 schema column, got %d", len(result.Schema))
		}
		if result.Schema[0].Name != "student_name" || result.Schema[0].Type != shared.TypeString {
			t.Fatalf("unexpected projected schema: %+v", result.Schema[0])
		}
		if len(result.Rows) != 2 {
			t.Fatalf("expected 2 projected rows, got %d", len(result.Rows))
		}
		if len(result.Rows[0].Values) != 1 || len(result.Rows[1].Values) != 1 {
			t.Fatalf("expected 1 value per row, got %+v", result.Rows)
		}
		if result.Rows[0].Values[0] != storage.NewStringValue("Bob") || result.Rows[1].Values[0] != storage.NewStringValue("Carol") {
			t.Fatalf("unexpected projected rows: %+v", result.Rows)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalProjectExecuteExpandsStar(t *testing.T) {
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
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		payload, err := storage.EncodeRow(storage.Row{
			storage.NewIntegerValue(1),
			storage.NewStringValue("Alice"),
		})
		if err != nil {
			return err
		}

		return tx.Put(table.TableBucket, storage.EncodeRID(1), payload)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		result, err := ExecutePlan(tx, PhysicalProject{
			Input: PhysicalTableScan{
				Table: binder.BoundTable{
					Name:     "students",
					Metadata: table,
				},
			},
			Outputs: []PhysicalOutput{
				{
					Name:        "*",
					Expr:        binder.BoundStarExpr{},
					ExpandsStar: true,
				},
			},
		})
		if err != nil {
			return err
		}

		if len(result.Schema) != 2 {
			t.Fatalf("expected 2 schema columns, got %d", len(result.Schema))
		}
		if len(result.Rows) != 1 || len(result.Rows[0].Values) != 2 {
			t.Fatalf("unexpected star-projected rows: %+v", result.Rows)
		}
		if result.Rows[0].Values[0] != storage.NewIntegerValue(1) || result.Rows[0].Values[1] != storage.NewStringValue("Alice") {
			t.Fatalf("unexpected star-projected row: %+v", result.Rows[0])
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalSortExecuteOrdersAscending(t *testing.T) {
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
				{Name: "name", Type: shared.TypeString},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rows := []storage.Row{
			{storage.NewIntegerValue(2), storage.NewStringValue("Bob")},
			{storage.NewIntegerValue(1), storage.NewStringValue("Alice")},
			{storage.NewIntegerValue(3), storage.NewStringValue("Carol")},
		}
		for i, row := range rows {
			payload, err := storage.EncodeRow(row)
			if err != nil {
				return err
			}
			if err := tx.Put(table.TableBucket, storage.EncodeRID(storage.RID(i+1)), payload); err != nil {
				return err
			}
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

		result, err := ExecutePlan(tx, PhysicalSort{
			Input: PhysicalTableScan{
				Table: binder.BoundTable{
					Name:     "students",
					Metadata: table,
				},
			},
			OrderBy: []PhysicalSortKey{
				{
					Expr: binder.BoundColumnRef{
						TableName:  "students",
						ColumnName: "name",
						Ordinal:    1,
						Type:       shared.TypeString,
					},
					Type: shared.TypeString,
				},
			},
		})
		if err != nil {
			return err
		}

		if len(result.Rows) != 3 {
			t.Fatalf("expected 3 sorted rows, got %d", len(result.Rows))
		}
		if result.Rows[0].Values[1] != storage.NewStringValue("Alice") ||
			result.Rows[1].Values[1] != storage.NewStringValue("Bob") ||
			result.Rows[2].Values[1] != storage.NewStringValue("Carol") {
			t.Fatalf("unexpected ascending order: %+v", result.Rows)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalSortExecuteOrdersDescendingAfterFilter(t *testing.T) {
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

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rows := []storage.Row{
			{storage.NewIntegerValue(1), storage.NewIntegerValue(18)},
			{storage.NewIntegerValue(2), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(3), storage.NewIntegerValue(19)},
		}
		for i, row := range rows {
			payload, err := storage.EncodeRow(row)
			if err != nil {
				return err
			}
			if err := tx.Put(table.TableBucket, storage.EncodeRID(storage.RID(i+1)), payload); err != nil {
				return err
			}
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

		result, err := ExecutePlan(tx, PhysicalSort{
			Input: PhysicalFilter{
				Input: PhysicalTableScan{
					Table: binder.BoundTable{
						Name:     "students",
						Metadata: table,
					},
				},
				Predicate: binder.BoundComparisonExpr{
					Left: binder.BoundColumnRef{
						TableName:  "students",
						ColumnName: "age",
						Ordinal:    1,
						Type:       shared.TypeInteger,
					},
					Operator: statement.OpGreaterThan,
					Right: binder.BoundLiteralExpr{
						Value: storage.NewIntegerValue(18),
					},
				},
			},
			OrderBy: []PhysicalSortKey{
				{
					Expr: binder.BoundColumnRef{
						TableName:  "students",
						ColumnName: "age",
						Ordinal:    1,
						Type:       shared.TypeInteger,
					},
					Desc: true,
					Type: shared.TypeInteger,
				},
			},
		})
		if err != nil {
			return err
		}

		if len(result.Rows) != 2 {
			t.Fatalf("expected 2 sorted rows, got %d", len(result.Rows))
		}
		if result.Rows[0].Values[1] != storage.NewIntegerValue(20) ||
			result.Rows[1].Values[1] != storage.NewIntegerValue(19) {
			t.Fatalf("unexpected descending order after filter: %+v", result.Rows)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalAggregateExecuteCountsRows(t *testing.T) {
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
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		for i := 1; i <= 3; i++ {
			payload, err := storage.EncodeRow(storage.Row{
				storage.NewIntegerValue(int64(i)),
			})
			if err != nil {
				return err
			}
			if err := tx.Put(table.TableBucket, storage.EncodeRID(storage.RID(i)), payload); err != nil {
				return err
			}
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

		result, err := PhysicalAggregate{
			Input: PhysicalTableScan{
				Table: binder.BoundTable{
					Name:     "students",
					Metadata: table,
				},
			},
			Aggregates: []binder.BoundAggregateExpr{
				{
					Function: statement.AggCount,
					Arg:      binder.BoundStarExpr{},
				},
			},
		}.Execute(tx)
		if err != nil {
			return err
		}

		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 aggregate row, got %d", len(result.Rows))
		}

		value, err := EvaluateScalar(binder.BoundAggregateExpr{
			Function: statement.AggCount,
			Arg:      binder.BoundStarExpr{},
		}, result.Rows[0])
		if err != nil {
			return err
		}
		if value != storage.NewIntegerValue(3) {
			t.Fatalf("expected COUNT(*) = 3, got %+v", value)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecutePlanProjectsCountAggregate(t *testing.T) {
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
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		for i := 1; i <= 2; i++ {
			payload, err := storage.EncodeRow(storage.Row{
				storage.NewIntegerValue(int64(i)),
			})
			if err != nil {
				return err
			}
			if err := tx.Put(table.TableBucket, storage.EncodeRID(storage.RID(i)), payload); err != nil {
				return err
			}
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

		result, err := ExecutePlan(tx, PhysicalProject{
			Input: PhysicalAggregate{
				Input: PhysicalTableScan{
					Table: binder.BoundTable{
						Name:     "students",
						Metadata: table,
					},
				},
				Aggregates: []binder.BoundAggregateExpr{
					{
						Function: statement.AggCount,
						Arg:      binder.BoundStarExpr{},
					},
				},
			},
			Outputs: []PhysicalOutput{
				{
					Name: "COUNT(*)",
					Expr: binder.BoundAggregateExpr{
						Function: statement.AggCount,
						Arg:      binder.BoundStarExpr{},
					},
					Type: shared.TypeInteger,
				},
			},
		})
		if err != nil {
			return err
		}

		if len(result.Schema) != 1 || result.Schema[0].Name != "COUNT(*)" {
			t.Fatalf("unexpected aggregate projection schema: %+v", result.Schema)
		}
		if len(result.Rows) != 1 || len(result.Rows[0].Values) != 1 {
			t.Fatalf("unexpected aggregate projection rows: %+v", result.Rows)
		}
		if result.Rows[0].Values[0] != storage.NewIntegerValue(2) {
			t.Fatalf("expected projected COUNT(*) = 2, got %+v", result.Rows[0].Values[0])
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalAggregateExecuteCountsEmptyInputAsZero(t *testing.T) {
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
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		result, err := PhysicalAggregate{
			Input: PhysicalTableScan{
				Table: binder.BoundTable{
					Name:     "students",
					Metadata: table,
				},
			},
			Aggregates: []binder.BoundAggregateExpr{
				{
					Function: statement.AggCount,
					Arg:      binder.BoundStarExpr{},
				},
			},
		}.Execute(tx)
		if err != nil {
			return err
		}

		value, err := EvaluateScalar(binder.BoundAggregateExpr{
			Function: statement.AggCount,
			Arg:      binder.BoundStarExpr{},
		}, result.Rows[0])
		if err != nil {
			return err
		}
		if value != storage.NewIntegerValue(0) {
			t.Fatalf("expected COUNT(*) = 0, got %+v", value)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecutePlanSupportsHavingOnCountAggregate(t *testing.T) {
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
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		for i := 1; i <= 3; i++ {
			payload, err := storage.EncodeRow(storage.Row{
				storage.NewIntegerValue(int64(i)),
			})
			if err != nil {
				return err
			}
			if err := tx.Put(table.TableBucket, storage.EncodeRID(storage.RID(i)), payload); err != nil {
				return err
			}
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

		result, err := ExecutePlan(tx, PhysicalProject{
			Input: PhysicalFilter{
				Input: PhysicalAggregate{
					Input: PhysicalTableScan{
						Table: binder.BoundTable{
							Name:     "students",
							Metadata: table,
						},
					},
					Aggregates: []binder.BoundAggregateExpr{
						{
							Function: statement.AggCount,
							Arg:      binder.BoundStarExpr{},
						},
					},
				},
				Predicate: binder.BoundComparisonExpr{
					Left: binder.BoundAggregateExpr{
						Function: statement.AggCount,
						Arg:      binder.BoundStarExpr{},
					},
					Operator: statement.OpGreaterThan,
					Right: binder.BoundLiteralExpr{
						Value: storage.NewIntegerValue(1),
					},
				},
			},
			Outputs: []PhysicalOutput{
				{
					Name: "COUNT(*)",
					Expr: binder.BoundAggregateExpr{
						Function: statement.AggCount,
						Arg:      binder.BoundStarExpr{},
					},
					Type: shared.TypeInteger,
				},
			},
		})
		if err != nil {
			return err
		}

		// fmt.Printf("HAVING result: %+v\n", result)

		if len(result.Rows) != 1 || len(result.Rows[0].Values) != 1 {
			t.Fatalf("expected one projected HAVING row, got %+v", result.Rows)
		}
		if result.Rows[0].Values[0] != storage.NewIntegerValue(3) {
			t.Fatalf("expected COUNT(*) = 3 after HAVING, got %+v", result.Rows[0].Values[0])
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecutePlanHavingCanFilterOutAggregateRow(t *testing.T) {
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
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		payload, err := storage.EncodeRow(storage.Row{
			storage.NewIntegerValue(1),
		})
		if err != nil {
			return err
		}
		return tx.Put(table.TableBucket, storage.EncodeRID(1), payload)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		result, err := ExecutePlan(tx, PhysicalProject{
			Input: PhysicalFilter{
				Input: PhysicalAggregate{
					Input: PhysicalTableScan{
						Table: binder.BoundTable{
							Name:     "students",
							Metadata: table,
						},
					},
					Aggregates: []binder.BoundAggregateExpr{
						{
							Function: statement.AggCount,
							Arg:      binder.BoundStarExpr{},
						},
					},
				},
				Predicate: binder.BoundComparisonExpr{
					Left: binder.BoundAggregateExpr{
						Function: statement.AggCount,
						Arg:      binder.BoundStarExpr{},
					},
					Operator: statement.OpGreaterThan,
					Right: binder.BoundLiteralExpr{
						Value: storage.NewIntegerValue(5),
					},
				},
			},
			Outputs: []PhysicalOutput{
				{
					Name: "COUNT(*)",
					Expr: binder.BoundAggregateExpr{
						Function: statement.AggCount,
						Arg:      binder.BoundStarExpr{},
					},
					Type: shared.TypeInteger,
				},
			},
		})
		if err != nil {
			return err
		}

		if len(result.Rows) != 0 {
			t.Fatalf("expected HAVING to remove aggregate row, got %+v", result.Rows)
		}
		if len(result.Schema) != 1 || result.Schema[0].Name != "COUNT(*)" {
			t.Fatalf("unexpected projected schema after HAVING: %+v", result.Schema)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalAggregateExecuteSupportsSumMinMax(t *testing.T) {
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

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rows := []storage.Row{
			{storage.NewIntegerValue(1), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(2), storage.NewIntegerValue(18)},
			{storage.NewIntegerValue(3), storage.NewIntegerValue(25)},
		}
		for i, row := range rows {
			payload, err := storage.EncodeRow(row)
			if err != nil {
				return err
			}
			if err := tx.Put(table.TableBucket, storage.EncodeRID(storage.RID(i+1)), payload); err != nil {
				return err
			}
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

		result, err := PhysicalAggregate{
			Input: PhysicalTableScan{
				Table: binder.BoundTable{
					Name:     "students",
					Metadata: table,
				},
			},
			Aggregates: []binder.BoundAggregateExpr{
				{
					Function: statement.AggSum,
					Arg: binder.BoundColumnRef{
						TableName:  "students",
						ColumnName: "age",
						Ordinal:    1,
						Type:       shared.TypeInteger,
					},
				},
				{
					Function: statement.AggMin,
					Arg: binder.BoundColumnRef{
						TableName:  "students",
						ColumnName: "age",
						Ordinal:    1,
						Type:       shared.TypeInteger,
					},
				},
				{
					Function: statement.AggMax,
					Arg: binder.BoundColumnRef{
						TableName:  "students",
						ColumnName: "age",
						Ordinal:    1,
						Type:       shared.TypeInteger,
					},
				},
			},
		}.Execute(tx)
		if err != nil {
			return err
		}

		row := result.Rows[0]

		sumValue, err := EvaluateScalar(binder.BoundAggregateExpr{
			Function: statement.AggSum,
			Arg: binder.BoundColumnRef{
				TableName:  "students",
				ColumnName: "age",
				Ordinal:    1,
				Type:       shared.TypeInteger,
			},
		}, row)
		if err != nil {
			return err
		}
		if sumValue != storage.NewIntegerValue(63) {
			t.Fatalf("expected SUM(age) = 63, got %+v", sumValue)
		}

		minValue, err := EvaluateScalar(binder.BoundAggregateExpr{
			Function: statement.AggMin,
			Arg: binder.BoundColumnRef{
				TableName:  "students",
				ColumnName: "age",
				Ordinal:    1,
				Type:       shared.TypeInteger,
			},
		}, row)
		if err != nil {
			return err
		}
		if minValue != storage.NewIntegerValue(18) {
			t.Fatalf("expected MIN(age) = 18, got %+v", minValue)
		}

		maxValue, err := EvaluateScalar(binder.BoundAggregateExpr{
			Function: statement.AggMax,
			Arg: binder.BoundColumnRef{
				TableName:  "students",
				ColumnName: "age",
				Ordinal:    1,
				Type:       shared.TypeInteger,
			},
		}, row)
		if err != nil {
			return err
		}
		if maxValue != storage.NewIntegerValue(25) {
			t.Fatalf("expected MAX(age) = 25, got %+v", maxValue)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecutePlanProjectsSumAndUsesHaving(t *testing.T) {
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

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rows := []storage.Row{
			{storage.NewIntegerValue(1), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(2), storage.NewIntegerValue(22)},
		}
		for i, row := range rows {
			payload, err := storage.EncodeRow(row)
			if err != nil {
				return err
			}
			if err := tx.Put(table.TableBucket, storage.EncodeRID(storage.RID(i+1)), payload); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	sumExpr := binder.BoundAggregateExpr{
		Function: statement.AggSum,
		Arg: binder.BoundColumnRef{
			TableName:  "students",
			ColumnName: "age",
			Ordinal:    1,
			Type:       shared.TypeInteger,
		},
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		result, err := ExecutePlan(tx, PhysicalProject{
			Input: PhysicalFilter{
				Input: PhysicalAggregate{
					Input: PhysicalTableScan{
						Table: binder.BoundTable{
							Name:     "students",
							Metadata: table,
						},
					},
					Aggregates: []binder.BoundAggregateExpr{sumExpr},
				},
				Predicate: binder.BoundComparisonExpr{
					Left:     sumExpr,
					Operator: statement.OpGreaterThan,
					Right: binder.BoundLiteralExpr{
						Value: storage.NewIntegerValue(40),
					},
				},
			},
			Outputs: []PhysicalOutput{
				{
					Name: "SUM(age)",
					Expr: sumExpr,
					Type: shared.TypeInteger,
				},
			},
		})
		if err != nil {
			return err
		}

		if len(result.Schema) != 1 || result.Schema[0].Name != "SUM(age)" {
			t.Fatalf("unexpected SUM(age) schema: %+v", result.Schema)
		}
		if len(result.Rows) != 1 || len(result.Rows[0].Values) != 1 {
			t.Fatalf("unexpected SUM(age) rows: %+v", result.Rows)
		}
		if result.Rows[0].Values[0] != storage.NewIntegerValue(42) {
			t.Fatalf("expected projected SUM(age) = 42, got %+v", result.Rows[0].Values[0])
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalAggregateExecuteReturnsZeroValuesOnEmptyInput(t *testing.T) {
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
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		result, err := PhysicalAggregate{
			Input: PhysicalTableScan{
				Table: binder.BoundTable{
					Name:     "students",
					Metadata: table,
				},
			},
			Aggregates: []binder.BoundAggregateExpr{
				{
					Function: statement.AggSum,
					Arg: binder.BoundColumnRef{
						TableName:  "students",
						ColumnName: "age",
						Ordinal:    1,
						Type:       shared.TypeInteger,
					},
				},
				{
					Function: statement.AggMin,
					Arg: binder.BoundColumnRef{
						TableName:  "students",
						ColumnName: "age",
						Ordinal:    1,
						Type:       shared.TypeInteger,
					},
				},
				{
					Function: statement.AggMax,
					Arg: binder.BoundColumnRef{
						TableName:  "students",
						ColumnName: "age",
						Ordinal:    1,
						Type:       shared.TypeInteger,
					},
				},
			},
		}.Execute(tx)
		if err != nil {
			return err
		}

		row := result.Rows[0]
		for _, expr := range []binder.BoundAggregateExpr{
			{
				Function: statement.AggSum,
				Arg: binder.BoundColumnRef{
					TableName:  "students",
					ColumnName: "age",
					Ordinal:    1,
					Type:       shared.TypeInteger,
				},
			},
			{
				Function: statement.AggMin,
				Arg: binder.BoundColumnRef{
					TableName:  "students",
					ColumnName: "age",
					Ordinal:    1,
					Type:       shared.TypeInteger,
				},
			},
			{
				Function: statement.AggMax,
				Arg: binder.BoundColumnRef{
					TableName:  "students",
					ColumnName: "age",
					Ordinal:    1,
					Type:       shared.TypeInteger,
				},
			},
		} {
			value, err := EvaluateScalar(expr, row)
			if err != nil {
				return err
			}
			if value != storage.NewIntegerValue(0) {
				t.Fatalf("expected empty aggregate %s to be 0, got %+v", expr.Function, value)
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalAggregateExecuteGroupsByOneColumnWithCount(t *testing.T) {
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
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rows := []storage.Row{
			{storage.NewIntegerValue(1), storage.NewIntegerValue(10)},
			{storage.NewIntegerValue(2), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(3), storage.NewIntegerValue(10)},
		}
		for i, row := range rows {
			payload, err := storage.EncodeRow(row)
			if err != nil {
				return err
			}
			if err := tx.Put(table.TableBucket, storage.EncodeRID(storage.RID(i+1)), payload); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	groupExpr := binder.BoundColumnRef{
		TableName:  "students",
		ColumnName: "dept_id",
		Ordinal:    1,
		Type:       shared.TypeInteger,
	}
	countExpr := binder.BoundAggregateExpr{
		Function: statement.AggCount,
		Arg:      binder.BoundStarExpr{},
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		result, err := PhysicalAggregate{
			Input: PhysicalTableScan{
				Table: binder.BoundTable{
					Name:     "students",
					Metadata: table,
				},
			},
			GroupBy:    []binder.BoundExpression{groupExpr},
			Aggregates: []binder.BoundAggregateExpr{countExpr},
		}.Execute(tx)
		if err != nil {
			return err
		}

		if len(result.Rows) != 2 {
			t.Fatalf("expected 2 grouped rows, got %d", len(result.Rows))
		}

		group1, err := EvaluateScalar(groupExpr, result.Rows[0])
		if err != nil {
			return err
		}
		count1, err := EvaluateScalar(countExpr, result.Rows[0])
		if err != nil {
			return err
		}
		group2, err := EvaluateScalar(groupExpr, result.Rows[1])
		if err != nil {
			return err
		}
		count2, err := EvaluateScalar(countExpr, result.Rows[1])
		if err != nil {
			return err
		}

		if group1 != storage.NewIntegerValue(10) || count1 != storage.NewIntegerValue(2) {
			t.Fatalf("unexpected first group row: group=%+v count=%+v", group1, count1)
		}
		if group2 != storage.NewIntegerValue(20) || count2 != storage.NewIntegerValue(1) {
			t.Fatalf("unexpected second group row: group=%+v count=%+v", group2, count2)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecutePlanProjectsGroupedCountWithHaving(t *testing.T) {
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
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rows := []storage.Row{
			{storage.NewIntegerValue(1), storage.NewIntegerValue(10)},
			{storage.NewIntegerValue(2), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(3), storage.NewIntegerValue(10)},
		}
		for i, row := range rows {
			payload, err := storage.EncodeRow(row)
			if err != nil {
				return err
			}
			if err := tx.Put(table.TableBucket, storage.EncodeRID(storage.RID(i+1)), payload); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	groupExpr := binder.BoundColumnRef{
		TableName:  "students",
		ColumnName: "dept_id",
		Ordinal:    1,
		Type:       shared.TypeInteger,
	}
	countExpr := binder.BoundAggregateExpr{
		Function: statement.AggCount,
		Arg:      binder.BoundStarExpr{},
	}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		result, err := ExecutePlan(tx, PhysicalProject{
			Input: PhysicalFilter{
				Input: PhysicalAggregate{
					Input: PhysicalTableScan{
						Table: binder.BoundTable{
							Name:     "students",
							Metadata: table,
						},
					},
					GroupBy:    []binder.BoundExpression{groupExpr},
					Aggregates: []binder.BoundAggregateExpr{countExpr},
				},
				Predicate: binder.BoundComparisonExpr{
					Left:     countExpr,
					Operator: statement.OpGreaterThan,
					Right: binder.BoundLiteralExpr{
						Value: storage.NewIntegerValue(1),
					},
				},
			},
			Outputs: []PhysicalOutput{
				{
					Name: "dept_id",
					Expr: groupExpr,
					Type: shared.TypeInteger,
				},
				{
					Name: "COUNT(*)",
					Expr: countExpr,
					Type: shared.TypeInteger,
				},
			},
		})
		if err != nil {
			return err
		}

		if len(result.Rows) != 1 || len(result.Rows[0].Values) != 2 {
			t.Fatalf("unexpected grouped projection rows: %+v", result.Rows)
		}
		if result.Rows[0].Values[0] != storage.NewIntegerValue(10) {
			t.Fatalf("expected surviving dept_id = 10, got %+v", result.Rows[0].Values[0])
		}
		if result.Rows[0].Values[1] != storage.NewIntegerValue(2) {
			t.Fatalf("expected surviving COUNT(*) = 2, got %+v", result.Rows[0].Values[1])
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalAggregateExecuteGroupsByOneColumnWithSumMinMax(t *testing.T) {
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
				{Name: "age", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rows := []storage.Row{
			{storage.NewIntegerValue(1), storage.NewIntegerValue(10), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(2), storage.NewIntegerValue(20), storage.NewIntegerValue(18)},
			{storage.NewIntegerValue(3), storage.NewIntegerValue(10), storage.NewIntegerValue(25)},
		}
		for i, row := range rows {
			payload, err := storage.EncodeRow(row)
			if err != nil {
				return err
			}
			if err := tx.Put(table.TableBucket, storage.EncodeRID(storage.RID(i+1)), payload); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	groupExpr := binder.BoundColumnRef{
		TableName:  "students",
		ColumnName: "dept_id",
		Ordinal:    1,
		Type:       shared.TypeInteger,
	}
	ageExpr := binder.BoundColumnRef{
		TableName:  "students",
		ColumnName: "age",
		Ordinal:    2,
		Type:       shared.TypeInteger,
	}
	sumExpr := binder.BoundAggregateExpr{Function: statement.AggSum, Arg: ageExpr}
	minExpr := binder.BoundAggregateExpr{Function: statement.AggMin, Arg: ageExpr}
	maxExpr := binder.BoundAggregateExpr{Function: statement.AggMax, Arg: ageExpr}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		result, err := PhysicalAggregate{
			Input: PhysicalTableScan{
				Table: binder.BoundTable{Name: "students", Metadata: table},
			},
			GroupBy:    []binder.BoundExpression{groupExpr},
			Aggregates: []binder.BoundAggregateExpr{sumExpr, minExpr, maxExpr},
		}.Execute(tx)
		if err != nil {
			return err
		}

		if len(result.Rows) != 2 {
			t.Fatalf("expected 2 grouped rows, got %d", len(result.Rows))
		}

		group1, _ := EvaluateScalar(groupExpr, result.Rows[0])
		sum1, _ := EvaluateScalar(sumExpr, result.Rows[0])
		min1, _ := EvaluateScalar(minExpr, result.Rows[0])
		max1, _ := EvaluateScalar(maxExpr, result.Rows[0])
		group2, _ := EvaluateScalar(groupExpr, result.Rows[1])
		sum2, _ := EvaluateScalar(sumExpr, result.Rows[1])
		min2, _ := EvaluateScalar(minExpr, result.Rows[1])
		max2, _ := EvaluateScalar(maxExpr, result.Rows[1])

		if group1 != storage.NewIntegerValue(10) || sum1 != storage.NewIntegerValue(45) || min1 != storage.NewIntegerValue(20) || max1 != storage.NewIntegerValue(25) {
			t.Fatalf("unexpected first grouped row: group=%+v sum=%+v min=%+v max=%+v", group1, sum1, min1, max1)
		}
		if group2 != storage.NewIntegerValue(20) || sum2 != storage.NewIntegerValue(18) || min2 != storage.NewIntegerValue(18) || max2 != storage.NewIntegerValue(18) {
			t.Fatalf("unexpected second grouped row: group=%+v sum=%+v min=%+v max=%+v", group2, sum2, min2, max2)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecuteSelectRunsSingleTablePipeline(t *testing.T) {
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
				{Name: "name", Type: shared.TypeString},
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

		insertExec := statement.NewInsertExecutor(manager)
		for _, row := range []storage.Row{
			{storage.NewIntegerValue(1), storage.NewStringValue("Alice"), storage.NewIntegerValue(19)},
			{storage.NewIntegerValue(2), storage.NewStringValue("Bob"), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(3), storage.NewStringValue("Carol"), storage.NewIntegerValue(20)},
		} {
			if _, err := insertExec.Execute(tx, statement.InsertStatement{
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

	err = store.View(func(tx *storage.Tx) error {
		result, err := ExecuteSelect(tx, manager, statement.SelectStatement{
			SelectItems: []statement.SelectItem{
				{Expr: statement.ColumnRef{ColumnName: "name"}},
			},
			From: []statement.TableRef{
				{Name: "students"},
			},
			Where: statement.ComparisonExpr{
				Left:     statement.ColumnRef{ColumnName: "age"},
				Operator: statement.OpEqual,
				Right:    statement.LiteralExpr{Value: storage.NewIntegerValue(20)},
			},
			OrderBy: []statement.OrderByTerm{
				{Expr: statement.ColumnRef{ColumnName: "name"}, Desc: true},
			},
		})
		if err != nil {
			return err
		}

		if len(result.Schema) != 1 || result.Schema[0].Name != "name" {
			t.Fatalf("unexpected ExecuteSelect schema: %+v", result.Schema)
		}
		if len(result.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(result.Rows))
		}
		if result.Rows[0].Values[0] != storage.NewStringValue("Carol") || result.Rows[1].Values[0] != storage.NewStringValue("Bob") {
			t.Fatalf("unexpected ExecuteSelect rows: %+v", result.Rows)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecuteSelectRunsGroupedHavingPipeline(t *testing.T) {
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
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		insertExec := statement.NewInsertExecutor(manager)
		for _, row := range []storage.Row{
			{storage.NewIntegerValue(1), storage.NewIntegerValue(10)},
			{storage.NewIntegerValue(2), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(3), storage.NewIntegerValue(10)},
		} {
			if _, err := insertExec.Execute(tx, statement.InsertStatement{
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

	err = store.View(func(tx *storage.Tx) error {
		result, err := ExecuteSelect(tx, manager, statement.SelectStatement{
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

		if len(result.Schema) != 2 {
			t.Fatalf("unexpected grouped schema: %+v", result.Schema)
		}
		if len(result.Rows) != 1 || len(result.Rows[0].Values) != 2 {
			t.Fatalf("unexpected grouped rows: %+v", result.Rows)
		}
		if result.Rows[0].Values[0] != storage.NewIntegerValue(10) || result.Rows[0].Values[1] != storage.NewIntegerValue(2) {
			t.Fatalf("unexpected grouped ExecuteSelect row: %+v", result.Rows[0])
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecutePlanProjectsGroupedSumWithHaving(t *testing.T) {
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
				{Name: "age", Type: shared.TypeInteger},
			},
			PrimaryKey: []string{"id"},
		}); err != nil {
			return err
		}

		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		rows := []storage.Row{
			{storage.NewIntegerValue(1), storage.NewIntegerValue(10), storage.NewIntegerValue(20)},
			{storage.NewIntegerValue(2), storage.NewIntegerValue(20), storage.NewIntegerValue(18)},
			{storage.NewIntegerValue(3), storage.NewIntegerValue(10), storage.NewIntegerValue(25)},
		}
		for i, row := range rows {
			payload, err := storage.EncodeRow(row)
			if err != nil {
				return err
			}
			if err := tx.Put(table.TableBucket, storage.EncodeRID(storage.RID(i+1)), payload); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	groupExpr := binder.BoundColumnRef{
		TableName:  "students",
		ColumnName: "dept_id",
		Ordinal:    1,
		Type:       shared.TypeInteger,
	}
	ageExpr := binder.BoundColumnRef{
		TableName:  "students",
		ColumnName: "age",
		Ordinal:    2,
		Type:       shared.TypeInteger,
	}
	sumExpr := binder.BoundAggregateExpr{Function: statement.AggSum, Arg: ageExpr}

	err = store.View(func(tx *storage.Tx) error {
		table, err := manager.GetTable(tx, "students")
		if err != nil {
			return err
		}

		result, err := ExecutePlan(tx, PhysicalProject{
			Input: PhysicalFilter{
				Input: PhysicalAggregate{
					Input: PhysicalTableScan{
						Table: binder.BoundTable{Name: "students", Metadata: table},
					},
					GroupBy:    []binder.BoundExpression{groupExpr},
					Aggregates: []binder.BoundAggregateExpr{sumExpr},
				},
				Predicate: binder.BoundComparisonExpr{
					Left:     sumExpr,
					Operator: statement.OpGreaterThan,
					Right: binder.BoundLiteralExpr{
						Value: storage.NewIntegerValue(30),
					},
				},
			},
			Outputs: []PhysicalOutput{
				{Name: "dept_id", Expr: groupExpr, Type: shared.TypeInteger},
				{Name: "SUM(age)", Expr: sumExpr, Type: shared.TypeInteger},
			},
		})
		if err != nil {
			return err
		}

		if len(result.Rows) != 1 || len(result.Rows[0].Values) != 2 {
			t.Fatalf("unexpected grouped SUM rows: %+v", result.Rows)
		}
		if result.Rows[0].Values[0] != storage.NewIntegerValue(10) {
			t.Fatalf("expected surviving dept_id = 10, got %+v", result.Rows[0].Values[0])
		}
		if result.Rows[0].Values[1] != storage.NewIntegerValue(45) {
			t.Fatalf("expected surviving SUM(age) = 45, got %+v", result.Rows[0].Values[1])
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
