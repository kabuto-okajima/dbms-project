package planner

import (
	"testing"

	"dbms-project/internal/binder"
)

func TestOptimizeRemovesEmptySort(t *testing.T) {
	plan := LogicalSort{
		Input:   LogicalScan{},
		OrderBy: nil,
	}

	optimized := Optimize(plan)

	if _, ok := optimized.(LogicalScan); !ok {
		t.Fatalf("expected empty sort to be removed, got %T", optimized)
	}
}

func TestOptimizeRemovesFilterWithoutPredicate(t *testing.T) {
	plan := LogicalFilter{
		Input:     LogicalScan{},
		Predicate: nil,
	}

	optimized := Optimize(plan)

	if _, ok := optimized.(LogicalScan); !ok {
		t.Fatalf("expected empty filter to be removed, got %T", optimized)
	}
}

func TestOptimizeCombinesAdjacentFilters(t *testing.T) {
	plan := LogicalFilter{
		Input: LogicalFilter{
			Input: LogicalScan{},
			Predicate: binder.BoundComparisonExpr{
				Operator: "=",
			},
		},
		Predicate: binder.BoundComparisonExpr{
			Operator: ">",
		},
	}

	optimized := Optimize(plan)

	filter, ok := optimized.(LogicalFilter)
	if !ok {
		t.Fatalf("expected optimized plan to remain a filter, got %T", optimized)
	}
	if _, ok := filter.Input.(LogicalScan); !ok {
		t.Fatalf("expected merged filter input to be scan, got %T", filter.Input)
	}

	logicalExpr, ok := filter.Predicate.(binder.BoundLogicalExpr)
	if !ok {
		t.Fatalf("expected merged predicate to be AND logical expr, got %T", filter.Predicate)
	}
	if logicalExpr.Operator != "AND" || len(logicalExpr.Terms) != 2 {
		t.Fatalf("unexpected merged predicate: %+v", logicalExpr)
	}
}

func TestOptimizeFlattensNestedAndPredicate(t *testing.T) {
	plan := LogicalFilter{
		Input: LogicalScan{},
		Predicate: binder.BoundLogicalExpr{
			Operator: "AND",
			Terms: []binder.BoundExpression{
				binder.BoundComparisonExpr{Operator: "="},
				binder.BoundLogicalExpr{
					Operator: "AND",
					Terms: []binder.BoundExpression{
						binder.BoundComparisonExpr{Operator: ">"},
						binder.BoundComparisonExpr{Operator: "<"},
					},
				},
			},
		},
	}

	optimized := Optimize(plan)

	filter, ok := optimized.(LogicalFilter)
	if !ok {
		t.Fatalf("expected optimized plan to remain a filter, got %T", optimized)
	}

	logicalExpr, ok := filter.Predicate.(binder.BoundLogicalExpr)
	if !ok {
		t.Fatalf("expected flattened predicate to be AND logical expr, got %T", filter.Predicate)
	}
	if logicalExpr.Operator != "AND" || len(logicalExpr.Terms) != 3 {
		t.Fatalf("unexpected flattened predicate: %+v", logicalExpr)
	}
}

func TestOptimizeCollapsesSingleTermAndPredicate(t *testing.T) {
	onlyTerm := binder.BoundComparisonExpr{Operator: "="}
	plan := LogicalFilter{
		Input: LogicalScan{},
		Predicate: binder.BoundLogicalExpr{
			Operator: "AND",
			Terms:    []binder.BoundExpression{onlyTerm},
		},
	}

	optimized := Optimize(plan)

	filter, ok := optimized.(LogicalFilter)
	if !ok {
		t.Fatalf("expected optimized plan to remain a filter, got %T", optimized)
	}
	if _, ok := filter.Predicate.(binder.BoundLogicalExpr); ok {
		t.Fatalf("expected single-term AND to collapse, got %+v", filter.Predicate)
	}
	if comparison, ok := filter.Predicate.(binder.BoundComparisonExpr); !ok || comparison.Operator != "=" {
		t.Fatalf("unexpected collapsed predicate: %+v", filter.Predicate)
	}
}

func TestOptimizeOrdersIndexFriendlyAndTermsFirst(t *testing.T) {
	plan := LogicalFilter{
		Input: LogicalScan{},
		Predicate: binder.BoundLogicalExpr{
			Operator: "AND",
			Terms: []binder.BoundExpression{
				binder.BoundComparisonExpr{
					Left:     binder.BoundColumnRef{ColumnName: "name"},
					Operator: "=",
					Right:    binder.BoundLiteralExpr{},
				},
				binder.BoundComparisonExpr{
					Left:     binder.BoundColumnRef{ColumnName: "age"},
					Operator: "=",
					Right:    binder.BoundLiteralExpr{},
				},
				binder.BoundComparisonExpr{
					Left:     binder.BoundColumnRef{ColumnName: "id"},
					Operator: ">",
					Right:    binder.BoundLiteralExpr{},
				},
			},
		},
	}

	optimized := Optimize(plan)

	filter := optimized.(LogicalFilter)
	logicalExpr, ok := filter.Predicate.(binder.BoundLogicalExpr)
	if !ok {
		t.Fatalf("expected AND logical expr, got %T", filter.Predicate)
	}
	if len(logicalExpr.Terms) != 3 {
		t.Fatalf("expected 3 terms, got %+v", logicalExpr)
	}

	first, ok := logicalExpr.Terms[0].(binder.BoundComparisonExpr)
	if !ok || first.Operator != "=" {
		t.Fatalf("expected first term to be equality comparison, got %+v", logicalExpr.Terms[0])
	}
	firstCol := first.Left.(binder.BoundColumnRef)
	if firstCol.ColumnName != "name" && firstCol.ColumnName != "age" {
		t.Fatalf("expected first equality term to stay among equality terms, got %+v", first)
	}

	second, ok := logicalExpr.Terms[1].(binder.BoundComparisonExpr)
	if !ok || second.Operator != "=" {
		t.Fatalf("expected second term to be equality comparison, got %+v", logicalExpr.Terms[1])
	}

	third, ok := logicalExpr.Terms[2].(binder.BoundComparisonExpr)
	if !ok || third.Operator != ">" {
		t.Fatalf("expected range comparison to move after equality terms, got %+v", logicalExpr.Terms[2])
	}
}

func TestOptimizePushesFilterBelowPassThroughProject(t *testing.T) {
	plan := LogicalFilter{
		Predicate: binder.BoundComparisonExpr{
			Left:     binder.BoundColumnRef{TableName: "students", ColumnName: "age"},
			Operator: "=",
			Right:    binder.BoundLiteralExpr{},
		},
		Input: LogicalProject{
			Input: LogicalScan{},
			Outputs: []LogicalOutput{
				{
					Name: "id",
					Expr: binder.BoundColumnRef{TableName: "students", ColumnName: "id"},
				},
				{
					Name: "age",
					Expr: binder.BoundColumnRef{TableName: "students", ColumnName: "age"},
				},
			},
		},
	}

	optimized := Optimize(plan)

	project, ok := optimized.(LogicalProject)
	if !ok {
		t.Fatalf("expected project root after pushdown, got %T", optimized)
	}
	filter, ok := project.Input.(LogicalFilter)
	if !ok {
		t.Fatalf("expected filter below project, got %T", project.Input)
	}
	if _, ok := filter.Input.(LogicalScan); !ok {
		t.Fatalf("expected pushed filter input to be scan, got %T", filter.Input)
	}
}

func TestOptimizeKeepsFilterAboveProjectWhenProjectDropsNeededColumn(t *testing.T) {
	plan := LogicalFilter{
		Predicate: binder.BoundComparisonExpr{
			Left:     binder.BoundColumnRef{TableName: "students", ColumnName: "age"},
			Operator: "=",
			Right:    binder.BoundLiteralExpr{},
		},
		Input: LogicalProject{
			Input: LogicalScan{},
			Outputs: []LogicalOutput{
				{
					Name: "id",
					Expr: binder.BoundColumnRef{TableName: "students", ColumnName: "id"},
				},
			},
		},
	}

	optimized := Optimize(plan)

	filter, ok := optimized.(LogicalFilter)
	if !ok {
		t.Fatalf("expected filter to remain above project, got %T", optimized)
	}
	if _, ok := filter.Input.(LogicalProject); !ok {
		t.Fatalf("expected filter input to remain project, got %T", filter.Input)
	}
}

func TestOptimizePushdownMergesWithAdjacentFilter(t *testing.T) {
	plan := LogicalFilter{
		Predicate: binder.BoundComparisonExpr{
			Left:     binder.BoundColumnRef{TableName: "students", ColumnName: "age"},
			Operator: "=",
			Right:    binder.BoundLiteralExpr{},
		},
		Input: LogicalProject{
			Input: LogicalFilter{
				Input: LogicalScan{},
				Predicate: binder.BoundComparisonExpr{
					Left:     binder.BoundColumnRef{TableName: "students", ColumnName: "id"},
					Operator: ">",
					Right:    binder.BoundLiteralExpr{},
				},
			},
			Outputs: []LogicalOutput{
				{
					Name: "id",
					Expr: binder.BoundColumnRef{TableName: "students", ColumnName: "id"},
				},
				{
					Name: "age",
					Expr: binder.BoundColumnRef{TableName: "students", ColumnName: "age"},
				},
			},
		},
	}

	optimized := Optimize(plan)

	project, ok := optimized.(LogicalProject)
	if !ok {
		t.Fatalf("expected project root after pushdown, got %T", optimized)
	}
	filter, ok := project.Input.(LogicalFilter)
	if !ok {
		t.Fatalf("expected merged filter below project, got %T", project.Input)
	}
	if _, ok := filter.Input.(LogicalScan); !ok {
		t.Fatalf("expected merged filter input to be scan, got %T", filter.Input)
	}

	logicalExpr, ok := filter.Predicate.(binder.BoundLogicalExpr)
	if !ok {
		t.Fatalf("expected merged predicate to be AND logical expr, got %T", filter.Predicate)
	}
	if len(logicalExpr.Terms) != 2 {
		t.Fatalf("expected 2 merged terms, got %+v", logicalExpr)
	}
	first := logicalExpr.Terms[0].(binder.BoundComparisonExpr)
	second := logicalExpr.Terms[1].(binder.BoundComparisonExpr)
	if first.Operator != "=" || second.Operator != ">" {
		t.Fatalf("unexpected merged predicate order: %+v", logicalExpr.Terms)
	}
}

func TestOptimizePushesFilterBelowSort(t *testing.T) {
	plan := LogicalFilter{
		Predicate: binder.BoundComparisonExpr{
			Left:     binder.BoundColumnRef{TableName: "students", ColumnName: "age"},
			Operator: "=",
			Right:    binder.BoundLiteralExpr{},
		},
		Input: LogicalSort{
			Input: LogicalScan{},
			OrderBy: []LogicalSortKey{
				{
					Expr: binder.BoundColumnRef{TableName: "students", ColumnName: "name"},
					Desc: true,
				},
			},
		},
	}

	optimized := Optimize(plan)

	sortNode, ok := optimized.(LogicalSort)
	if !ok {
		t.Fatalf("expected sort root after pushdown, got %T", optimized)
	}
	filter, ok := sortNode.Input.(LogicalFilter)
	if !ok {
		t.Fatalf("expected filter below sort, got %T", sortNode.Input)
	}
	if _, ok := filter.Input.(LogicalScan); !ok {
		t.Fatalf("expected pushed filter input to be scan, got %T", filter.Input)
	}
}

func TestOptimizePushdownBelowSortMergesWithAdjacentFilter(t *testing.T) {
	plan := LogicalFilter{
		Predicate: binder.BoundComparisonExpr{
			Left:     binder.BoundColumnRef{TableName: "students", ColumnName: "age"},
			Operator: "=",
			Right:    binder.BoundLiteralExpr{},
		},
		Input: LogicalSort{
			Input: LogicalFilter{
				Input: LogicalScan{},
				Predicate: binder.BoundComparisonExpr{
					Left:     binder.BoundColumnRef{TableName: "students", ColumnName: "id"},
					Operator: ">",
					Right:    binder.BoundLiteralExpr{},
				},
			},
			OrderBy: []LogicalSortKey{
				{
					Expr: binder.BoundColumnRef{TableName: "students", ColumnName: "name"},
				},
			},
		},
	}

	optimized := Optimize(plan)

	sortNode, ok := optimized.(LogicalSort)
	if !ok {
		t.Fatalf("expected sort root after pushdown, got %T", optimized)
	}
	filter, ok := sortNode.Input.(LogicalFilter)
	if !ok {
		t.Fatalf("expected merged filter below sort, got %T", sortNode.Input)
	}
	if _, ok := filter.Input.(LogicalScan); !ok {
		t.Fatalf("expected merged filter input to be scan, got %T", filter.Input)
	}

	logicalExpr, ok := filter.Predicate.(binder.BoundLogicalExpr)
	if !ok {
		t.Fatalf("expected merged predicate to be AND logical expr, got %T", filter.Predicate)
	}
	if len(logicalExpr.Terms) != 2 {
		t.Fatalf("expected 2 merged terms, got %+v", logicalExpr)
	}
	first := logicalExpr.Terms[0].(binder.BoundComparisonExpr)
	second := logicalExpr.Terms[1].(binder.BoundComparisonExpr)
	if first.Operator != "=" || second.Operator != ">" {
		t.Fatalf("unexpected merged predicate order: %+v", logicalExpr.Terms)
	}
}

func TestOptimizePushesFilterBelowAggregateForGroupingColumn(t *testing.T) {
	plan := LogicalFilter{
		Predicate: binder.BoundComparisonExpr{
			Left:     binder.BoundColumnRef{TableName: "students", ColumnName: "dept_id"},
			Operator: "=",
			Right:    binder.BoundLiteralExpr{},
		},
		Input: LogicalAggregate{
			Input: LogicalScan{},
			GroupBy: []binder.BoundExpression{
				binder.BoundColumnRef{TableName: "students", ColumnName: "dept_id"},
			},
			Aggregates: []binder.BoundAggregateExpr{
				{Function: "COUNT", Arg: binder.BoundStarExpr{}},
			},
		},
	}

	optimized := Optimize(plan)

	aggregate, ok := optimized.(LogicalAggregate)
	if !ok {
		t.Fatalf("expected aggregate root after pushdown, got %T", optimized)
	}
	filter, ok := aggregate.Input.(LogicalFilter)
	if !ok {
		t.Fatalf("expected filter below aggregate, got %T", aggregate.Input)
	}
	if _, ok := filter.Input.(LogicalScan); !ok {
		t.Fatalf("expected pushed filter input to be scan, got %T", filter.Input)
	}
}

func TestOptimizeKeepsHavingFilterAboveAggregate(t *testing.T) {
	countExpr := binder.BoundAggregateExpr{Function: "COUNT", Arg: binder.BoundStarExpr{}}
	plan := LogicalFilter{
		Predicate: binder.BoundComparisonExpr{
			Left:     countExpr,
			Operator: ">",
			Right:    binder.BoundLiteralExpr{},
		},
		Input: LogicalAggregate{
			Input: LogicalScan{},
			GroupBy: []binder.BoundExpression{
				binder.BoundColumnRef{TableName: "students", ColumnName: "dept_id"},
			},
			Aggregates: []binder.BoundAggregateExpr{countExpr},
		},
	}

	optimized := Optimize(plan)

	filter, ok := optimized.(LogicalFilter)
	if !ok {
		t.Fatalf("expected HAVING filter to remain above aggregate, got %T", optimized)
	}
	if _, ok := filter.Input.(LogicalAggregate); !ok {
		t.Fatalf("expected filter input to remain aggregate, got %T", filter.Input)
	}
}

func TestOptimizePushesFilterThroughSortAndProjectIntoCanonicalScanNearFilter(t *testing.T) {
	plan := LogicalFilter{
		Predicate: binder.BoundComparisonExpr{
			Left:     binder.BoundColumnRef{TableName: "students", ColumnName: "age"},
			Operator: "=",
			Right:    binder.BoundLiteralExpr{},
		},
		Input: LogicalSort{
			OrderBy: []LogicalSortKey{
				{
					Expr: binder.BoundColumnRef{TableName: "students", ColumnName: "name"},
				},
			},
			Input: LogicalProject{
				Outputs: []LogicalOutput{
					{
						Name: "id",
						Expr: binder.BoundColumnRef{TableName: "students", ColumnName: "id"},
					},
					{
						Name: "age",
						Expr: binder.BoundColumnRef{TableName: "students", ColumnName: "age"},
					},
				},
				Input: LogicalFilter{
					Predicate: binder.BoundComparisonExpr{
						Left:     binder.BoundColumnRef{TableName: "students", ColumnName: "id"},
						Operator: ">",
						Right:    binder.BoundLiteralExpr{},
					},
					Input: LogicalScan{},
				},
			},
		},
	}

	optimized := Optimize(plan)

	sortNode, ok := optimized.(LogicalSort)
	if !ok {
		t.Fatalf("expected sort root after pushdown, got %T", optimized)
	}
	project, ok := sortNode.Input.(LogicalProject)
	if !ok {
		t.Fatalf("expected project below sort, got %T", sortNode.Input)
	}
	filter, ok := project.Input.(LogicalFilter)
	if !ok {
		t.Fatalf("expected one canonical filter below project, got %T", project.Input)
	}
	if _, ok := filter.Input.(LogicalScan); !ok {
		t.Fatalf("expected canonical filter input to be scan, got %T", filter.Input)
	}

	logicalExpr, ok := filter.Predicate.(binder.BoundLogicalExpr)
	if !ok {
		t.Fatalf("expected canonical predicate to be AND logical expr, got %T", filter.Predicate)
	}
	if len(logicalExpr.Terms) != 2 {
		t.Fatalf("expected 2 canonical terms, got %+v", logicalExpr)
	}
	first := logicalExpr.Terms[0].(binder.BoundComparisonExpr)
	second := logicalExpr.Terms[1].(binder.BoundComparisonExpr)
	if first.Operator != "=" || second.Operator != ">" {
		t.Fatalf("unexpected canonical predicate order: %+v", logicalExpr.Terms)
	}
}
