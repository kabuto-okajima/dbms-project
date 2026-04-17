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
