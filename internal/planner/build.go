package planner

import (
	"fmt"

	"dbms-project/internal/binder"
	"dbms-project/internal/shared"
)

// BuildLogicalSelect converts one bound SELECT into the first logical plan
// shape supported by the planner.
//
// Tree:
//
//	Project
//		└─ Sort
//				└─ Filter(HAVING)
//						└─ Aggregate
//								└─ Filter(WHERE)
//										└─ Join or Scan
func BuildLogicalSelect(bound *binder.BoundSelect) (LogicalPlan, error) {
	if bound == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "planner: bound select is required")
	}
	plan, err := buildLogicalFromPlan(bound)
	if err != nil {
		return nil, err
	}
	// examples:
	//  - SELECT * FROM t
	//  - LogicalScan{Table: t}
	//
	//   - SELECT * FROM t JOIN u ON t.a = u.a
	//   - LogicalJoin{
	//       Left: LogicalScan{Table: t},
	//       Right: LogicalScan{Table: u},
	//       Predicate: t.a = u.a,
	//     }

	if bound.Where != nil {
		plan = LogicalFilter{
			Input:     plan,
			Predicate: bound.Where,
		}
		// example:
		//   - SELECT * FROM t WHERE a > 5
		//   - LogicalFilter{Predicate: a > 5, Input: LogicalScan{Table: t}}
	}

	if requiresAggregate(bound) {
		plan = LogicalAggregate{
			Input:      plan,
			GroupBy:    bound.GroupBy,
			Aggregates: collectAggregates(bound.SelectItems, bound.Having),
		}
		// example:
		//   - SELECT a, COUNT(*) FROM t GROUP BY a
		//   - LogicalAggregate{
		//       GroupBy: [a],
		//       Aggregates: [COUNT(*)],
		//       Input: LogicalScan{Table: t},
		//     }
	}

	if bound.Having != nil {
		plan = LogicalFilter{
			Input:     plan,
			Predicate: bound.Having,
		}
		// example:
		//   - SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 5
		//   - LogicalFilter{
		//       Predicate: COUNT(*) > 5,
		//       Input: LogicalAggregate{
		//         GroupBy: [a],
		//         Aggregates: [COUNT(*)],
		//         Input: LogicalScan{Table: t},
		//       },
		//     }
	}

	if len(bound.OrderBy) > 0 {
		sortKeys, err := buildLogicalSortKeys(bound.OrderBy)
		if err != nil {
			return nil, err
		}
		plan = LogicalSort{
			Input:   plan,
			OrderBy: sortKeys,
		}
		// example:
		//   - SELECT a FROM t ORDER BY a DESC
		//   - LogicalSort{
		//       Input: LogicalScan{Table: t},
		//       OrderBy: [
		//         {Expr: a, Desc: true, Type: ...},
		//       ],
		//     }
	}

	outputs, err := buildLogicalOutputs(bound.SelectItems)
	if err != nil {
		return nil, err
	}

	plan = LogicalProject{
		Input:   plan,
		Outputs: outputs,
	}
	// example:
	//   - SELECT a, COUNT(*) FROM t
	//   - LogicalProject{
	//       Input: LogicalScan{Table: t},
	//       Outputs: [
	//         {Name: "a", Expr: a, Type: ...},
	//         {Name: "COUNT(*)", Expr: COUNT(*), Type: ...},
	//       ],
	//     }

	return plan, nil
}

func buildLogicalFromPlan(bound *binder.BoundSelect) (LogicalPlan, error) {
	if bound == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "planner: bound select is required")
	}

	if bound.Join == nil {
		if len(bound.From) != 1 {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "planner: exactly one FROM table is required when JOIN is absent")
		}
		return LogicalScan{
			Table: bound.From[0],
		}, nil
	}

	if len(bound.From) != 2 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "planner: exactly two FROM tables are required when JOIN is present")
	}

	return LogicalJoin{
		Left: LogicalScan{
			Table: bound.From[0],
		},
		Right: LogicalScan{
			Table: bound.From[1],
		},
		Predicate: bound.Join.On,
	}, nil
}

func requiresAggregate(bound *binder.BoundSelect) bool {
	if bound == nil {
		return false
	}
	if len(bound.GroupBy) > 0 {
		return true
	}
	for _, item := range bound.SelectItems {
		if containsAggregate(item.Expr) {
			return true
		}
	}
	if containsAggregate(bound.Having) {
		return true
	}
	return false
}

// collectAggregates traverses projected expressions and HAVING predicates and
// collects the unique aggregates needed by the LogicalAggregate node.
func collectAggregates(items []binder.BoundSelectItem, having binder.BoundExpression) []binder.BoundAggregateExpr {
	aggregates := make([]binder.BoundAggregateExpr, 0)
	seen := make(map[string]struct{})
	for _, item := range items {
		collectAggregatesFromExpression(item.Expr, &aggregates, seen)
	}
	collectAggregatesFromExpression(having, &aggregates, seen)
	return aggregates
}

func collectAggregatesFromExpression(expr binder.BoundExpression, out *[]binder.BoundAggregateExpr, seen map[string]struct{}) {
	switch valueExpr := expr.(type) {
	case binder.BoundAggregateExpr:
		key := aggregateSignature(valueExpr)
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		*out = append(*out, valueExpr)
	case binder.BoundComparisonExpr:
		collectAggregatesFromExpression(valueExpr.Left, out, seen)
		collectAggregatesFromExpression(valueExpr.Right, out, seen)
	case binder.BoundLogicalExpr:
		for _, term := range valueExpr.Terms {
			collectAggregatesFromExpression(term, out, seen)
		}
	}
}

func containsAggregate(expr binder.BoundExpression) bool {
	switch valueExpr := expr.(type) {
	case binder.BoundAggregateExpr:
		return true
	case binder.BoundComparisonExpr:
		return containsAggregate(valueExpr.Left) || containsAggregate(valueExpr.Right)
	case binder.BoundLogicalExpr:
		for _, term := range valueExpr.Terms {
			if containsAggregate(term) {
				return true
			}
		}
	}
	return false
}

func buildLogicalOutputs(items []binder.BoundSelectItem) ([]LogicalOutput, error) {
	if len(items) == 0 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "planner: at least one output is required")
	}

	outputs := make([]LogicalOutput, 0, len(items))
	for _, item := range items {
		outputType, expandsStar, err := logicalOutputType(item.Expr)
		if err != nil {
			return nil, err
		}

		name := item.Alias
		if name == "" {
			name = defaultOutputName(item.Expr)
		}

		outputs = append(outputs, LogicalOutput{
			Name:        name,
			Expr:        item.Expr,
			Type:        outputType,
			ExpandsStar: expandsStar,
		})
	}

	return outputs, nil
}

func buildLogicalSortKeys(terms []binder.BoundOrderByTerm) ([]LogicalSortKey, error) {
	if len(terms) == 0 {
		return nil, nil
	}

	keys := make([]LogicalSortKey, 0, len(terms))
	for _, term := range terms {
		keyType, expandsStar, err := logicalOutputType(term.Expr)
		if err != nil {
			return nil, err
		}
		if expandsStar {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "planner: ORDER BY expressions must be scalar")
		}

		keys = append(keys, LogicalSortKey{
			Expr: term.Expr,
			Desc: term.Desc,
			Type: keyType,
		})
	}

	return keys, nil
}

func logicalOutputType(expr binder.BoundExpression) (shared.DataType, bool, error) {
	switch valueExpr := expr.(type) {
	case binder.BoundStarExpr:
		return "", true, nil
	case binder.BoundColumnRef:
		return valueExpr.Type, false, nil
	case binder.BoundLiteralExpr:
		return valueExpr.Value.Type, false, nil
	case binder.BoundAggregateExpr:
		switch valueExpr.Function {
		case "COUNT", "SUM", "MIN", "MAX":
			return shared.TypeInteger, false, nil
		default:
			return "", false, shared.NewError(shared.ErrInvalidDefinition, "planner: unsupported output aggregate %q", valueExpr.Function)
		}
	default:
		return "", false, shared.NewError(shared.ErrInvalidDefinition, "planner: unsupported output expression %T", expr)
	}
}

func defaultOutputName(expr binder.BoundExpression) string {
	switch valueExpr := expr.(type) {
	case binder.BoundStarExpr:
		return "*"
	case binder.BoundColumnRef:
		return valueExpr.ColumnName
	case binder.BoundLiteralExpr:
		switch valueExpr.Value.Type {
		case shared.TypeInteger:
			return "literal_int"
		case shared.TypeString:
			return "literal_string"
		default:
			return "literal"
		}
	case binder.BoundAggregateExpr:
		return aggregateOutputName(valueExpr)
	default:
		return "expr"
	}
}

func aggregateSignature(expr binder.BoundAggregateExpr) string {
	return aggregateOutputName(expr)
}

func aggregateOutputName(expr binder.BoundAggregateExpr) string {
	switch arg := expr.Arg.(type) {
	case binder.BoundStarExpr:
		return fmt.Sprintf("%s(*)", expr.Function)
	case binder.BoundColumnRef:
		return fmt.Sprintf("%s(%s)", expr.Function, arg.ColumnName)
	default:
		return fmt.Sprintf("%s(expr)", expr.Function)
	}
}
