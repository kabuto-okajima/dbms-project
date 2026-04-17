package planner

import "dbms-project/internal/binder"

// Optimize applies rule-based rewrites to a logical plan.
//
// This first version installs the optimizer boundary and walks the plan
// bottom-up, but does not yet change plan semantics beyond rebuilding nodes
// with optimized children.
func Optimize(plan LogicalPlan) LogicalPlan {
	if plan == nil {
		return nil
	}

	return optimizePlan(plan)
}

func optimizePlan(plan LogicalPlan) LogicalPlan {
	switch node := plan.(type) {
	case LogicalScan:
		return node
	case LogicalFilter:
		node.Input = optimizePlan(node.Input)
		return rewriteFilter(node)
	case LogicalAggregate:
		node.Input = optimizePlan(node.Input)
		return rewriteAggregate(node)
	case LogicalProject:
		node.Input = optimizePlan(node.Input)
		return rewriteProject(node)
	case LogicalSort:
		node.Input = optimizePlan(node.Input)
		return rewriteSort(node)
	default:
		return plan
	}
}

func rewriteFilter(node LogicalFilter) LogicalPlan {
	if node.Predicate == nil {
		return node.Input
	}
	if child, ok := node.Input.(LogicalFilter); ok {
		if child.Predicate == nil {
			return LogicalFilter{
				Input:     child.Input,
				Predicate: node.Predicate,
			}
		}
		return LogicalFilter{
			Input:     child.Input,
			Predicate: combinePredicatesWithAnd(child.Predicate, node.Predicate),
		}
		// Filter(p2) -> Filter(p1) -> Scan
		// becomes
		// Filter(p1 AND p2) -> Scan
	}
	return node
}

func rewriteAggregate(node LogicalAggregate) LogicalPlan {
	return node
}

func rewriteProject(node LogicalProject) LogicalPlan {
	return node
}

func rewriteSort(node LogicalSort) LogicalPlan {
	if len(node.OrderBy) == 0 {
		return node.Input
	}
	return node
}

func combinePredicatesWithAnd(left, right binder.BoundExpression) binder.BoundExpression {
	terms := make([]binder.BoundExpression, 0, 4)
	appendAndTerms(&terms, left)
	appendAndTerms(&terms, right)

	if len(terms) == 1 {
		return terms[0]
	}

	return binder.BoundLogicalExpr{
		Operator: "AND",
		Terms:    terms,
	}
}

func appendAndTerms(dst *[]binder.BoundExpression, expr binder.BoundExpression) {
	logicalExpr, ok := expr.(binder.BoundLogicalExpr)
	if !ok || logicalExpr.Operator != "AND" {
		*dst = append(*dst, expr)
		return
	}

	for _, term := range logicalExpr.Terms {
		appendAndTerms(dst, term)
	}
}
