package planner

import (
	"sort"

	"dbms-project/internal/binder"
	"dbms-project/internal/statement"
)

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
		// Scan(students)
		return node
	case LogicalFilter:
		// Filter(age = 20) -> ...
		node.Input = optimizePlan(node.Input)
		return rewriteFilter(node)
	case LogicalAggregate:
		// Aggregate(GROUP BY dept_id) -> ...
		node.Input = optimizePlan(node.Input)
		return rewriteAggregate(node)
	case LogicalProject:
		// Project(name) -> ...
		node.Input = optimizePlan(node.Input)
		return rewriteProject(node)
	case LogicalSort:
		// Sort(name DESC) -> ...
		node.Input = optimizePlan(node.Input)
		return rewriteSort(node)
	default:
		return plan
	}
}

func rewriteFilter(node LogicalFilter) LogicalPlan {
	if node.Predicate == nil {
		// Filter(nil) -> child
		return node.Input
	}

	// (age = 20 AND (id > 1 AND dept_id = 10))
	// -> age = 20 AND id > 1 AND dept_id = 10
	node.Predicate = canonicalizeFilterPredicate(node.Predicate)
	if child, ok := node.Input.(LogicalFilter); ok {
		if child.Predicate == nil {
			return LogicalFilter{
				Input:     child.Input,
				Predicate: node.Predicate,
			}
		}

		// Filter(id > 1) -> Filter(age = 20) -> Scan
		// -> Filter(age = 20 AND id > 1) -> Scan
		return LogicalFilter{
			Input:     child.Input,
			Predicate: canonicalizeFilterPredicate(combinePredicatesWithAnd(child.Predicate, node.Predicate)),
		}
	}
	if child, ok := node.Input.(LogicalProject); ok && canPushFilterBelowProject(child, node.Predicate) {
		// Logical Tree:
		// Filter(age = 20) -> Project(id, age) -> Scan
		// becomes
		// Project(id, age) -> Filter(age = 20) -> Scan
		// Data flow: Scan -> Filter(age = 20) -> Project(id, age)
		//
		// Filter(age = 20) -> Project(id, age) -> Filter(id > 1) -> Scan
		// becomes
		// Project(id, age) -> Filter(age = 20 AND id > 1) -> Scan
		pushedFilter := rewriteFilter(LogicalFilter{
			Input:     child.Input,
			Predicate: node.Predicate,
		})
		return LogicalProject{
			Input:   pushedFilter,
			Outputs: child.Outputs,
		}
	}
	if child, ok := node.Input.(LogicalSort); ok {
		// Logical Tree:
		// Filter(age = 20) -> Sort(name DESC) -> Scan
		// becomes
		// Sort(name DESC) -> Filter(age = 20) -> Scan
		// Data flow: Scan -> Filter(age = 20) -> Sort(name DESC)
		//
		// Filter(age = 20) -> Sort(name DESC) -> Filter(id > 1) -> Scan
		// becomes
		// Sort(name DESC) -> Filter(age = 20 AND id > 1) -> Scan
		pushedFilter := rewriteFilter(LogicalFilter{
			Input:     child.Input,
			Predicate: node.Predicate,
		})
		return LogicalSort{
			Input:   pushedFilter,
			OrderBy: child.OrderBy,
		}
	}
	if child, ok := node.Input.(LogicalAggregate); ok && canPushFilterBelowAggregate(child, node.Predicate) {
		// Logical Tree:
		// Filter(dept_id = 10) -> Aggregate(GROUP BY dept_id) -> Scan
		// becomes
		// Aggregate(GROUP BY dept_id) -> Filter(dept_id = 10) -> Scan
		// Data flow: Scan -> Filter(dept_id = 10) -> Aggregate(GROUP BY dept_id)
		//
		// Filter(dept_id = 10) -> Aggregate(GROUP BY dept_id) -> Filter(id > 1) -> Scan
		// becomes
		// Aggregate(GROUP BY dept_id) -> Filter(dept_id = 10 AND id > 1) -> Scan
		pushedFilter := rewriteFilter(LogicalFilter{
			Input:     child.Input,
			Predicate: node.Predicate,
		})
		return LogicalAggregate{
			Input:      pushedFilter,
			GroupBy:    child.GroupBy,
			Aggregates: child.Aggregates,
		}
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
		// Sort([]) -> child
		return node.Input
	}
	return node
}

func combinePredicatesWithAnd(left, right binder.BoundExpression) binder.BoundExpression {
	terms := make([]binder.BoundExpression, 0, 4)

	// left:  age = 20
	// right: id > 1 AND dept_id = 10
	appendAndTerms(&terms, left)
	appendAndTerms(&terms, right)
	orderAndTerms(terms)

	if len(terms) == 1 {
		return terms[0]
	}

	return binder.BoundLogicalExpr{
		Operator: "AND",
		Terms:    terms,
	}
}

func normalizeAndPredicate(expr binder.BoundExpression) binder.BoundExpression {
	terms := make([]binder.BoundExpression, 0, 4)

	// age = 20 AND (id > 1 AND dept_id = 10)
	// -> [age = 20, id > 1, dept_id = 10]
	appendAndTerms(&terms, expr)
	orderAndTerms(terms) // age = 20 should come before id > 1 and dept_id = 10 for better index utilization

	if len(terms) == 1 {
		// [age = 20] -> age = 20
		return terms[0]
	}

	return binder.BoundLogicalExpr{
		Operator: "AND",
		Terms:    terms,
	}
}

func canonicalizeFilterPredicate(expr binder.BoundExpression) binder.BoundExpression {
	// age = 20 AND (id > 1 AND dept_id = 10)
	// -> age = 20 AND id > 1 AND dept_id = 10
	return normalizeAndPredicate(expr)
}

func appendAndTerms(dst *[]binder.BoundExpression, expr binder.BoundExpression) {
	logicalExpr, ok := expr.(binder.BoundLogicalExpr)
	if !ok || logicalExpr.Operator != "AND" {
		// age = 20 -> append
		*dst = append(*dst, expr)
		return
	}

	// (age = 20 AND id > 1) -> recurse
	for _, term := range logicalExpr.Terms {
		appendAndTerms(dst, term)
	}
}

// canPushFilterBelowProject reports whether a filter can move below a project
// without losing any columns the predicate still needs.
func canPushFilterBelowProject(project LogicalProject, predicate binder.BoundExpression) bool {
	available := make(map[string]struct{}, len(project.Outputs))
	allColumnsAvailable := false

	for _, output := range project.Outputs {
		if output.ExpandsStar {
			// Project(*) -> all base columns still available
			allColumnsAvailable = true
			continue
		}

		column, ok := output.Expr.(binder.BoundColumnRef)
		if !ok {
			// Project(literal) or Project(expr) -> stop
			return false
		}
		available[columnRefKey(column)] = struct{}{}
	}

	if allColumnsAvailable {
		return true
	}

	for _, column := range collectPredicateColumns(predicate) {
		if _, ok := available[columnRefKey(column)]; !ok {
			// Filter(age = 20) cannot move below Project(id)
			return false
		}
	}

	return true
}

// canPushFilterBelowAggregate reports whether the filter only uses grouping
// columns and does not depend on aggregate values like COUNT(*).
// If the aggregate has no GROUP BY, we cannot push down any filter because it may reference aggregate values.
func canPushFilterBelowAggregate(aggregate LogicalAggregate, predicate binder.BoundExpression) bool {
	if len(aggregate.GroupBy) == 0 {
		// Filter(COUNT(*) > 1) -> Aggregate(...) stays above Aggregate
		return false
	}
	if containsAggregateExpression(predicate) {
		// Filter(COUNT(*) > 1) stays above Aggregate
		return false
	}

	groupColumns := make(map[string]struct{}, len(aggregate.GroupBy))
	for _, expr := range aggregate.GroupBy {
		column, ok := expr.(binder.BoundColumnRef)
		if !ok {
			return false
		}
		groupColumns[columnRefKey(column)] = struct{}{}
	}

	for _, column := range collectPredicateColumns(predicate) {
		if _, ok := groupColumns[columnRefKey(column)]; !ok {
			// Filter(name = 'Bob') cannot move below Aggregate(GROUP BY dept_id)
			return false
		}
	}

	return true
}

func collectPredicateColumns(expr binder.BoundExpression) []binder.BoundColumnRef {
	columns := make([]binder.BoundColumnRef, 0, 4)
	appendPredicateColumns(&columns, expr)
	return columns
}

func appendPredicateColumns(dst *[]binder.BoundColumnRef, expr binder.BoundExpression) {
	switch valueExpr := expr.(type) {
	case binder.BoundColumnRef:
		*dst = append(*dst, valueExpr)
	case binder.BoundComparisonExpr:
		appendPredicateColumns(dst, valueExpr.Left)
		appendPredicateColumns(dst, valueExpr.Right)
	case binder.BoundLogicalExpr:
		for _, term := range valueExpr.Terms {
			appendPredicateColumns(dst, term)
		}
	}
}

func containsAggregateExpression(expr binder.BoundExpression) bool {
	switch valueExpr := expr.(type) {
	case nil:
		return false
	case binder.BoundAggregateExpr:
		return true
	case binder.BoundComparisonExpr:
		return containsAggregateExpression(valueExpr.Left) || containsAggregateExpression(valueExpr.Right)
	case binder.BoundLogicalExpr:
		for _, term := range valueExpr.Terms {
			if containsAggregateExpression(term) {
				return true
			}
		}
	}
	return false
}

func columnRefKey(column binder.BoundColumnRef) string {
	return column.TableName + "." + column.ColumnName
}

func orderAndTerms(terms []binder.BoundExpression) {
	// [name = 'Bob', age = 20, id > 1]
	// -> [age = 20, id > 1, name = 'Bob']
	sort.SliceStable(terms, func(i, j int) bool {
		return andTermPriority(terms[i]) < andTermPriority(terms[j])
	})
}

func andTermPriority(expr binder.BoundExpression) int {
	comparison, ok := expr.(binder.BoundComparisonExpr)
	if !ok {
		return 3
	}

	if !isColumnLiteralComparison(comparison) {
		return 2
	}

	switch comparison.Operator {
	case statement.OpEqual:
		return 0
	case statement.OpLessThan, statement.OpLessThanOrEqual, statement.OpGreaterThan, statement.OpGreaterThanOrEqual:
		return 1
	default:
		return 2
	}
}

func isColumnLiteralComparison(expr binder.BoundComparisonExpr) bool {
	_, leftColumn := expr.Left.(binder.BoundColumnRef)
	_, rightLiteral := expr.Right.(binder.BoundLiteralExpr)
	if leftColumn && rightLiteral {
		return true
	}

	_, leftLiteral := expr.Left.(binder.BoundLiteralExpr)
	_, rightColumn := expr.Right.(binder.BoundColumnRef)
	return leftLiteral && rightColumn
}
