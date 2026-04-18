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

// optimizePlan walks the tree bottom-up so each rewrite sees already-optimized
// children before deciding whether to transform the current node.
func optimizePlan(plan LogicalPlan) LogicalPlan {
	switch node := plan.(type) {
	case LogicalScan:
		// Scan(students)
		return node
	case LogicalJoin:
		// Join(students.dept_id = departments.id) -> ...
		// node.Left: LogicalScan{Table: students}
		// node.Right: LogicalScan{Table: departments}
		node.Left = optimizePlan(node.Left)
		node.Right = optimizePlan(node.Right)
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

// rewriteFilter is the main rewrite entrypoint for WHERE/HAVING-style filters.
// It canonicalizes AND predicates, merges adjacent filters, and pushes filters
// below pass-through operators when that is semantically safe.
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
	if child, ok := node.Input.(LogicalJoin); ok {
		return rewriteFilterOverJoin(node.Predicate, child)
	}
	return node
}

// rewriteFilterOverJoin splits an AND-connected filter above a join into:
// left-only terms, right-only terms, and residual cross-table terms.
//
// Only single-table terms are pushed below the join. The join predicate itself
// stays on the LogicalJoin node, and any cross-table WHERE term remains above.
//
//		example:
//	  SELECT * FROM students JOIN departments ON students.dept_id = departments.id WHERE students.age > 20 AND departments.name = 'CS'
//	  LogicalFilter{
//	      Predicate: students.age > 20 AND departments.name = 'CS',
//	      Input: LogicalJoin{
//	        Predicate: students.dept_id = departments.id,
//	        Left: LogicalScan{Table: students},
//	        Right: LogicalScan{Table: departments},
//	      },
//	    }
//
// becomes
//
//	LogicalJoin{
//	    Predicate: students.dept_id = departments.id,
//	    Left: LogicalFilter{
//	      Predicate: students.age > 20,
//	      Input: LogicalScan{Table: students},
//	    },
//	    Right: LogicalFilter{
//	      Predicate: departments.name = 'CS',
//	      Input: LogicalScan{Table: departments},
//	    },
//	  }
func rewriteFilterOverJoin(predicate binder.BoundExpression, join LogicalJoin) LogicalPlan {
	// leftTerms: [students.dept_id = 10]
	// rightTerms: [departments.name = 'CS']
	// residualTerms: [students.age > 20]
	leftTerms := make([]binder.BoundExpression, 0, 2)
	rightTerms := make([]binder.BoundExpression, 0, 2)
	residualTerms := make([]binder.BoundExpression, 0, 2) // residualTerms are predicates that reference columns from both sides of the join and cannot be pushed down to either side, so they stay above the join.
	leftTableName := logicalPlanTableName(join.Left)
	rightTableName := logicalPlanTableName(join.Right)

	for _, term := range andTerms(predicate) {
		// terms: [students.dept_id = 10, departments.name = 'CS', students.age > 20]
		tableName, ok := singleTablePredicateOwner(term)
		if !ok {
			residualTerms = append(residualTerms, term)
			continue
		}

		switch tableName {
		case leftTableName:
			leftTerms = append(leftTerms, term)
		case rightTableName:
			rightTerms = append(rightTerms, term)
		default:
			residualTerms = append(residualTerms, term)
		}
	}

	if len(leftTerms) > 0 {
		leftPredicate, ok := rebaseExpressionOrdinals(combineResidualTerms(leftTerms), 0)
		if !ok {
			residualTerms = append(residualTerms, leftTerms...)
		} else {
			join.Left = rewriteFilter(LogicalFilter{
				Input:     join.Left,
				Predicate: leftPredicate,
			})
		}
	}
	if len(rightTerms) > 0 {
		rightOffset, ok := logicalPlanOutputWidth(join.Left)
		if !ok {
			residualTerms = append(residualTerms, rightTerms...)
		} else if rightPredicate, ok := rebaseExpressionOrdinals(combineResidualTerms(rightTerms), rightOffset); !ok {
			residualTerms = append(residualTerms, rightTerms...)
		} else {
			join.Right = rewriteFilter(LogicalFilter{
				Input:     join.Right,
				Predicate: rightPredicate,
			})
		}
	}

	residual := combineResidualTerms(residualTerms)
	if residual == nil {
		return join
	}

	return LogicalFilter{
		Input:     join,
		Predicate: residual,
	}
}

// rewriteAggregate is the placeholder hook for future aggregate-specific rules.
func rewriteAggregate(node LogicalAggregate) LogicalPlan {
	return node
}

// rewriteProject is the placeholder hook for future projection-specific rules.
func rewriteProject(node LogicalProject) LogicalPlan {
	return node
}

// rewriteSort removes no-op sorts and otherwise preserves sort placement.
func rewriteSort(node LogicalSort) LogicalPlan {
	if len(node.OrderBy) == 0 {
		// Sort([]) -> child
		return node.Input
	}
	return node
}

// combinePredicatesWithAnd rebuilds two predicates into one canonical AND tree.
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

// normalizeAndPredicate flattens nested AND expressions and reorders terms into
// a stable, index-friendly order.
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

// canonicalizeFilterPredicate is the one place that normalizes filter shape
// before the optimizer tries to merge or push a predicate.
func canonicalizeFilterPredicate(expr binder.BoundExpression) binder.BoundExpression {
	// age = 20 AND (id > 1 AND dept_id = 10)
	// -> age = 20 AND id > 1 AND dept_id = 10
	return normalizeAndPredicate(expr)
}

// appendAndTerms recursively flattens nested AND expressions into one slice.
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
	// expr: students.age > 20 AND departments.name = 'CS'
	// -> [students.age, departments.name]
	columns := make([]binder.BoundColumnRef, 0, 4)
	appendPredicateColumns(&columns, expr)
	return columns
}

// andTerms splits a predicate into a list of terms connected by AND.
// age = 20 AND (id > 1 AND dept_id = 10)
// -> [age = 20, id > 1, dept_id = 10]
func andTerms(expr binder.BoundExpression) []binder.BoundExpression {
	terms := make([]binder.BoundExpression, 0, 4)
	appendAndTerms(&terms, expr)
	return terms
}

// combineResidualTerms rebuilds a slice of leftover terms into nil, one term,
// or a canonical AND expression.
func combineResidualTerms(terms []binder.BoundExpression) binder.BoundExpression {
	switch len(terms) {
	case 0:
		return nil
	case 1:
		return terms[0]
	default:
		orderAndTerms(terms)
		return binder.BoundLogicalExpr{
			Operator: statement.OpAnd,
			Terms:    terms,
		}
	}
}

// singleTablePredicateOwner reports which table a predicate belongs to when
// every referenced column comes from the same table.
func singleTablePredicateOwner(expr binder.BoundExpression) (string, bool) {
	columns := collectPredicateColumns(expr)
	if len(columns) == 0 {
		return "", false
	}

	tableName := columns[0].TableName
	for _, column := range columns[1:] {
		if column.TableName != tableName {
			return "", false
		}
	}

	return tableName, true
}

// logicalPlanTableName peels through simple unary nodes to find the base scan
// table name underneath a join child.
func logicalPlanTableName(plan LogicalPlan) string {
	scan, ok := plan.(LogicalScan)
	if !ok {
		if filter, ok := plan.(LogicalFilter); ok {
			return logicalPlanTableName(filter.Input)
		}
		if sortNode, ok := plan.(LogicalSort); ok {
			return logicalPlanTableName(sortNode.Input)
		}
		return ""
	}
	return scan.Table.Name
}

// logicalPlanOutputWidth returns the row width produced by simple scan/filter/
// sort subtrees so pushed-down join predicates can be rebased to child-local
// ordinals.
func logicalPlanOutputWidth(plan LogicalPlan) (int, bool) {
	switch node := plan.(type) {
	case LogicalScan:
		if node.Table.Metadata == nil {
			return 0, false
		}
		return len(node.Table.Metadata.Columns), true
	case LogicalFilter:
		return logicalPlanOutputWidth(node.Input)
	case LogicalSort:
		return logicalPlanOutputWidth(node.Input)
	default:
		return 0, false
	}
}

// rebaseExpressionOrdinals rewrites joined-row ordinals into child-local
// ordinals after a predicate is pushed below a join.
//
// Example: if right-side joined columns start at offset 3, a right predicate
// using ordinal 4 must become ordinal 1 before it can run on the right child.
func rebaseExpressionOrdinals(expr binder.BoundExpression, offset int) (binder.BoundExpression, bool) {
	switch valueExpr := expr.(type) {
	case nil:
		return nil, true
	case binder.BoundColumnRef:
		if valueExpr.Ordinal < offset {
			return nil, false
		}
		valueExpr.Ordinal -= offset
		return valueExpr, true
	case binder.BoundLiteralExpr:
		return valueExpr, true
	case binder.BoundComparisonExpr:
		left, ok := rebaseExpressionOrdinals(valueExpr.Left, offset)
		if !ok {
			return nil, false
		}
		right, ok := rebaseExpressionOrdinals(valueExpr.Right, offset)
		if !ok {
			return nil, false
		}
		valueExpr.Left = left
		valueExpr.Right = right
		return valueExpr, true
	case binder.BoundLogicalExpr:
		terms := make([]binder.BoundExpression, 0, len(valueExpr.Terms))
		for _, term := range valueExpr.Terms {
			rebased, ok := rebaseExpressionOrdinals(term, offset)
			if !ok {
				return nil, false
			}
			terms = append(terms, rebased)
		}
		valueExpr.Terms = terms
		return valueExpr, true
	default:
		return nil, false
	}
}

// appendPredicateColumns walks a predicate tree and collects every referenced
// column so ownership and pushdown checks can reason about table usage.
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

// containsAggregateExpression reports whether a predicate depends on aggregate
// values and therefore cannot move below an aggregate node.
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

// columnRefKey gives one stable map key for table-qualified columns.
func columnRefKey(column binder.BoundColumnRef) string {
	return column.TableName + "." + column.ColumnName
}

// orderAndTerms keeps predicate terms in a deterministic order so tests are
// stable and equality terms stay ahead of weaker index candidates.
func orderAndTerms(terms []binder.BoundExpression) {
	// [name = 'Bob', age = 20, id > 1]
	// -> [age = 20, id > 1, name = 'Bob']
	sort.SliceStable(terms, func(i, j int) bool {
		return andTermPriority(terms[i]) < andTermPriority(terms[j])
	})
}

// andTermPriority ranks AND terms by how useful they are for early filtering
// and simple index selection.
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
