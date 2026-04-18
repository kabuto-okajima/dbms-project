package executor

import (
	"dbms-project/internal/binder"
	"dbms-project/internal/catalog"
	"dbms-project/internal/planner"
	"dbms-project/internal/statement"
	"dbms-project/internal/storage"
)

// tryBuildIndexScan tries to replace a filter-over-scan shape with one
// PhysicalIndexScan when the predicate has a simple index-eligible term.
func tryBuildIndexScan(tx *storage.Tx, filter planner.LogicalFilter) (PhysicalPlan, bool, error) {
	scanNode, ok := filter.Input.(planner.LogicalScan)
	if !ok {
		return nil, false, nil
	}

	column, literal, operator, residual, ok := indexEligiblePredicate(filter.Predicate)
	if !ok {
		return nil, false, nil
	}

	manager := catalog.NewManager()
	indexes, err := manager.ListIndexesByTable(tx, scanNode.Table.Name)
	if err != nil {
		return nil, false, err
	}
	for _, indexMeta := range indexes {
		if indexMeta.ColumnName != column.ColumnName {
			continue
		}

		return PhysicalIndexScan{
			Table:    scanNode.Table,
			Index:    indexMeta,
			Column:   column,
			Operator: operator,
			Value:    literal.Value,
			Residual: residual,
		}, true, nil
	}

	return nil, false, nil
}

// indexEligiblePredicate finds one index-eligible predicate term and returns
// the remaining terms as a residual predicate.
// e.g., for "WHERE a = 5 AND b > 10 AND c < 20", it might return the "a = 5" term for index scanning and combine the other two terms back into "(b > 10 AND c < 20)" as the residual for full scan.
func indexEligiblePredicate(expr binder.BoundExpression) (binder.BoundColumnRef, binder.BoundLiteralExpr, statement.ComparisonOperator, binder.BoundExpression, bool) {
	if comparison, ok := expr.(binder.BoundComparisonExpr); ok {
		column, literal, operator, ok := indexEligibleComparison(comparison)
		if !ok {
			return binder.BoundColumnRef{}, binder.BoundLiteralExpr{}, "", nil, false
		}
		return column, literal, operator, nil, true
	}

	logicalExpr, ok := expr.(binder.BoundLogicalExpr)
	if !ok || logicalExpr.Operator != statement.OpAnd {
		return binder.BoundColumnRef{}, binder.BoundLiteralExpr{}, "", nil, false
	}

	for i, term := range logicalExpr.Terms {
		comparison, ok := term.(binder.BoundComparisonExpr)
		if !ok {
			continue
		}

		column, literal, operator, ok := indexEligibleComparison(comparison)
		if !ok {
			continue
		}

		residualTerms := make([]binder.BoundExpression, 0, len(logicalExpr.Terms)-1)
		residualTerms = append(residualTerms, logicalExpr.Terms[:i]...)
		residualTerms = append(residualTerms, logicalExpr.Terms[i+1:]...)

		return column, literal, operator, combineResidualTerms(residualTerms), true
	}

	return binder.BoundColumnRef{}, binder.BoundLiteralExpr{}, "", nil, false
}

// combineResidualTerms rebuilds leftover AND terms into one residual predicate.
func combineResidualTerms(terms []binder.BoundExpression) binder.BoundExpression {
	switch len(terms) {
	case 0:
		return nil
	case 1:
		return terms[0]
	default:
		return binder.BoundLogicalExpr{
			Operator: statement.OpAnd,
			Terms:    terms,
		}
	}
}

// indexEligibleComparison normalizes one column-vs-literal comparison into the
// form needed by PhysicalIndexScan.
func indexEligibleComparison(expr binder.BoundComparisonExpr) (binder.BoundColumnRef, binder.BoundLiteralExpr, statement.ComparisonOperator, bool) {
	if column, ok := expr.Left.(binder.BoundColumnRef); ok {
		if literal, ok := expr.Right.(binder.BoundLiteralExpr); ok && isIndexEligibleOperator(expr.Operator) {
			return column, literal, expr.Operator, true
		}
	}

	if literal, ok := expr.Left.(binder.BoundLiteralExpr); ok {
		if column, ok := expr.Right.(binder.BoundColumnRef); ok {
			operator, ok := flippedComparisonOperator(expr.Operator)
			if ok && isIndexEligibleOperator(operator) {
				return column, literal, operator, true
			}
		}
	}

	return binder.BoundColumnRef{}, binder.BoundLiteralExpr{}, "", false
}

// isIndexEligibleOperator reports whether the operator can use an index in v1.
func isIndexEligibleOperator(op statement.ComparisonOperator) bool {
	switch op {
	case statement.OpEqual, statement.OpLessThan, statement.OpLessThanOrEqual, statement.OpGreaterThan, statement.OpGreaterThanOrEqual:
		return true
	default:
		return false
	}
}

// flippedComparisonOperator rewrites literal-vs-column comparisons into the
// equivalent column-vs-literal direction.
func flippedComparisonOperator(op statement.ComparisonOperator) (statement.ComparisonOperator, bool) {
	switch op {
	case statement.OpEqual:
		return statement.OpEqual, true
	case statement.OpLessThan:
		return statement.OpGreaterThan, true
	case statement.OpLessThanOrEqual:
		return statement.OpGreaterThanOrEqual, true
	case statement.OpGreaterThan:
		return statement.OpLessThan, true
	case statement.OpGreaterThanOrEqual:
		return statement.OpLessThanOrEqual, true
	default:
		return "", false
	}
}
