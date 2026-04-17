package binder

import (
	"fmt"

	"dbms-project/internal/catalog"
	"dbms-project/internal/shared"
	"dbms-project/internal/statement"
	"dbms-project/internal/storage"
)

// Binder resolves parser-owned statement shapes against catalog metadata.
type Binder struct {
	Catalog *catalog.Manager
}

// New creates a binder with an optional catalog manager.
func New(manager *catalog.Manager) *Binder {
	return &Binder{Catalog: manager}
}

// BoundSelect is the binder output contract for one SELECT statement.
//
// In this first step, the binder fully resolves FROM tables and leaves the
// expression-level binding work for later steps.
type BoundSelect struct {
	Statement   statement.SelectStatement
	From        []BoundTable
	SelectItems []BoundSelectItem
	Where       BoundExpression
	GroupBy     []BoundExpression
	Having      BoundExpression
	OrderBy     []BoundOrderByTerm
}

// BoundTable describes one FROM entry after semantic table-name resolution.
type BoundTable struct {
	Name     string
	Alias    string
	Metadata *catalog.TableMetadata
}

// BoundExpression is the future expression-level binder contract.
//
// We define it now so later steps can add bound columns, literals, predicates,
// and aggregates without changing the binder package boundary again.
type BoundExpression interface {
	isBoundExpression()
}

// BoundStarExpr represents a resolved "*" expression.
type BoundStarExpr struct{}

func (BoundStarExpr) isBoundExpression() {}

// BoundColumnRef identifies a column after catalog resolution.
type BoundColumnRef struct {
	TableName  string
	ColumnName string
	Ordinal    int
	Type       shared.DataType
}

func (BoundColumnRef) isBoundExpression() {}

// BoundLiteralExpr wraps one typed scalar literal.
type BoundLiteralExpr struct {
	Value storage.Value
}

func (BoundLiteralExpr) isBoundExpression() {}

// BoundComparisonExpr represents a bound binary comparison.
type BoundComparisonExpr struct {
	Left     BoundExpression
	Operator statement.ComparisonOperator
	Right    BoundExpression
}

func (BoundComparisonExpr) isBoundExpression() {}

// BoundLogicalExpr represents a bound same-operator boolean chain.
type BoundLogicalExpr struct {
	Operator statement.LogicalOperator
	Terms    []BoundExpression
}

func (BoundLogicalExpr) isBoundExpression() {}

// BoundAggregateExpr represents a bound aggregate call.
type BoundAggregateExpr struct {
	Function statement.AggregateFunction
	Arg      BoundExpression
	Distinct bool
}

func (BoundAggregateExpr) isBoundExpression() {}

// BoundSelectItem is the future bound form of one SELECT-list item.
type BoundSelectItem struct {
	Expr  BoundExpression
	Alias string
}

// BoundOrderByTerm is the future bound form of one ORDER BY item.
type BoundOrderByTerm struct {
	Expr BoundExpression
	Desc bool
}

type aliasScope map[string]BoundExpression

// BindSelect resolves the catalog metadata needed by a SELECT statement.
func (b *Binder) BindSelect(tx *storage.Tx, stmt statement.SelectStatement) (*BoundSelect, error) {
	if tx == nil {
		return nil, fmt.Errorf("binder: transaction is required")
	}
	if len(stmt.SelectItems) == 0 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "bind select: at least one select item is required")
	}
	if len(stmt.From) == 0 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "bind select: at least one FROM table is required")
	}

	manager := b.Catalog
	if manager == nil {
		manager = catalog.NewManager()
	}

	boundTables := make([]BoundTable, 0, len(stmt.From))
	seenNames := make(map[string]struct{}, len(stmt.From)*2)

	for _, tableRef := range stmt.From {
		if tableRef.Name == "" {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "bind select: table name is required")
		}

		metadata, err := manager.GetTable(tx, tableRef.Name)
		if err != nil {
			return nil, err
		}

		lookupNames := []string{tableRef.Name}
		if tableRef.Alias != "" {
			lookupNames = append(lookupNames, tableRef.Alias)
		}
		// Check for duplicate table names or aliases in the FROM clause.
		for _, name := range lookupNames {
			if _, exists := seenNames[name]; exists {
				return nil, shared.NewError(shared.ErrInvalidDefinition, "bind select: duplicate table name or alias %q", name)
			}
			seenNames[name] = struct{}{}
		}

		boundTables = append(boundTables, BoundTable{
			Name:     tableRef.Name,
			Alias:    tableRef.Alias,
			Metadata: metadata,
		})
	}

	if len(boundTables) != 1 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "bind select: multi-table binding is unsupported")
	}

	boundSelectItems := make([]BoundSelectItem, 0, len(stmt.SelectItems))
	aliases := make(aliasScope, len(stmt.SelectItems))
	for _, item := range stmt.SelectItems {
		// e.g., boundExpr would be BoundColumnRef, BoundLiteralExpr, or BoundAggregateExpr after this call.
		// BoundColumnRef is like SELECT name FROM users, BoundLiteralExpr is like SELECT 42, and BoundAggregateExpr is like SELECT COUNT(*).
		boundExpr, err := bindExpression(boundTables, item.Expr, bindOptions{allowStar: true})
		if err != nil {
			return nil, err
		}
		boundSelectItems = append(boundSelectItems, BoundSelectItem{
			Expr:  boundExpr,
			Alias: item.Alias,
		})

		if err := registerSelectItemAlias(aliases, item.Alias, boundExpr); err != nil {
			return nil, err
		}
	}

	boundWhere, err := bindOptionalExpression(boundTables, stmt.Where)
	if err != nil {
		return nil, err
	}

	boundGroupBy := make([]BoundExpression, 0, len(stmt.GroupBy))
	for _, expr := range stmt.GroupBy {
		boundExpr, err := bindExpression(boundTables, expr, bindOptions{})
		if err != nil {
			return nil, err
		}
		boundGroupBy = append(boundGroupBy, boundExpr)
	}

	boundHaving, err := bindOptionalExpression(boundTables, stmt.Having, bindOptions{allowAliasRefs: true, aliases: aliases})
	if err != nil {
		return nil, err
	}

	boundOrderBy := make([]BoundOrderByTerm, 0, len(stmt.OrderBy))
	for _, term := range stmt.OrderBy {
		boundExpr, err := bindExpression(boundTables, term.Expr, bindOptions{allowAliasRefs: true, aliases: aliases})
		if err != nil {
			return nil, err
		}
		boundOrderBy = append(boundOrderBy, BoundOrderByTerm{
			Expr: boundExpr,
			Desc: term.Desc,
		})
	}

	bound := &BoundSelect{
		Statement:   stmt,
		From:        boundTables,
		SelectItems: boundSelectItems,
		Where:       boundWhere,
		GroupBy:     boundGroupBy,
		Having:      boundHaving,
		OrderBy:     boundOrderBy,
	}

	if err := validateBoundSelect(bound); err != nil {
		return nil, err
	}

	return bound, nil
}

type bindOptions struct {
	allowStar      bool
	allowAliasRefs bool
	aliases        aliasScope
}

func bindOptionalExpression(tables []BoundTable, expr statement.Expression, opts ...bindOptions) (BoundExpression, error) {
	if expr == nil {
		return nil, nil
	}

	if len(opts) == 0 {
		return bindExpression(tables, expr, bindOptions{})
	}

	return bindExpression(tables, expr, opts[0])
}

func bindExpression(tables []BoundTable, expr statement.Expression, opts bindOptions) (BoundExpression, error) {
	switch valueExpr := expr.(type) {
	case statement.StarExpr:
		if !opts.allowStar {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "bind select: * is only valid in SELECT items or COUNT(*)")
		}
		return BoundStarExpr{}, nil
	case statement.ColumnRef:
		if valueExpr.TableName == "" && opts.allowAliasRefs {
			if aliasedExpr, ok := opts.aliases[valueExpr.ColumnName]; ok {
				return aliasedExpr, nil
			}
		}
		return bindColumnRef(tables, valueExpr)
	case statement.LiteralExpr:
		return BoundLiteralExpr{Value: valueExpr.Value}, nil
	case statement.ComparisonExpr:
		left, err := bindExpression(tables, valueExpr.Left, opts)
		if err != nil {
			return nil, err
		}
		right, err := bindExpression(tables, valueExpr.Right, opts)
		if err != nil {
			return nil, err
		}
		return BoundComparisonExpr{
			Left:     left,
			Operator: valueExpr.Operator,
			Right:    right,
		}, nil
	case statement.LogicalExpr:
		terms := make([]BoundExpression, 0, len(valueExpr.Terms))
		for _, term := range valueExpr.Terms {
			boundTerm, err := bindExpression(tables, term, opts)
			if err != nil {
				return nil, err
			}
			terms = append(terms, boundTerm)
		}
		return BoundLogicalExpr{
			Operator: valueExpr.Operator,
			Terms:    terms,
		}, nil
	case statement.AggregateExpr:
		arg, err := bindExpression(tables, valueExpr.Arg, bindOptions{allowStar: valueExpr.Function == statement.AggCount})
		if err != nil {
			return nil, err
		}
		return BoundAggregateExpr{
			Function: valueExpr.Function,
			Arg:      arg,
			Distinct: valueExpr.Distinct,
		}, nil
	default:
		return nil, shared.NewError(shared.ErrInvalidDefinition, "bind select: unsupported expression %T", expr)
	}
}

func registerSelectItemAlias(aliases aliasScope, alias string, expr BoundExpression) error {
	if alias == "" {
		return nil
	}
	if _, exists := aliases[alias]; exists {
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: duplicate select-item alias %q", alias)
	}
	aliases[alias] = expr
	return nil
}

func bindColumnRef(tables []BoundTable, ref statement.ColumnRef) (BoundExpression, error) {
	if len(tables) != 1 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "bind select: column resolution for multiple tables is unsupported")
	}

	table := tables[0]
	if ref.TableName != "" && ref.TableName != table.Name && ref.TableName != table.Alias {
		return nil, shared.NewError(shared.ErrNotFound, "bind select: table or alias %q does not exist in scope", ref.TableName)
	}

	column, ordinal, err := catalog.ColumnByName(table.Metadata, ref.ColumnName)
	if err != nil {
		return nil, err
	}

	return BoundColumnRef{
		TableName:  table.Name,
		ColumnName: column.Name,
		Ordinal:    ordinal,
		Type:       column.Type,
	}, nil
}

// support:
//   - SELECT *
//   - SELECT column
//   - SELECT literal
//   - SELECT COUNT(*), SUM(int_column), MIN(int_column), MAX(int_column)
//   - WHERE with comparisons and conjunction-only or disjunction-only chains
//   - GROUP BY column
//   - HAVING with comparisons and conjunction-only or disjunction-only chains;
//     HAVING requires GROUP BY, and column references must be grouping columns
//     or aggregate expressions
//   - ORDER BY column
//
// unsupported:
//   - mixed AND/OR predicate trees
//   - multi-table queries and JOINs
//   - subqueries
func validateBoundSelect(bound *BoundSelect) error {
	if bound == nil {
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: bound statement is required")
	}

	for _, item := range bound.SelectItems {
		if err := validateExpressionTypes(item.Expr); err != nil {
			return err
		}
	}
	if err := validateOptionalExpressionTypes(bound.Where); err != nil {
		return err
	}
	for _, expr := range bound.GroupBy {
		if err := validateExpressionTypes(expr); err != nil {
			return err
		}
	}
	if err := validateOptionalExpressionTypes(bound.Having); err != nil {
		return err
	}
	for _, term := range bound.OrderBy {
		if err := validateExpressionTypes(term.Expr); err != nil {
			return err
		}
	}

	if err := validateWhereExpression(bound.Where); err != nil {
		return err
	}

	return validateAggregateLegality(bound)
}

func validateOptionalExpressionTypes(expr BoundExpression) error {
	if expr == nil {
		return nil
	}
	return validateExpressionTypes(expr)
}

func validateExpressionTypes(expr BoundExpression) error {
	switch valueExpr := expr.(type) {
	case BoundStarExpr:
		return nil
	case BoundColumnRef:
		return nil
	case BoundLiteralExpr:
		return nil
	case BoundComparisonExpr:
		if err := validateExpressionTypes(valueExpr.Left); err != nil {
			return err
		}
		if err := validateExpressionTypes(valueExpr.Right); err != nil {
			return err
		}

		leftType, err := scalarExpressionType(valueExpr.Left)
		if err != nil {
			return err
		}
		rightType, err := scalarExpressionType(valueExpr.Right)
		if err != nil {
			return err
		}
		if leftType != rightType {
			return shared.NewError(
				shared.ErrTypeMismatch,
				"bind select: comparison operator %q cannot compare %q with %q",
				valueExpr.Operator,
				leftType,
				rightType,
			)
		}
		return nil
	case BoundLogicalExpr:
		for _, term := range valueExpr.Terms {
			if err := validateExpressionTypes(term); err != nil {
				return err
			}
			if !isPredicateExpression(term) {
				return shared.NewError(shared.ErrInvalidDefinition, "bind select: %s terms must be predicate expressions", valueExpr.Operator)
			}
		}
		return nil
	case BoundAggregateExpr:
		switch valueExpr.Function {
		case statement.AggCount:
			if _, ok := valueExpr.Arg.(BoundStarExpr); !ok {
				return shared.NewError(shared.ErrInvalidDefinition, "bind select: only COUNT(*) is supported in v1")
			}
			return nil
		case statement.AggSum, statement.AggMin, statement.AggMax:
			argType, err := scalarExpressionType(valueExpr.Arg)
			if err != nil {
				return err
			}
			if argType != shared.TypeInteger {
				return shared.NewError(
					shared.ErrTypeMismatch,
					"bind select: %s requires an integer argument in v1",
					valueExpr.Function,
				)
			}
			return nil
		default:
			return shared.NewError(shared.ErrInvalidDefinition, "bind select: unsupported aggregate %q", valueExpr.Function)
		}
	default:
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: unsupported bound expression %T", expr)
	}
}

func validateWhereExpression(expr BoundExpression) error {
	if expr == nil {
		return nil
	}
	if !isPredicateExpression(expr) {
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: WHERE must be a predicate expression")
	}
	return nil
}

func validateAggregateLegality(bound *BoundSelect) error {
	hasAggregate := false
	for _, item := range bound.SelectItems {
		if containsAggregate(item.Expr) {
			hasAggregate = true
			break
		}
	}
	if !hasAggregate && containsAggregate(bound.Having) {
		hasAggregate = true
	}

	if bound.Having != nil && len(bound.GroupBy) == 0 {
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: HAVING requires GROUP BY in v1")
	}

	if len(bound.GroupBy) > 0 {
		for _, expr := range bound.GroupBy {
			if _, ok := expr.(BoundColumnRef); !ok {
				return shared.NewError(shared.ErrInvalidDefinition, "bind select: GROUP BY must use column references in v1")
			}
		}
	}

	if !hasAggregate {
		return nil
	}

	if len(bound.GroupBy) == 0 {
		for _, item := range bound.SelectItems {
			if !containsAggregate(item.Expr) {
				return shared.NewError(shared.ErrInvalidDefinition, "bind select: non-aggregated SELECT expressions require GROUP BY")
			}
		}
		return nil
	}

	for _, item := range bound.SelectItems {
		if containsAggregate(item.Expr) {
			continue
		}
		if !matchesGroupBy(bound.GroupBy, item.Expr) {
			return shared.NewError(shared.ErrInvalidDefinition, "bind select: non-aggregated SELECT expressions must appear in GROUP BY")
		}
	}

	if err := validateHavingExpression(bound.Having, bound.GroupBy); err != nil {
		return err
	}

	return nil
}

func validateHavingExpression(expr BoundExpression, groupBy []BoundExpression) error {
	if expr == nil {
		return nil
	}

	switch valueExpr := expr.(type) {
	case BoundColumnRef:
		if !matchesGroupBy(groupBy, valueExpr) {
			return shared.NewError(shared.ErrInvalidDefinition, "bind select: HAVING column references must be grouping columns or aggregates")
		}
		return nil
	case BoundLiteralExpr:
		return nil
	case BoundAggregateExpr:
		return nil
	case BoundComparisonExpr:
		if err := validateHavingExpression(valueExpr.Left, groupBy); err != nil {
			return err
		}
		return validateHavingExpression(valueExpr.Right, groupBy)
	case BoundLogicalExpr:
		for _, term := range valueExpr.Terms {
			if err := validateHavingExpression(term, groupBy); err != nil {
				return err
			}
		}
		return nil
	default:
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: unsupported HAVING expression %T", expr)
	}
}

func scalarExpressionType(expr BoundExpression) (shared.DataType, error) {
	switch valueExpr := expr.(type) {
	case BoundColumnRef:
		return valueExpr.Type, nil
	case BoundLiteralExpr:
		return valueExpr.Value.Type, nil
	case BoundAggregateExpr:
		switch valueExpr.Function {
		case statement.AggCount, statement.AggSum, statement.AggMin, statement.AggMax:
			return shared.TypeInteger, nil
		default:
			return "", shared.NewError(shared.ErrInvalidDefinition, "bind select: unsupported aggregate %q", valueExpr.Function)
		}
	case BoundStarExpr:
		return "", shared.NewError(shared.ErrInvalidDefinition, "bind select: * is not a scalar expression")
	case BoundComparisonExpr, BoundLogicalExpr:
		return "", shared.NewError(shared.ErrInvalidDefinition, "bind select: predicate expressions are not scalar values")
	default:
		return "", shared.NewError(shared.ErrInvalidDefinition, "bind select: unsupported scalar expression %T", expr)
	}
}

func isPredicateExpression(expr BoundExpression) bool {
	switch expr.(type) {
	case BoundComparisonExpr, BoundLogicalExpr:
		return true
	default:
		return false
	}
}

func containsAggregate(expr BoundExpression) bool {
	switch valueExpr := expr.(type) {
	case nil:
		return false
	case BoundAggregateExpr:
		return true
	case BoundComparisonExpr:
		return containsAggregate(valueExpr.Left) || containsAggregate(valueExpr.Right)
	case BoundLogicalExpr:
		for _, term := range valueExpr.Terms {
			if containsAggregate(term) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func matchesGroupBy(groupBy []BoundExpression, expr BoundExpression) bool {
	for _, groupExpr := range groupBy {
		if sameBoundExpression(groupExpr, expr) {
			return true
		}
	}
	return false
}

func sameBoundExpression(left, right BoundExpression) bool {
	switch leftExpr := left.(type) {
	case BoundColumnRef:
		rightExpr, ok := right.(BoundColumnRef)
		if !ok {
			return false
		}
		return leftExpr.TableName == rightExpr.TableName &&
			leftExpr.ColumnName == rightExpr.ColumnName &&
			leftExpr.Ordinal == rightExpr.Ordinal
	case BoundStarExpr:
		_, ok := right.(BoundStarExpr)
		return ok
	case BoundLiteralExpr:
		rightExpr, ok := right.(BoundLiteralExpr)
		if !ok {
			return false
		}
		return leftExpr.Value == rightExpr.Value
	default:
		return false
	}
}
