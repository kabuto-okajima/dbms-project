package binder

import (
	"errors"
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

// BindTablePredicate resolves one optional predicate expression against a
// single table for DML statements such as DELETE and UPDATE.
func (b *Binder) BindTablePredicate(tx *storage.Tx, tableName string, expr statement.Expression) (BoundExpression, error) {
	if tx == nil {
		return nil, fmt.Errorf("binder: transaction is required")
	}
	if tableName == "" {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "bind predicate: table name is required")
	}

	// example:
	// for DELETE FROM users WHERE id = 42, tableName is "users" and expr is the parsed form of "id = 42".
	// for UPDATE users SET name = 'Alice' WHERE id = 42, tableName is "users" and expr is the parsed form of "id = 42".
	tables, err := b.bindTables(tx, []statement.TableRef{{Name: tableName}})
	if err != nil {
		return nil, err
	}

	boundExpr, err := bindOptionalExpression(tables, expr)
	if err != nil {
		return nil, err
	}
	if err := validateOptionalExpressionTypes(boundExpr); err != nil {
		return nil, err
	}
	if err := validateWhereExpression(boundExpr); err != nil {
		return nil, err
	}
	if containsAggregate(boundExpr) {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "bind predicate: aggregates are unsupported in DML WHERE clauses")
	}

	return boundExpr, nil
}

// BoundSelect is the binder output contract for one SELECT statement.
//
// In this first step, the binder fully resolves FROM tables and leaves the
// expression-level binding work for later steps.
type BoundSelect struct {
	Statement   statement.SelectStatement
	From        []BoundTable
	Join        *BoundJoinClause
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

// BoundJoinClause stores the bound ON predicate for one explicit JOIN.
type BoundJoinClause struct {
	Type statement.JoinType
	On   BoundExpression
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
	BindingName string // e.g., "users" or "u" if the table is aliased as "u"
	TableName   string
	ColumnName  string
	Ordinal     int
	Type        shared.DataType
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

	boundTables, err := b.bindTables(tx, stmt.From)
	if err != nil {
		return nil, err
	}

	if err := validateFromClauseShape(stmt); err != nil {
		return nil, err
	}

	boundJoin, err := bindOptionalJoin(boundTables, stmt.Join)
	if err != nil {
		return nil, err
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
		Join:        boundJoin,
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

// bindTables resolves one or more table references in the FROM clause against the catalog.
func (b *Binder) bindTables(tx *storage.Tx, refs []statement.TableRef) ([]BoundTable, error) {
	manager := b.Catalog
	if manager == nil {
		manager = catalog.NewManager()
	}

	boundTables := make([]BoundTable, 0, len(refs))
	seenNames := make(map[string]struct{}, len(refs)*2)

	for _, tableRef := range refs {
		if tableRef.Name == "" {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "bind select: table name is required")
		}

		metadata, err := manager.GetTable(tx, tableRef.Name)
		if err != nil {
			return nil, err
		}

		visibleName := tableBindingName(tableRef.Name, tableRef.Alias)
		if _, exists := seenNames[visibleName]; exists {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "bind select: duplicate table name or alias %q", visibleName)
		}
		seenNames[visibleName] = struct{}{}

		boundTables = append(boundTables, BoundTable{
			Name:     tableRef.Name,
			Alias:    tableRef.Alias,
			Metadata: metadata,
		})
	}

	return boundTables, nil
}

func tableBindingName(baseName, alias string) string {
	if alias != "" {
		return alias
	}
	return baseName
}

func validateFromClauseShape(stmt statement.SelectStatement) error {
	if stmt.Join == nil {
		if len(stmt.From) != 1 {
			return shared.NewError(shared.ErrInvalidDefinition, "bind select: exactly one FROM table is required when JOIN is absent")
		}
		return nil
	}

	if len(stmt.From) != 2 {
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: exactly two FROM tables are required when JOIN is present")
	}

	return nil
}

func bindOptionalJoin(tables []BoundTable, join *statement.JoinClause) (*BoundJoinClause, error) {
	if join == nil {
		return nil, nil
	}

	// For JOIN binding, expression is like:
	// BoundJoinClause{
	//   Type: INNER,
	//   On: BoundComparisonExpr{
	//     Left: BoundColumnRef{users.id},
	//     Operator: "=",
	//     Right: BoundColumnRef{departments.user_id},
	//   },
	// }
	boundOn, err := bindOptionalExpression(tables, join.On)
	if err != nil {
		return nil, err
	}

	return &BoundJoinClause{
		Type: join.Type,
		On:   boundOn,
	}, nil
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
	if ref.TableName != "" {
		return bindQualifiedColumnRef(tables, ref)
	}
	return bindUnqualifiedColumnRef(tables, ref.ColumnName)
}

func bindQualifiedColumnRef(tables []BoundTable, ref statement.ColumnRef) (BoundExpression, error) {
	tableIndex, table, ok := findBoundTableByName(tables, ref.TableName)
	if !ok {
		return nil, shared.NewError(shared.ErrNotFound, "bind select: table or alias %q does not exist in scope", ref.TableName)
	}

	return bindColumnRefFromTable(tables, tableIndex, table, ref.ColumnName)
}

func bindUnqualifiedColumnRef(tables []BoundTable, columnName string) (BoundExpression, error) {
	var (
		match   BoundColumnRef
		matched bool
	)
	for tableIndex, table := range tables {
		boundColumn, found, err := tryBindColumnRefFromTable(tables, tableIndex, table, columnName)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}

		if matched {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "bind select: column %q is ambiguous across tables", columnName)
		}

		match = boundColumn
		matched = true
	}

	if !matched {
		return nil, shared.NewError(shared.ErrNotFound, "bind select: column %q does not exist in scope", columnName)
	}

	return match, nil
}

func findBoundTableByName(tables []BoundTable, name string) (int, BoundTable, bool) {
	for tableIndex, table := range tables {
		if name == tableBindingName(table.Name, table.Alias) {
			return tableIndex, table, true
		}
	}
	return 0, BoundTable{}, false
}

func tryBindColumnRefFromTable(tables []BoundTable, tableIndex int, table BoundTable, columnName string) (BoundColumnRef, bool, error) {
	boundColumn, err := bindColumnRefFromTable(tables, tableIndex, table, columnName)
	if err != nil {
		if errors.Is(err, shared.ErrNotFound) {
			return BoundColumnRef{}, false, nil
		}
		return BoundColumnRef{}, false, err
	}
	return boundColumn, true, nil
}

func bindColumnRefFromTable(tables []BoundTable, tableIndex int, table BoundTable, columnName string) (BoundColumnRef, error) {
	column, ordinal, err := catalog.ColumnByName(table.Metadata, columnName)
	if err != nil {
		return BoundColumnRef{}, err
	}

	return BoundColumnRef{
		BindingName: tableBindingName(table.Name, table.Alias),
		TableName:   table.Name,
		ColumnName:  column.Name,
		Ordinal:     globalColumnOrdinal(tables, tableIndex, ordinal),
		Type:        column.Type,
	}, nil
}

func globalColumnOrdinal(tables []BoundTable, tableIndex int, tableOrdinal int) int {
	ordinal := tableOrdinal
	for i := 0; i < tableIndex; i++ {
		ordinal += len(tables[i].Metadata.Columns)
	}
	return ordinal
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
//   - non-equi joins and non-column join keys
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
	if bound.Join != nil {
		if err := validateOptionalExpressionTypes(bound.Join.On); err != nil {
			return err
		}
	}

	if err := validateWhereExpression(bound.Where); err != nil {
		return err
	}
	if err := validateJoinClause(bound.Join); err != nil {
		return err
	}

	return validateAggregateLegality(bound)
}

func validateJoinClause(join *BoundJoinClause) error {
	if join == nil {
		return nil
	}
	if join.Type != statement.JoinInner {
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: only INNER JOIN is supported")
	}
	if join.On == nil {
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: JOIN ON predicate is required")
	}

	comparison, ok := join.On.(BoundComparisonExpr)
	if !ok {
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: JOIN ON must be a single comparison predicate")
	}
	if comparison.Operator != statement.OpEqual {
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: only equi-join predicates are supported")
	}

	leftColumn, ok := comparison.Left.(BoundColumnRef)
	if !ok {
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: left JOIN key must be a column reference")
	}
	rightColumn, ok := comparison.Right.(BoundColumnRef)
	if !ok {
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: right JOIN key must be a column reference")
	}
	if leftColumn.BindingName == rightColumn.BindingName {
		return shared.NewError(shared.ErrInvalidDefinition, "bind select: JOIN keys must come from different tables")
	}

	return nil
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
