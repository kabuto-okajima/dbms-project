package statement

import "dbms-project/internal/storage"

// SelectStatement is the parser-owned, project-defined SELECT shape that the
// binder will consume.
//
// The parser should populate names and aliases here without resolving them
// against catalog metadata.
type SelectStatement struct {
	SelectItems []SelectItem
	From        []TableRef
	Where       Expression
	GroupBy     []Expression
	Having      Expression
	OrderBy     []OrderByTerm
}

// SelectItem represents one item in the SELECT list.
type SelectItem struct {
	Expr  Expression
	Alias string
}

// TableRef represents one table name in the FROM clause, with an optional
// alias chosen by the query.
type TableRef struct {
	Name  string
	Alias string
}

// OrderByTerm represents one ORDER BY item.
type OrderByTerm struct {
	Expr Expression
	Desc bool
}

// Expression is the project-owned expression tree used by SELECT statements.
//
// These nodes are intentionally parser-agnostic so later layers do not depend
// on Vitess AST types.
type Expression interface {
	isExpression()
}

// StarExpr represents a "*" projection.
type StarExpr struct{}

func (StarExpr) isExpression() {}

// ColumnRef names one column, optionally qualified by table or alias.
type ColumnRef struct {
	TableName  string
	ColumnName string
}

func (ColumnRef) isExpression() {}

// LiteralExpr wraps one scalar literal value.
// e.g., `WHERE age > 30` would have a LiteralExpr with Value=30.
type LiteralExpr struct {
	Value storage.Value
}

func (LiteralExpr) isExpression() {}

// ComparisonOperator identifies a comparison predicate such as "=".
type ComparisonOperator string

const (
	OpEqual              ComparisonOperator = "="
	OpNotEqual           ComparisonOperator = "!="
	OpLessThan           ComparisonOperator = "<"
	OpLessThanOrEqual    ComparisonOperator = "<="
	OpGreaterThan        ComparisonOperator = ">"
	OpGreaterThanOrEqual ComparisonOperator = ">="
)

// ComparisonExpr represents a binary comparison between two expressions.
type ComparisonExpr struct {
	Left     Expression
	Operator ComparisonOperator
	Right    Expression
}

func (ComparisonExpr) isExpression() {}

// LogicalOperator identifies an AND/OR predicate chain.
type LogicalOperator string

const (
	OpAnd LogicalOperator = "AND"
	OpOr  LogicalOperator = "OR"
)

// LogicalExpr represents a same-operator boolean chain.
type LogicalExpr struct {
	Operator LogicalOperator
	Terms    []Expression
}

func (LogicalExpr) isExpression() {}

// AggregateFunction identifies one supported aggregate.
type AggregateFunction string

const (
	AggCount AggregateFunction = "COUNT"
	AggSum   AggregateFunction = "SUM"
	AggMin   AggregateFunction = "MIN"
	AggMax   AggregateFunction = "MAX"
)

// AggregateExpr represents one aggregate call such as COUNT(*) or SUM(age).
type AggregateExpr struct {
	Function AggregateFunction
	Arg      Expression
	Distinct bool
}

func (AggregateExpr) isExpression() {}
