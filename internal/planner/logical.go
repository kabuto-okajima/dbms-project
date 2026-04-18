package planner

import (
	"dbms-project/internal/binder"
	"dbms-project/internal/shared"
)

// LogicalPlan is the planner's tree representation for one bound SELECT query.
//
// These nodes describe query shape only. They do not execute anything.
type LogicalPlan interface {
	isLogicalPlan()
}

// LogicalScan is the leaf node for reading one base table.
// example:
//   - SELECT * FROM t
//   - LogicalScan{Table: t}
type LogicalScan struct {
	Table binder.BoundTable
}

func (LogicalScan) isLogicalPlan() {}

// LogicalFilter keeps only rows whose predicate evaluates to true.
//
// example:
//   - SELECT * FROM t WHERE a > 5
//   - LogicalFilter{Predicate: a > 5, Input: LogicalScan{Table: t}}
type LogicalFilter struct {
	Input     LogicalPlan
	Predicate binder.BoundExpression
}

func (LogicalFilter) isLogicalPlan() {}

// LogicalJoin combines two input plans using one join predicate.
//
// example:
//   - SELECT * FROM s JOIN d ON s.dept_id = d.id
//   - LogicalJoin{
//     Left: LogicalScan{Table: s},
//     Right: LogicalScan{Table: d},
//     Predicate: s.dept_id = d.id,
//     }
type LogicalJoin struct {
	Left      LogicalPlan
	Right     LogicalPlan
	Predicate binder.BoundExpression
}

func (LogicalJoin) isLogicalPlan() {}

// LogicalAggregate groups rows and computes aggregate values.
//
// example:
//   - SELECT COUNT(*) FROM t GROUP BY a
//   - LogicalAggregate{GroupBy: [a], Aggregates: [COUNT(*)], Input: LogicalScan{Table: t}}
type LogicalAggregate struct {
	Input      LogicalPlan
	GroupBy    []binder.BoundExpression
	Aggregates []binder.BoundAggregateExpr
}

func (LogicalAggregate) isLogicalPlan() {}

// LogicalProject defines the final output expressions and names.
// example:
//   - SELECT name AS student_name FROM students
//   - LogicalProject{Input: LogicalScan{Table: students}, Outputs: [{Name: "student_name", Expr: {TableName: "students", ColumnName: "name", Ordinal: 1, Type: string}, Type: string, ExpandsStar: false}]}
type LogicalProject struct {
	Input   LogicalPlan
	Outputs []LogicalOutput
}

func (LogicalProject) isLogicalPlan() {}

// LogicalSort orders rows by one or more scalar expressions.
type LogicalSort struct {
	Input   LogicalPlan
	OrderBy []LogicalSortKey
}

func (LogicalSort) isLogicalPlan() {}

// LogicalOutput describes one projected output column.
// example:
//   - SELECT a + b AS sum FROM t
//   - LogicalOutput{Name: "sum", Expr: a + b, Type: int, ExpandsStar: false}
type LogicalOutput struct {
	Name        string
	Expr        binder.BoundExpression
	Type        shared.DataType
	ExpandsStar bool
}

// LogicalSortKey describes one logical ORDER BY key.
type LogicalSortKey struct {
	Expr binder.BoundExpression
	Desc bool
	Type shared.DataType
}
