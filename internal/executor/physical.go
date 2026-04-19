package executor

import (
	"dbms-project/internal/binder"
	"dbms-project/internal/catalog"
	"dbms-project/internal/shared"
	"dbms-project/internal/statement"
	"dbms-project/internal/storage"
)

// PhysicalPlan is the execution-facing tree for one SELECT statement.
//
// The first version mirrors the current logical operators one-to-one so we can
// establish a stable planner/executor boundary before adding runtime behavior.
type PhysicalPlan interface {
	isPhysicalPlan()
}

// PhysicalTableScan reads one base table.
type PhysicalTableScan struct {
	Table binder.BoundTable
}

func (PhysicalTableScan) isPhysicalPlan() {}

// PhysicalIndexScan reads matching rows through one single-column index.
type PhysicalIndexScan struct {
	Table    binder.BoundTable
	Index    catalog.IndexMetadata
	Column   binder.BoundColumnRef
	Operator statement.ComparisonOperator
	Value    storage.Value
	Residual binder.BoundExpression
}

func (PhysicalIndexScan) isPhysicalPlan() {}

// PhysicalFilter keeps rows whose predicate evaluates to true.
type PhysicalFilter struct {
	Input     PhysicalPlan
	Predicate binder.BoundExpression
}

func (PhysicalFilter) isPhysicalPlan() {}

// PhysicalNestedLoopJoin combines two inputs using one join predicate.
type PhysicalNestedLoopJoin struct {
	Left      PhysicalPlan
	Right     PhysicalPlan
	Predicate binder.BoundExpression
}

func (PhysicalNestedLoopJoin) isPhysicalPlan() {}

// PhysicalHashJoin combines two inputs using one equi-join key from each side.
type PhysicalHashJoin struct {
	Left      PhysicalPlan
	Right     PhysicalPlan
	LeftKey   binder.BoundColumnRef
	RightKey  binder.BoundColumnRef
	Predicate binder.BoundExpression
}

func (PhysicalHashJoin) isPhysicalPlan() {}

// PhysicalAggregate groups rows and computes aggregate values.
type PhysicalAggregate struct {
	Input      PhysicalPlan
	GroupBy    []binder.BoundExpression
	Aggregates []binder.BoundAggregateExpr
}

func (PhysicalAggregate) isPhysicalPlan() {}

// PhysicalProject defines the final output expressions and names.
type PhysicalProject struct {
	Input   PhysicalPlan
	Outputs []PhysicalOutput
}

func (PhysicalProject) isPhysicalPlan() {}

// PhysicalSort orders rows by one or more scalar expressions.
type PhysicalSort struct {
	Input   PhysicalPlan
	OrderBy []PhysicalSortKey
}

func (PhysicalSort) isPhysicalPlan() {}

// PhysicalOutput describes one projected output column.
type PhysicalOutput struct {
	Name        string
	Expr        binder.BoundExpression
	Type        shared.DataType
	ExpandsStar bool
}

// PhysicalSortKey describes one ORDER BY key.
type PhysicalSortKey struct {
	Expr binder.BoundExpression
	Desc bool
	Type shared.DataType
}
