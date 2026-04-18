package executor

import (
	"dbms-project/internal/planner"
	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

// BuildPhysicalPlan lowers a logical SELECT plan into the first physical plan
// shape supported by the executor.
//
// This version chooses an index scan for the simplest eligible filter shape:
// one indexed column compared against one literal.
func BuildPhysicalPlan(tx *storage.Tx, plan planner.LogicalPlan) (PhysicalPlan, error) {
	if plan == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "executor: logical plan is required")
	}
	if tx == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "executor: transaction is required")
	}

	return buildPhysicalPlan(tx, plan)
}

func buildPhysicalPlan(tx *storage.Tx, plan planner.LogicalPlan) (PhysicalPlan, error) {
	switch node := plan.(type) {
	case planner.LogicalScan:
		return PhysicalTableScan{
			Table: node.Table,
		}, nil
	case planner.LogicalJoin:
		left, err := buildPhysicalPlan(tx, node.Left)
		if err != nil {
			return nil, err
		}
		right, err := buildPhysicalPlan(tx, node.Right)
		if err != nil {
			return nil, err
		}
		return PhysicalNestedLoopJoin{
			Left:      left,
			Right:     right,
			Predicate: node.Predicate,
		}, nil
	case planner.LogicalFilter:
		// If index-based scanning is possible, use it instead of a filter over a table scan.
		indexScan, ok, err := tryBuildIndexScan(tx, node)
		if err != nil {
			return nil, err
		}
		if ok {
			return indexScan, nil
		}

		input, err := buildPhysicalPlan(tx, node.Input)
		if err != nil {
			return nil, err
		}
		return PhysicalFilter{
			Input:     input,
			Predicate: node.Predicate,
		}, nil
	case planner.LogicalAggregate:
		input, err := buildPhysicalPlan(tx, node.Input)
		if err != nil {
			return nil, err
		}
		return PhysicalAggregate{
			Input:      input,
			GroupBy:    node.GroupBy,
			Aggregates: node.Aggregates,
		}, nil
	case planner.LogicalProject:
		input, err := buildPhysicalPlan(tx, node.Input)
		if err != nil {
			return nil, err
		}

		outputs := make([]PhysicalOutput, 0, len(node.Outputs))
		for _, output := range node.Outputs {
			outputs = append(outputs, PhysicalOutput{
				Name:        output.Name,
				Expr:        output.Expr,
				Type:        output.Type,
				ExpandsStar: output.ExpandsStar,
			})
		}

		return PhysicalProject{
			Input:   input,
			Outputs: outputs,
		}, nil
	case planner.LogicalSort:
		input, err := buildPhysicalPlan(tx, node.Input)
		if err != nil {
			return nil, err
		}

		orderBy := make([]PhysicalSortKey, 0, len(node.OrderBy))
		for _, key := range node.OrderBy {
			orderBy = append(orderBy, PhysicalSortKey{
				Expr: key.Expr,
				Desc: key.Desc,
				Type: key.Type,
			})
		}

		return PhysicalSort{
			Input:   input,
			OrderBy: orderBy,
		}, nil
	default:
		return nil, shared.NewError(shared.ErrInvalidDefinition, "executor: unsupported logical plan %T", plan)
	}
}
