package executor

import (
	"dbms-project/internal/binder"
	"dbms-project/internal/shared"
	"dbms-project/internal/statement"
	"dbms-project/internal/storage"
	"sort"
)

// RuntimeColumn describes one column in an operator's output schema.
type RuntimeColumn struct {
	Name string
	Type shared.DataType
}

// RuntimeSchema is the ordered output-column list for one physical operator.
type RuntimeSchema []RuntimeColumn

// RuntimeRow is one materialized row flowing between physical operators.
//
// For the first execution steps this wraps the stored row payload directly and
// relies on bound expression ordinals for column access.
type RuntimeRow struct {
	Values     storage.Row
	Aggregates map[string]storage.Value
}

// RuntimeResult is one materialized operator result.
type RuntimeResult struct {
	Schema RuntimeSchema
	Rows   []RuntimeRow
}

// ExecutePlan runs one physical plan and materializes its full result.
func ExecutePlan(tx *storage.Tx, plan PhysicalPlan) (RuntimeResult, error) {
	if tx == nil {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: transaction is required")
	}
	if plan == nil {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: physical plan is required")
	}

	return executePhysicalPlan(tx, plan)
}

func executePhysicalPlan(tx *storage.Tx, plan PhysicalPlan) (RuntimeResult, error) {
	switch node := plan.(type) {
	case PhysicalTableScan:
		return node.Execute(tx)
	case PhysicalIndexScan:
		return node.Execute(tx)
	case PhysicalFilter:
		return node.Execute(tx)
	case PhysicalAggregate:
		return node.Execute(tx)
	case PhysicalProject:
		return node.Execute(tx)
	case PhysicalSort:
		return node.Execute(tx)
	default:
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: execution for physical plan %T is not implemented yet", plan)
	}
}

func (row RuntimeRow) valueAt(ordinal int) (storage.Value, error) {
	if ordinal < 0 || ordinal >= len(row.Values) {
		return storage.Value{}, shared.NewError(
			shared.ErrInvalidDefinition,
			"executor: column ordinal %d is out of range for row width %d",
			ordinal,
			len(row.Values),
		)
	}

	return row.Values[ordinal], nil
}

// EvaluateScalar evaluates one bound scalar expression against a runtime row.
func EvaluateScalar(expr binder.BoundExpression, row RuntimeRow) (storage.Value, error) {
	switch valueExpr := expr.(type) {
	case binder.BoundColumnRef:
		return row.valueAt(valueExpr.Ordinal)
	case binder.BoundLiteralExpr:
		return valueExpr.Value, nil
	case binder.BoundAggregateExpr:
		if row.Aggregates == nil {
			return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "executor: aggregate expressions require aggregate execution context")
		}
		value, ok := row.Aggregates[aggregateSignature(valueExpr)]
		if !ok {
			return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "executor: aggregate value %q is not available in this row", aggregateOutputName(valueExpr))
		}
		return value, nil
	case binder.BoundStarExpr:
		return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "executor: * is not a scalar expression")
	case binder.BoundComparisonExpr, binder.BoundLogicalExpr:
		return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "executor: predicate expressions are not scalar values")
	case nil:
		return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "executor: scalar expression is required")
	default:
		return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "executor: unsupported scalar expression %T", expr)
	}
}

// EvaluatePredicate evaluates one bound predicate expression against a runtime row.
func EvaluatePredicate(expr binder.BoundExpression, row RuntimeRow) (bool, error) {
	switch valueExpr := expr.(type) {
	case binder.BoundComparisonExpr:
		left, err := EvaluateScalar(valueExpr.Left, row)
		if err != nil {
			return false, err
		}
		right, err := EvaluateScalar(valueExpr.Right, row)
		if err != nil {
			return false, err
		}
		return compareValues(left, valueExpr.Operator, right)
	case binder.BoundLogicalExpr:
		if len(valueExpr.Terms) == 0 {
			return false, shared.NewError(shared.ErrInvalidDefinition, "executor: logical predicate must contain at least one term")
		}

		switch valueExpr.Operator {
		case statement.OpAnd:
			for _, term := range valueExpr.Terms {
				ok, err := EvaluatePredicate(term, row)
				if err != nil {
					return false, err
				}
				if !ok {
					return false, nil
				}
			}
			return true, nil
		case statement.OpOr:
			for _, term := range valueExpr.Terms {
				ok, err := EvaluatePredicate(term, row)
				if err != nil {
					return false, err
				}
				if ok {
					return true, nil
				}
			}
			return false, nil
		default:
			return false, shared.NewError(shared.ErrInvalidDefinition, "executor: unsupported logical operator %q", valueExpr.Operator)
		}
	case nil:
		return false, shared.NewError(shared.ErrInvalidDefinition, "executor: predicate expression is required")
	default:
		return false, shared.NewError(shared.ErrInvalidDefinition, "executor: unsupported predicate expression %T", expr)
	}
}

func compareValues(left storage.Value, op statement.ComparisonOperator, right storage.Value) (bool, error) {
	if left.Type != right.Type {
		return false, shared.NewError(
			shared.ErrTypeMismatch,
			"executor: comparison operator %q cannot compare %q with %q",
			op,
			left.Type,
			right.Type,
		)
	}

	switch left.Type {
	case shared.TypeInteger:
		return compareIntegers(left.IntegerValue, op, right.IntegerValue)
	case shared.TypeString:
		return compareStrings(left.StringValue, op, right.StringValue)
	default:
		return false, shared.NewError(shared.ErrInvalidDefinition, "executor: unsupported comparison type %q", left.Type)
	}
}

func compareIntegers(left int64, op statement.ComparisonOperator, right int64) (bool, error) {
	switch op {
	case statement.OpEqual:
		return left == right, nil
	case statement.OpNotEqual:
		return left != right, nil
	case statement.OpLessThan:
		return left < right, nil
	case statement.OpLessThanOrEqual:
		return left <= right, nil
	case statement.OpGreaterThan:
		return left > right, nil
	case statement.OpGreaterThanOrEqual:
		return left >= right, nil
	default:
		return false, shared.NewError(shared.ErrInvalidDefinition, "executor: unsupported comparison operator %q", op)
	}
}

func compareStrings(left string, op statement.ComparisonOperator, right string) (bool, error) {
	switch op {
	case statement.OpEqual:
		return left == right, nil
	case statement.OpNotEqual:
		return left != right, nil
	case statement.OpLessThan:
		return left < right, nil
	case statement.OpLessThanOrEqual:
		return left <= right, nil
	case statement.OpGreaterThan:
		return left > right, nil
	case statement.OpGreaterThanOrEqual:
		return left >= right, nil
	default:
		return false, shared.NewError(shared.ErrInvalidDefinition, "executor: unsupported comparison operator %q", op)
	}
}

// -1 if left < right, 0 if left == right, 1 if left > right
// e.g., compareForOrdering(left=10, right=5) returns 1, compareForOrdering(left='foo', right='bar') returns 1, compareForOrdering(left=42, right=42) returns 0.
func compareForOrdering(left storage.Value, right storage.Value) (int, error) {
	if left.Type != right.Type {
		return 0, shared.NewError(
			shared.ErrTypeMismatch,
			"executor: cannot order %q with %q",
			left.Type,
			right.Type,
		)
	}

	switch left.Type {
	case shared.TypeInteger:
		switch {
		case left.IntegerValue < right.IntegerValue:
			return -1, nil
		case left.IntegerValue > right.IntegerValue:
			return 1, nil
		default:
			return 0, nil
		}
	case shared.TypeString:
		switch {
		case left.StringValue < right.StringValue:
			return -1, nil
		case left.StringValue > right.StringValue:
			return 1, nil
		default:
			return 0, nil
		}
	default:
		return 0, shared.NewError(shared.ErrInvalidDefinition, "executor: unsupported ordering type %q", left.Type)
	}
}

func aggregateSignature(expr binder.BoundAggregateExpr) string {
	return aggregateOutputName(expr)
}

func aggregateOutputName(expr binder.BoundAggregateExpr) string {
	switch arg := expr.Arg.(type) {
	case binder.BoundStarExpr:
		return string(expr.Function) + "(*)"
	case binder.BoundColumnRef:
		return string(expr.Function) + "(" + arg.ColumnName + ")"
	default:
		return string(expr.Function) + "(expr)"
	}
}

// Execute materializes all rows from the table scan's base relation.
func (scan PhysicalTableScan) Execute(tx *storage.Tx) (RuntimeResult, error) {
	if tx == nil {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: transaction is required")
	}
	if scan.Table.Metadata == nil {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: table metadata is required for scan of %q", scan.Table.Name)
	}

	result := RuntimeResult{
		Schema: make(RuntimeSchema, 0, len(scan.Table.Metadata.Columns)),
		Rows:   make([]RuntimeRow, 0),
	}
	for _, column := range scan.Table.Metadata.Columns {
		result.Schema = append(result.Schema, RuntimeColumn{
			Name: column.Name,
			Type: column.Type,
		})
	}

	// Main loop: decode all rows from the table and append them to the result.
	err := tx.ForEach(scan.Table.Metadata.TableBucket, func(_ []byte, value []byte) error {
		row, err := storage.DecodeRow(value)
		if err != nil {
			return err
		}
		result.Rows = append(result.Rows, RuntimeRow{Values: row})
		return nil
	})
	if err != nil {
		return RuntimeResult{}, err
	}

	return result, nil
}

// Execute reads matching row IDs from the index, then loads the base rows.
func (scan PhysicalIndexScan) Execute(tx *storage.Tx) (RuntimeResult, error) {
	if tx == nil {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: transaction is required")
	}
	if scan.Table.Metadata == nil {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: table metadata is required for index scan of %q", scan.Table.Name)
	}
	if scan.Index.IndexBucket == "" {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: index metadata is required for index scan")
	}

	result := RuntimeResult{
		Schema: make(RuntimeSchema, 0, len(scan.Table.Metadata.Columns)),
		Rows:   make([]RuntimeRow, 0),
	}
	for _, column := range scan.Table.Metadata.Columns {
		result.Schema = append(result.Schema, RuntimeColumn{
			Name: column.Name,
			Type: column.Type,
		})
	}

	rids, err := scan.matchingIndexRIDs(tx)
	if err != nil {
		return RuntimeResult{}, err
	}
	for _, rid := range rids {
		payload, err := tx.Get(scan.Table.Metadata.TableBucket, storage.EncodeRID(rid))
		if err != nil {
			return RuntimeResult{}, err
		}
		if payload == nil {
			continue
		}

		row, err := storage.DecodeRow(payload)
		if err != nil {
			return RuntimeResult{}, err
		}
		runtimeRow := RuntimeRow{Values: row}
		if scan.Residual != nil {
			ok, err := EvaluatePredicate(scan.Residual, runtimeRow)
			if err != nil {
				return RuntimeResult{}, err
			}
			if !ok {
				continue
			}
		}
		result.Rows = append(result.Rows, runtimeRow)
	}

	return result, nil
}

// Execute materializes the filter input and keeps only rows whose predicate is true.
func (filter PhysicalFilter) Execute(tx *storage.Tx) (RuntimeResult, error) {
	if tx == nil {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: transaction is required")
	}
	if filter.Predicate == nil {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: filter predicate is required")
	}

	input, err := executePhysicalPlan(tx, filter.Input)
	if err != nil {
		return RuntimeResult{}, err
	}

	result := RuntimeResult{
		Schema: append(RuntimeSchema(nil), input.Schema...),
		Rows:   make([]RuntimeRow, 0, len(input.Rows)),
	}
	// Main loop: evaluate the predicate for each input row.
	// filter.Predicate is like: "WHERE column1 > 5 AND column2 = 'foo'"
	// row is like: [column1=10, column2='foo', column3=42]
	for _, row := range input.Rows {
		ok, err := EvaluatePredicate(filter.Predicate, row)
		if err != nil {
			return RuntimeResult{}, err
		}
		if ok {
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}

// matchingIndexRIDs returns the list of row IDs that match the index scan condition.
// For equality scans, this looks up the index key directly. For range scans, this iterates over all index keys and collects matching row IDs.
func (scan PhysicalIndexScan) matchingIndexRIDs(tx *storage.Tx) ([]storage.RID, error) {
	if scan.Operator == statement.OpEqual {
		indexKey, err := storage.EncodeIndexKey(scan.Value)
		if err != nil {
			return nil, err
		}
		data, err := tx.Get(scan.Index.IndexBucket, indexKey)
		if err != nil {
			return nil, err
		}
		return storage.DecodeRIDList(data)
	}

	rids := make([]storage.RID, 0)
	err := tx.ForEach(scan.Index.IndexBucket, func(key, value []byte) error {
		indexValue, err := storage.DecodeIndexKey(key)
		if err != nil {
			return err
		}
		matches, err := compareValues(indexValue, scan.Operator, scan.Value)
		if err != nil {
			return err
		}
		if !matches {
			return nil
		}

		matchedRIDs, err := storage.DecodeRIDList(value)
		if err != nil {
			return err
		}
		rids = append(rids, matchedRIDs...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return rids, nil
}

// Execute materializes the child result and reshapes it to the project's output schema.
func (project PhysicalProject) Execute(tx *storage.Tx) (RuntimeResult, error) {
	if tx == nil {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: transaction is required")
	}
	if len(project.Outputs) == 0 {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: project outputs are required")
	}

	input, err := executePhysicalPlan(tx, project.Input)
	if err != nil {
		return RuntimeResult{}, err
	}

	result := RuntimeResult{
		Schema: make(RuntimeSchema, 0, len(project.Outputs)),
		Rows:   make([]RuntimeRow, 0, len(input.Rows)),
	}

	// build the output columns for the result schema.
	for _, output := range project.Outputs {
		if output.ExpandsStar {
			result.Schema = append(result.Schema, input.Schema...)
			continue
		}
		result.Schema = append(result.Schema, RuntimeColumn{
			Name: output.Name,
			Type: output.Type,
		})
	}

	// Main loop: evaluate each output expression for each input row and append the projected row to the result.
	for _, inputRow := range input.Rows {
		projectedValues := make(storage.Row, 0, len(result.Schema))
		for _, output := range project.Outputs {
			if output.ExpandsStar {
				projectedValues = append(projectedValues, inputRow.Values...)
				continue
			}

			// e.g., output.Expr is like "column1 + column3" and inputRow is like [column1=10, column2='foo', column3=42]
			value, err := EvaluateScalar(output.Expr, inputRow)
			if err != nil {
				return RuntimeResult{}, err
			}
			projectedValues = append(projectedValues, value)
		}
		result.Rows = append(result.Rows, RuntimeRow{Values: projectedValues})
	}

	// e.g., if project.Outputs is like ["column1 + column3 AS sum", "column2"] then result.Schema is like [sum: integer, column2: string] and each projected row is like [sum=52, column2='foo'].
	return result, nil
}

// Execute materializes the child result and sorts all rows in memory.
func (sortNode PhysicalSort) Execute(tx *storage.Tx) (RuntimeResult, error) {
	if tx == nil {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: transaction is required")
	}
	if len(sortNode.OrderBy) == 0 {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: sort keys are required")
	}

	input, err := executePhysicalPlan(tx, sortNode.Input)
	if err != nil {
		return RuntimeResult{}, err
	}

	result := RuntimeResult{
		Schema: append(RuntimeSchema(nil), input.Schema...),
		Rows:   append([]RuntimeRow(nil), input.Rows...),
	}

	var compareErr error
	sort.SliceStable(result.Rows, func(i, j int) bool {
		if compareErr != nil {
			return false
		}

		leftRow := result.Rows[i]
		rightRow := result.Rows[j]

		for _, key := range sortNode.OrderBy {
			// e.g., key.Expr is like "column1 + column3" and leftRow is like [column1=10, column2='foo', column3=42] and rightRow is like [column1=5, column2='bar', column3=20]
			leftValue, err := EvaluateScalar(key.Expr, leftRow)
			if err != nil {
				compareErr = err
				return false
			}
			rightValue, err := EvaluateScalar(key.Expr, rightRow)
			if err != nil {
				compareErr = err
				return false
			}

			ordering, err := compareForOrdering(leftValue, rightValue)
			if err != nil {
				compareErr = err
				return false
			}
			if ordering == 0 {
				continue
			}
			if key.Desc {
				return ordering > 0
			}
			return ordering < 0
		}

		return false
	})
	if compareErr != nil {
		return RuntimeResult{}, compareErr
	}

	return result, nil
}

// Execute materializes the child result and computes aggregate values.
//
// This first version supports aggregate execution without GROUP BY.
func (aggregate PhysicalAggregate) Execute(tx *storage.Tx) (RuntimeResult, error) {
	if tx == nil {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: transaction is required")
	}
	if len(aggregate.Aggregates) == 0 {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: aggregate expressions are required")
	}

	input, err := executePhysicalPlan(tx, aggregate.Input)
	if err != nil {
		return RuntimeResult{}, err
	}

	if len(aggregate.GroupBy) > 0 {
		return executeGroupedAggregate(aggregate, input)
	}

	row := RuntimeRow{
		Values:     nil,
		Aggregates: make(map[string]storage.Value, len(aggregate.Aggregates)),
	}
	for _, expr := range aggregate.Aggregates {
		value, err := executeAggregateExpr(expr, input.Rows)
		if err != nil {
			return RuntimeResult{}, err
		}
		row.Aggregates[aggregateSignature(expr)] = value
	}

	return RuntimeResult{
		Rows: []RuntimeRow{row},
	}, nil
}

func executeAggregateExpr(expr binder.BoundAggregateExpr, rows []RuntimeRow) (storage.Value, error) {
	switch expr.Function {
	case statement.AggCount:
		return storage.NewIntegerValue(int64(len(rows))), nil
	case statement.AggSum:
		if len(rows) == 0 {
			return storage.NewIntegerValue(0), nil
		}

		var sum int64
		for _, row := range rows {
			value, err := EvaluateScalar(expr.Arg, row)
			if err != nil {
				return storage.Value{}, err
			}
			if value.Type != shared.TypeInteger {
				return storage.Value{}, shared.NewError(shared.ErrTypeMismatch, "executor: SUM requires integer input")
			}
			sum += value.IntegerValue
		}
		return storage.NewIntegerValue(sum), nil
	case statement.AggMin:
		if len(rows) == 0 {
			return storage.NewIntegerValue(0), nil
		}

		best, err := EvaluateScalar(expr.Arg, rows[0])
		if err != nil {
			return storage.Value{}, err
		}
		for _, row := range rows[1:] {
			value, err := EvaluateScalar(expr.Arg, row)
			if err != nil {
				return storage.Value{}, err
			}
			ordering, err := compareForOrdering(value, best)
			if err != nil {
				return storage.Value{}, err
			}
			if ordering < 0 {
				best = value
			}
		}
		return best, nil
	case statement.AggMax:
		if len(rows) == 0 {
			return storage.NewIntegerValue(0), nil
		}

		best, err := EvaluateScalar(expr.Arg, rows[0])
		if err != nil {
			return storage.Value{}, err
		}
		for _, row := range rows[1:] {
			value, err := EvaluateScalar(expr.Arg, row)
			if err != nil {
				return storage.Value{}, err
			}
			ordering, err := compareForOrdering(value, best)
			if err != nil {
				return storage.Value{}, err
			}
			if ordering > 0 {
				best = value
			}
		}
		return best, nil
	default:
		return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "executor: aggregate %q execution is not implemented yet", expr.Function)
	}
}

func executeGroupedAggregate(aggregate PhysicalAggregate, input RuntimeResult) (RuntimeResult, error) {
	// Only one grouping column is supported. No composite grouping or grouping sets yet.
	if len(aggregate.GroupBy) != 1 {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: only one-column GROUP BY is implemented")
	}

	// e.g., groupExpr is like "GROUP BY dept_id" and groupRef is like the column reference for dept_id with ordinal 1.
	groupExpr := aggregate.GroupBy[0]
	groupRef, ok := groupExpr.(binder.BoundColumnRef)
	if !ok {
		return RuntimeResult{}, shared.NewError(shared.ErrInvalidDefinition, "executor: GROUP BY expressions must be column references")
	}

	type groupState struct {
		values     storage.Row
		aggregates map[string]storage.Value
	}

	// Example input:
	// 	dept_id   age
	// 	10        20
	// 	20        30
	// 	10        25
	// query: "SELECT dept_id, COUNT(*), SUM(age) FROM employee GROUP BY dept_id"
	//
	// groups keeps one running state per key.
	// order becomes [10, 20].
	groups := make(map[string]*groupState, len(input.Rows))
	order := make([]string, 0, len(input.Rows))

	for _, row := range input.Rows {
		// Example: row dept_id=10 -> groupValue=10.
		groupValue, err := EvaluateScalar(groupExpr, row)
		if err != nil {
			return RuntimeResult{}, err
		}
		keyBytes, err := storage.EncodeIndexKey(groupValue)
		if err != nil {
			return RuntimeResult{}, err
		}
		key := string(keyBytes)

		state, exists := groups[key]
		if !exists {
			// New group for 10:
			// values   = [_, 10]
			// aggs     = {"COUNT(*)": 0, "SUM(age)": 0}
			// Ordinal 1 still stores dept_id.
			values := make(storage.Row, len(input.Schema))
			values[groupRef.Ordinal] = groupValue
			state = &groupState{
				values:     values,
				aggregates: make(map[string]storage.Value, len(aggregate.Aggregates)),
			}
			for _, expr := range aggregate.Aggregates {
				initial, err := initialGroupedAggregateValue(expr)
				if err != nil {
					return RuntimeResult{}, err
				}
				state.aggregates[aggregateSignature(expr)] = initial
			}
			groups[key] = state
			order = append(order, key)
		}

		// Row in group 10:
		// COUNT(*) 0 -> 1 -> 2
		// SUM(age) 0 -> 20 -> 45
		for _, expr := range aggregate.Aggregates {
			value, err := updateGroupedAggregateValue(expr, state.aggregates[aggregateSignature(expr)], row)
			if err != nil {
				return RuntimeResult{}, err
			}
			state.aggregates[aggregateSignature(expr)] = value
		}
	}

	result := RuntimeResult{
		Schema: append(RuntimeSchema(nil), input.Schema...),
		Rows:   make([]RuntimeRow, 0, len(order)),
	}
	for _, key := range order {
		state := groups[key]
		// Output row for group 10:
		// Values     = [_, 10]
		// Aggregates = {"COUNT(*)": 2}
		result.Rows = append(result.Rows, RuntimeRow{
			Values:     state.values,
			Aggregates: state.aggregates,
		})
	}

	return result, nil
}

func initialGroupedAggregateValue(expr binder.BoundAggregateExpr) (storage.Value, error) {
	switch expr.Function {
	case statement.AggCount, statement.AggSum:
		return storage.NewIntegerValue(0), nil
	case statement.AggMin, statement.AggMax:
		return storage.Value{}, nil
	default:
		return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "executor: grouped aggregate %q is not implemented yet", expr.Function)
	}
}

func updateGroupedAggregateValue(expr binder.BoundAggregateExpr, current storage.Value, row RuntimeRow) (storage.Value, error) {
	switch expr.Function {
	case statement.AggCount:
		return storage.NewIntegerValue(current.IntegerValue + 1), nil
	case statement.AggSum:
		value, err := EvaluateScalar(expr.Arg, row)
		if err != nil {
			return storage.Value{}, err
		}
		if value.Type != shared.TypeInteger {
			return storage.Value{}, shared.NewError(shared.ErrTypeMismatch, "executor: SUM requires integer input")
		}
		return storage.NewIntegerValue(current.IntegerValue + value.IntegerValue), nil
	case statement.AggMin:
		value, err := EvaluateScalar(expr.Arg, row)
		if err != nil {
			return storage.Value{}, err
		}
		if current.Type == "" {
			return value, nil
		}
		ordering, err := compareForOrdering(value, current)
		if err != nil {
			return storage.Value{}, err
		}
		if ordering < 0 {
			return value, nil
		}
		return current, nil
	case statement.AggMax:
		value, err := EvaluateScalar(expr.Arg, row)
		if err != nil {
			return storage.Value{}, err
		}
		if current.Type == "" {
			return value, nil
		}
		ordering, err := compareForOrdering(value, current)
		if err != nil {
			return storage.Value{}, err
		}
		if ordering > 0 {
			return value, nil
		}
		return current, nil
	default:
		return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "executor: grouped aggregate %q is not implemented yet", expr.Function)
	}
}
