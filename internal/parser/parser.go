package parser

import (
	"fmt"
	"strconv"
	"strings"

	"dbms-project/internal/shared"
	"dbms-project/internal/statement"
	"dbms-project/internal/storage"

	"vitess.io/vitess/go/vt/sqlparser"
)

// Parser flow, as currently intended:
//
//	SQL string
//	  ↓ Parse()
//	  RawStatement (includes Vitess AST)
//	  ↓ RawStatement.Kind()
//	  classified statement kind
//	  ↓ BuildRequest()
//	  project request type for later layers
//	  (CreateTableRequest, InsertRequest, etc.)
//
// The key design choice is to convert out of Vitess AST types inside this
// package, so the rest of the project can work with project-owned request
// structs instead of parser-specific nodes.
var vitessParser = mustNewVitessParser()

// RawStatement wraps one parsed SQL statement while keeping the Vitess AST
// contained inside this package for now.
type RawStatement struct {
	ast sqlparser.Statement
}

// Parse parses one SQL statement into a raw parser wrapper.
func Parse(sql string) (*RawStatement, error) {
	ast, err := vitessParser.Parse(sql)
	if err != nil {
		return nil, err
	}

	return &RawStatement{ast: ast}, nil
}

// CanonicalSQL returns Vitess' normalized SQL string for the parsed statement.
func (s *RawStatement) CanonicalSQL() string {
	if s == nil || s.ast == nil {
		return ""
	}

	return sqlparser.String(s.ast)
}

// Request is the parser package's public output contract. The parser will map
// supported SQL into one of these request shapes so later layers can work with
// project-owned types instead of Vitess AST nodes.
type Request interface {
	Kind() StatementKind
}

// CreateTableRequest is the parser output for CREATE TABLE statements.
type CreateTableRequest struct {
	Definition shared.TableDefinition
}

func (CreateTableRequest) Kind() StatementKind { return StatementCreateTable }

// CreateIndexRequest is the parser output for CREATE INDEX statements.
type CreateIndexRequest struct {
	Definition shared.IndexDefinition
}

func (CreateIndexRequest) Kind() StatementKind { return StatementCreateIndex }

// DropTableRequest is the parser output for DROP TABLE statements.
type DropTableRequest struct {
	TableName string
}

func (DropTableRequest) Kind() StatementKind { return StatementDropTable }

// DropIndexRequest is the parser output for DROP INDEX statements.
type DropIndexRequest struct {
	IndexName string
}

func (DropIndexRequest) Kind() StatementKind { return StatementDropIndex }

// InsertRequest is the parser output for INSERT statements.
type InsertRequest struct {
	Statement statement.InsertStatement
}

func (InsertRequest) Kind() StatementKind { return StatementInsert }

// DeleteRequest is the parser output for DELETE statements.
type DeleteRequest struct {
	Statement statement.DeleteStatement
}

func (DeleteRequest) Kind() StatementKind { return StatementDelete }

// UpdateRequest is the parser output for UPDATE statements.
type UpdateRequest struct {
	Statement statement.UpdateStatement
}

func (UpdateRequest) Kind() StatementKind { return StatementUpdate }

// ParseRequest is the future main parser entrypoint for the app layer. For now
// it reserves the contract boundary and routes through raw parsing.
func ParseRequest(sql string) (Request, error) {
	raw, err := Parse(sql)
	if err != nil {
		return nil, err
	}

	return BuildRequest(raw)
}

// BuildRequest converts a raw parsed statement into the parser package's
// public request contract.
func BuildRequest(raw *RawStatement) (Request, error) {
	if raw == nil {
		return nil, fmt.Errorf("parser: nil raw statement")
	}

	switch raw.Kind() {
	case StatementCreateTable:
		stmt, ok := raw.ast.(*sqlparser.CreateTable)
		if !ok {
			return nil, fmt.Errorf("parser: expected create-table AST, got %T", raw.ast)
		}
		return buildCreateTableRequest(stmt)
	case StatementCreateIndex:
		stmt, ok := raw.ast.(*sqlparser.AlterTable)
		if !ok {
			return nil, fmt.Errorf("parser: expected create-index AST, got %T", raw.ast)
		}
		return buildCreateIndexRequest(stmt)
	case StatementDropTable:
		stmt, ok := raw.ast.(*sqlparser.DropTable)
		if !ok {
			return nil, fmt.Errorf("parser: expected drop-table AST, got %T", raw.ast)
		}
		return buildDropTableRequest(stmt)
	case StatementDropIndex:
		stmt, ok := raw.ast.(*sqlparser.AlterTable)
		if !ok {
			return nil, fmt.Errorf("parser: expected drop-index AST, got %T", raw.ast)
		}
		return buildDropIndexRequest(stmt)
	case StatementInsert:
		stmt, ok := raw.ast.(*sqlparser.Insert)
		if !ok {
			return nil, fmt.Errorf("parser: expected insert AST, got %T", raw.ast)
		}
		return buildInsertRequest(stmt)
	case StatementDelete:
		stmt, ok := raw.ast.(*sqlparser.Delete)
		if !ok {
			return nil, fmt.Errorf("parser: expected delete AST, got %T", raw.ast)
		}
		return buildDeleteRequest(stmt)
	case StatementUpdate:
		stmt, ok := raw.ast.(*sqlparser.Update)
		if !ok {
			return nil, fmt.Errorf("parser: expected update AST, got %T", raw.ast)
		}
		return buildUpdateRequest(stmt)
	default:
		return nil, fmt.Errorf("parser: %s request conversion not implemented yet", raw.Kind())
	}
}

func buildCreateTableRequest(stmt *sqlparser.CreateTable) (Request, error) {
	if stmt == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create table: statement is required")
	}
	if stmt.OptLike != nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create table: CREATE TABLE ... LIKE is unsupported")
	}
	if stmt.Select != nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create table: CREATE TABLE ... SELECT is unsupported")
	}
	// In "CREATE TABLE t (id INT PRIMARY KEY)", the "(id INT PRIMARY KEY)" part is the TableSpec.
	if stmt.TableSpec == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create table: table definition is required")
	}

	// e.g., stmt.TableSpec.Columns is like the "(id INT, name VARCHAR(255))" part of "CREATE TABLE t (id INT, name VARCHAR(255))"
	columns, primaryKey, foreignKeys, err := buildCreateTableColumns(stmt.TableSpec.Columns)
	if err != nil {
		return nil, err
	}
	// e.g., stmt.TableSpec.Indexes is like the "(PRIMARY KEY (id))" part of "CREATE TABLE t (id INT, name VARCHAR(255), PRIMARY KEY (id))"
	primaryKey, err = buildCreateTablePrimaryKey(stmt.TableSpec.Indexes, primaryKey)
	if err != nil {
		return nil, err
	}
	// e.g., stmt.TableSpec.Constraints is like the "(FOREIGN KEY (id) REFERENCES other_table(other_id))" part
	// of "CREATE TABLE t (id INT, FOREIGN KEY (id) REFERENCES other_table(other_id))"
	foreignKeys, err = buildCreateTableForeignKeys(stmt.TableSpec.Constraints, foreignKeys)
	if err != nil {
		return nil, err
	}

	return CreateTableRequest{
		Definition: shared.TableDefinition{
			Name:        stmt.Table.Name.String(),
			Columns:     columns,
			PrimaryKey:  primaryKey,
			ForeignKeys: foreignKeys,
		},
	}, nil
}

// buildCreateIndexRequest converts the parsed CREATE INDEX form into the
// project's one-column index request shape.
func buildCreateIndexRequest(stmt *sqlparser.AlterTable) (Request, error) {
	if stmt == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create index: statement is required")
	}
	if stmt.Table.Qualifier.String() != "" {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create index: qualified table names are unsupported")
	}
	if len(stmt.AlterOptions) != 1 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create index: invalid create-index shape")
	}

	addIndexDef, ok := stmt.AlterOptions[0].(*sqlparser.AddIndexDefinition)
	if !ok || addIndexDef.IndexDefinition == nil || addIndexDef.IndexDefinition.Info == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create index: invalid index definition")
	}

	keyDef := addIndexDef.IndexDefinition
	if len(keyDef.Columns) != 1 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create index: composite indexes are unsupported")
	}

	// Support now:
	// - index name
	// - table name
	// - one plain column
	// - optional UNIQUE
	//
	// Unsupported as of now:
	// - unnamed indexes
	// - FULLTEXT / SPATIAL / PRIMARY index kinds
	// - expression indexes
	// - prefix length indexes
	// - DESC / ASC index columns
	// - extra index options
	if keyDef.Info.Name.String() == "" {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create index: index name is required")
	}
	if keyDef.Info.Type != sqlparser.IndexTypeDefault && keyDef.Info.Type != sqlparser.IndexTypeUnique {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create index: unsupported index kind")
	}
	if len(keyDef.Options) > 0 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create index: index options are unsupported")
	}

	columnDef := keyDef.Columns[0]
	if columnDef == nil || columnDef.Expression != nil || columnDef.Length != nil || columnDef.Direction != sqlparser.AscOrder {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "create index: unsupported index column shape")
	}

	return CreateIndexRequest{
		Definition: shared.IndexDefinition{
			Name:       keyDef.Info.Name.String(),
			TableName:  stmt.Table.Name.String(),
			ColumnName: columnDef.Column.String(),
			IsUnique:   keyDef.Info.Type == sqlparser.IndexTypeUnique,
		},
	}, nil
}

// buildDropTableRequest converts DROP TABLE into the project's single-table
// drop request shape.
func buildDropTableRequest(stmt *sqlparser.DropTable) (Request, error) {
	if stmt == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "drop table: statement is required")
	}

	// Support now:
	// - one table name
	//
	// Unsupported as of now:
	// - dropping multiple tables at once
	// - qualified table names
	if len(stmt.FromTables) != 1 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "drop table: dropping multiple tables is unsupported")
	}
	if stmt.FromTables[0].Qualifier.String() != "" {
		// e.g., in "DROP TABLE db1.t", the "db1" part is the qualifier, which we don't support for now
		return nil, shared.NewError(shared.ErrInvalidDefinition, "drop table: qualified table names are unsupported")
	}

	return DropTableRequest{
		TableName: stmt.FromTables[0].Name.String(),
	}, nil
}

// buildDropIndexRequest converts DROP INDEX into the project's simple
// drop-index request shape.
func buildDropIndexRequest(stmt *sqlparser.AlterTable) (Request, error) {
	if stmt == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "drop index: statement is required")
	}
	if stmt.Table.Qualifier.String() != "" {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "drop index: qualified table names are unsupported")
	}
	if len(stmt.AlterOptions) != 1 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "drop index: invalid drop-index shape")
	}

	dropKeyDef, ok := stmt.AlterOptions[0].(*sqlparser.DropKey)
	if !ok {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "drop index: invalid drop-index definition")
	}

	// Support now:
	// - one index name
	//
	// Unsupported as of now:
	// - PRIMARY KEY drops
	// - FOREIGN KEY drops
	if dropKeyDef.Type != sqlparser.NormalKeyType {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "drop index: only secondary index drops are supported")
	}
	if dropKeyDef.Name.String() == "" {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "drop index: index name is required")
	}

	return DropIndexRequest{
		IndexName: dropKeyDef.Name.String(),
	}, nil
}

// buildInsertRequest converts INSERT ... VALUES into the project's one-row
// insert request shape.
func buildInsertRequest(stmt *sqlparser.Insert) (Request, error) {
	if stmt == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "insert: statement is required")
	}
	if stmt.Action != sqlparser.InsertAct {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "insert: only INSERT is supported")
	}
	if stmt.Ignore {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "insert: INSERT IGNORE is unsupported")
	}
	if stmt.Table == nil || stmt.Table.Expr == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "insert: table is required")
	}

	tableName, err := extractSimpleTableName(stmt.Table.Expr)
	if err != nil {
		return nil, err
	}
	columnNames, err := buildInsertColumnNames(stmt.Columns)
	if err != nil {
		return nil, err
	}
	values, err := buildInsertValues(stmt.Rows)
	if err != nil {
		return nil, err
	}

	return InsertRequest{
		Statement: statement.InsertStatement{
			TableName:   tableName,
			ColumnNames: columnNames,
			Values:      values,
		},
	}, nil
}

// buildDeleteRequest converts a plain DELETE FROM table statement into the
// project's full-table delete request shape.
func buildDeleteRequest(stmt *sqlparser.Delete) (Request, error) {
	if stmt == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "delete: statement is required")
	}

	// Support now:
	// - DELETE FROM one table
	//
	// Unsupported as of now:
	// - WHERE
	// - ORDER BY / LIMIT
	// - multi-table DELETE
	// - partition deletes
	if stmt.Where != nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "delete: WHERE is unsupported")
	}
	if len(stmt.OrderBy) > 0 || stmt.Limit != nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "delete: ORDER BY and LIMIT are unsupported")
	}
	if len(stmt.Targets) > 0 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "delete: multi-table DELETE is unsupported")
	}
	if len(stmt.Partitions) > 0 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "delete: partition deletes are unsupported")
	}
	if len(stmt.TableExprs) != 1 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "delete: exactly one table is required")
	}

	tableName, err := extractSingleTableName(stmt.TableExprs[0])
	if err != nil {
		return nil, err
	}

	return DeleteRequest{
		Statement: statement.DeleteStatement{
			TableName: tableName,
		},
	}, nil
}

// buildUpdateRequest converts a plain one-column UPDATE statement into the
// project's simple update request shape.
func buildUpdateRequest(stmt *sqlparser.Update) (Request, error) {
	if stmt == nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "update: statement is required")
	}

	// Support now:
	// - UPDATE one table
	// - one column assignment
	// - integer/string literal value
	//
	// Unsupported as of now:
	// - WHERE
	// - ORDER BY / LIMIT
	// - multi-table UPDATE
	// - multiple assignments
	if stmt.Where != nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "update: WHERE is unsupported")
	}
	if len(stmt.OrderBy) > 0 || stmt.Limit != nil {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "update: ORDER BY and LIMIT are unsupported")
	}
	if len(stmt.TableExprs) != 1 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "update: exactly one table is required")
	}
	if len(stmt.Exprs) != 1 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "update: exactly one assignment is required")
	}

	tableName, err := extractSingleTableName(stmt.TableExprs[0])
	if err != nil {
		return nil, err
	}
	assignment, err := buildUpdateAssignment(stmt.Exprs[0])
	if err != nil {
		return nil, err
	}

	return UpdateRequest{
		Statement: statement.UpdateStatement{
			TableName:  tableName,
			ColumnName: assignment.ColumnName,
			Value:      assignment.Value,
		},
	}, nil
}

func buildCreateTableColumns(columns []*sqlparser.ColumnDefinition) ([]shared.ColumnDefinition, []string, []shared.ForeignKeyDefinition, error) {
	defs := make([]shared.ColumnDefinition, 0, len(columns))
	var primaryKey []string
	foreignKeys := make([]shared.ForeignKeyDefinition, 0)

	for _, column := range columns {
		if column == nil {
			return nil, nil, nil, shared.NewError(shared.ErrInvalidDefinition, "create table: column definition is required")
		}
		if column.Type == nil {
			return nil, nil, nil, shared.NewError(shared.ErrInvalidDefinition, "create table: type is required for column %q", column.Name.String())
		}
		if hasUnsupportedColumnOptions(column.Type.Options) {
			return nil, nil, nil, shared.NewError(shared.ErrInvalidDefinition, "create table: column options are not supported yet for column %q", column.Name.String())
		}

		dataType, err := mapCreateTableColumnType(column.Name.String(), column.Type)
		if err != nil {
			return nil, nil, nil, err
		}

		// e.g., in "CREATE TABLE t (id INT)"
		// column.Name.String() -> "id"
		// column.Type -> the "INT" part, which we map to shared.TypeInteger
		defs = append(defs, shared.ColumnDefinition{
			Name: column.Name.String(),
			Type: dataType,
		})

		if column.Type.Options != nil && column.Type.Options.KeyOpt == sqlparser.ColKeyPrimary {
			if len(primaryKey) > 0 && primaryKey[0] != column.Name.String() {
				return nil, nil, nil, shared.NewError(shared.ErrInvalidDefinition, "create table: composite primary keys are unsupported")
			}
			primaryKey = []string{column.Name.String()}
		}

		if column.Type.Options != nil && column.Type.Options.Reference != nil {
			foreignKey, err := buildForeignKeyReference([]string{column.Name.String()}, column.Type.Options.Reference)
			if err != nil {
				return nil, nil, nil, err
			}
			foreignKeys = append(foreignKeys, foreignKey)
		}
	}

	return defs, primaryKey, foreignKeys, nil
}

// This project only supports integer or string.
func mapCreateTableColumnType(columnName string, columnType *sqlparser.ColumnType) (shared.DataType, error) {
	baseType := strings.ToLower(columnType.Type)

	switch baseType {
	case "int", "integer":
		return shared.TypeInteger, nil
	case "char", "varchar", "text":
		return shared.TypeString, nil
	default:
		return "", shared.NewError(shared.ErrInvalidDefinition, "create table: unsupported type %q for column %q", columnType.Type, columnName)
	}
}

func hasUnsupportedColumnOptions(options *sqlparser.ColumnTypeOptions) bool {
	if options == nil {
		return false
	}

	// Support now:
	// - plain column name
	// - supported type
	// - inline PRIMARY KEY
	// - inline REFERENCES
	//
	// Unsupported as of now:
	// - inline UNIQUE / KEY markers
	// - NULL / NOT NULL
	// - DEFAULT / AUTO_INCREMENT / generated-column expressions
	// - comments / collations / extra attributes
	return (options.KeyOpt != sqlparser.ColKeyNone && options.KeyOpt != sqlparser.ColKeyPrimary) ||
		options.Null != nil ||
		options.Autoincrement ||
		options.Default != nil ||
		options.OnUpdate != nil ||
		options.As != nil ||
		options.Comment != nil ||
		options.Collate != "" ||
		options.Invisible != nil ||
		options.EngineAttribute != nil ||
		options.SecondaryEngineAttribute != nil ||
		options.SRID != nil
}

// buildCreateTablePrimaryKey reads table-level PRIMARY KEY definitions from the
// parsed CREATE TABLE key-definition list and merges them with any inline primary key
// already found on columns.
func buildCreateTablePrimaryKey(keyDefs []*sqlparser.IndexDefinition, current []string) ([]string, error) {
	primaryKey := current

	// "CREATE TABLE t (id INT, name VARCHAR(255), PRIMARY KEY (id))"
	// keyDef is like the "PRIMARY KEY (id)" part of
	for _, keyDef := range keyDefs {
		if keyDef == nil || keyDef.Info == nil {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "create table: invalid index definition")
		}
		if keyDef.Info.Type != sqlparser.IndexTypePrimary {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "create table: secondary indexes are not supported yet")
		}
		if len(keyDef.Columns) != 1 {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "create table: composite primary keys are unsupported")
		}
		// column is like the "id" part of "PRIMARY KEY (id)"
		column := keyDef.Columns[0]
		if column == nil || column.Expression != nil || column.Length != nil {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "create table: unsupported primary key shape")
		}
		columnName := column.Column.String()
		if len(primaryKey) > 0 && primaryKey[0] != columnName {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "create table: multiple primary keys are unsupported")
		}
		primaryKey = []string{columnName}
	}

	return primaryKey, nil
}

// buildCreateTableForeignKeys reads table-level FOREIGN KEY constraints and
// merges them with any inline REFERENCES definitions already found on columns.
func buildCreateTableForeignKeys(constraints []*sqlparser.ConstraintDefinition, current []shared.ForeignKeyDefinition) ([]shared.ForeignKeyDefinition, error) {
	foreignKeys := current

	// "CREATE TABLE t (id INT, name VARCHAR(255), FOREIGN KEY (id) REFERENCES other_table(other_id))"
	// constraint is like the "FOREIGN KEY (id) REFERENCES other_table(other_id)"
	for _, constraint := range constraints {
		if constraint == nil {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "create table: invalid constraint definition")
		}

		// assert that this is a FOREIGN KEY constraint, not UNIQUE / CHECK / etc.
		foreignKeyDef, ok := constraint.Details.(*sqlparser.ForeignKeyDefinition)
		if !ok {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "create table: unsupported constraint definition")
		}

		// foreignKeyDef.Source is like the "(id)" part of "FOREIGN KEY (id) REFERENCES other_table(other_id)"
		sourceColumns := make([]string, 0, len(foreignKeyDef.Source))
		for _, sourceColumn := range foreignKeyDef.Source {
			sourceColumns = append(sourceColumns, sourceColumn.String())
		}

		foreignKey, err := buildForeignKeyReference(sourceColumns, foreignKeyDef.ReferenceDefinition)
		if err != nil {
			return nil, err
		}
		foreignKeys = append(foreignKeys, foreignKey)
	}

	return foreignKeys, nil
}

// buildForeignKeyReference converts the parsed REFERENCES target into the
// project's one-column foreign-key shape.
func buildForeignKeyReference(sourceColumns []string, ref *sqlparser.ReferenceDefinition) (shared.ForeignKeyDefinition, error) {
	// Support now:
	// - one source column
	// - one referenced column
	// - default action or RESTRICT
	//
	// Unsupported as of now:
	// - composite foreign keys
	// - qualified table names
	// - MATCH clauses
	// - CASCADE / SET NULL / other actions
	if len(sourceColumns) != 1 {
		return shared.ForeignKeyDefinition{}, shared.NewError(shared.ErrInvalidDefinition, "create table: composite foreign keys are unsupported")
	}
	if ref == nil {
		return shared.ForeignKeyDefinition{}, shared.NewError(shared.ErrInvalidDefinition, "create table: foreign key reference is required")
	}
	if ref.ReferencedTable.Qualifier.String() != "" {
		return shared.ForeignKeyDefinition{}, shared.NewError(shared.ErrInvalidDefinition, "create table: qualified foreign key references are unsupported")
	}
	if len(ref.ReferencedColumns) != 1 {
		return shared.ForeignKeyDefinition{}, shared.NewError(shared.ErrInvalidDefinition, "create table: composite foreign keys are unsupported")
	}
	if ref.Match != sqlparser.DefaultMatch {
		return shared.ForeignKeyDefinition{}, shared.NewError(shared.ErrInvalidDefinition, "create table: MATCH is unsupported")
	}
	if ref.OnDelete != sqlparser.DefaultAction && ref.OnDelete != sqlparser.Restrict {
		return shared.ForeignKeyDefinition{}, shared.NewError(shared.ErrInvalidDefinition, "create table: unsupported foreign key ON DELETE action")
	}
	if ref.OnUpdate != sqlparser.DefaultAction && ref.OnUpdate != sqlparser.Restrict {
		return shared.ForeignKeyDefinition{}, shared.NewError(shared.ErrInvalidDefinition, "create table: unsupported foreign key ON UPDATE action")
	}

	return shared.ForeignKeyDefinition{
		ColumnName: sourceColumns[0],
		RefTable:   ref.ReferencedTable.Name.String(),
		RefColumn:  ref.ReferencedColumns[0].String(),
	}, nil
}

func extractSimpleTableName(expr sqlparser.SimpleTableExpr) (string, error) {
	tableName, ok := expr.(sqlparser.TableName)
	if !ok {
		return "", shared.NewError(shared.ErrInvalidDefinition, "parser: unsupported table expression")
	}
	if tableName.Qualifier.String() != "" {
		return "", shared.NewError(shared.ErrInvalidDefinition, "parser: qualified table names are unsupported")
	}
	if tableName.Name.String() == "" {
		return "", shared.NewError(shared.ErrInvalidDefinition, "parser: table name is required")
	}
	return tableName.Name.String(), nil
}

func extractSingleTableName(expr sqlparser.TableExpr) (string, error) {
	aliasedExpr, ok := expr.(*sqlparser.AliasedTableExpr)
	if !ok {
		return "", shared.NewError(shared.ErrInvalidDefinition, "parser: unsupported table expression")
	}

	tableName, err := extractSimpleTableName(aliasedExpr.Expr)
	if err != nil {
		return "", err
	}
	if aliasedExpr.As.String() != "" {
		return "", shared.NewError(shared.ErrInvalidDefinition, "parser: table aliases are unsupported")
	}

	return tableName, nil
}

func buildInsertColumnNames(columns sqlparser.Columns) ([]string, error) {
	if len(columns) == 0 {
		return nil, nil
	}

	names := make([]string, 0, len(columns))
	for _, column := range columns {
		if column.String() == "" {
			return nil, shared.NewError(shared.ErrInvalidDefinition, "insert: unsupported column reference")
		}
		names = append(names, column.String())
	}

	return names, nil
}

func buildInsertValues(rows sqlparser.InsertRows) ([]storage.Value, error) {
	valueRows, ok := rows.(sqlparser.Values)
	if !ok {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "parser: only VALUES inserts are supported")
	}

	// Support now:
	// - one VALUES row
	// - integer literals
	// - string literals
	//
	// Unsupported as of now:
	// - multi-row VALUES
	// - INSERT ... SELECT
	// - NULL / boolean / expression values
	if len(valueRows) != 1 {
		return nil, shared.NewError(shared.ErrInvalidDefinition, "parser: multi-row VALUES is unsupported")
	}

	values := make([]storage.Value, 0, len(valueRows[0]))
	for _, expr := range valueRows[0] {
		value, err := buildLiteralValue(expr)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}

	return values, nil
}

func buildLiteralValue(expr sqlparser.Expr) (storage.Value, error) {
	switch valueExpr := expr.(type) {
	case *sqlparser.Literal:
		switch valueExpr.Type {
		case sqlparser.IntVal:
			intValue, err := strconv.ParseInt(valueExpr.Val, 10, 64)
			if err != nil {
				return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "parser: invalid integer literal %q", valueExpr.Val)
			}
			return storage.NewIntegerValue(intValue), nil
		case sqlparser.StrVal:
			return storage.NewStringValue(valueExpr.Val), nil
		default:
			return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "parser: unsupported literal value")
		}
	case *sqlparser.NullVal:
		return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "parser: NULL values are unsupported")
	default:
		return storage.Value{}, shared.NewError(shared.ErrInvalidDefinition, "parser: only literal VALUES are supported")
	}
}

type updateAssignment struct {
	ColumnName string
	Value      storage.Value
}

func buildUpdateAssignment(expr *sqlparser.UpdateExpr) (updateAssignment, error) {
	if expr == nil {
		return updateAssignment{}, shared.NewError(shared.ErrInvalidDefinition, "update: assignment is required")
	}
	if expr.Name.Name.String() == "" || expr.Name.Qualifier.Name.String() != "" || expr.Name.Qualifier.Qualifier.String() != "" {
		return updateAssignment{}, shared.NewError(shared.ErrInvalidDefinition, "update: unsupported assignment target")
	}

	value, err := buildLiteralValue(expr.Expr)
	if err != nil {
		return updateAssignment{}, err
	}

	return updateAssignment{
		ColumnName: expr.Name.Name.String(),
		Value:      value,
	}, nil
}

func mustNewVitessParser() *sqlparser.Parser {
	parser, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		panic(err)
	}

	return parser
}
