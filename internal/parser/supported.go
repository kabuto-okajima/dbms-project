package parser

import "vitess.io/vitess/go/vt/sqlparser"

// StatementKind identifies the high-level SQL statement family.
type StatementKind string

const (
	StatementUnknown     StatementKind = "unknown"
	StatementCreateTable StatementKind = "create_table"
	StatementCreateIndex StatementKind = "create_index"
	StatementDropTable   StatementKind = "drop_table"
	StatementDropIndex   StatementKind = "drop_index"
	StatementInsert      StatementKind = "insert"
	StatementDelete      StatementKind = "delete"
	StatementUpdate      StatementKind = "update"
	StatementSelect      StatementKind = "select"
)

// Kind classifies the parsed statement into one of the project statement
// families without exposing Vitess AST types to callers.
func (s *RawStatement) Kind() StatementKind {
	if s == nil || s.ast == nil {
		return StatementUnknown
	}

	switch stmt := s.ast.(type) {
	case *sqlparser.CreateTable:
		return StatementCreateTable
	case *sqlparser.DropTable:
		return StatementDropTable
	case *sqlparser.Insert:
		return StatementInsert
	case *sqlparser.Delete:
		return StatementDelete
	case *sqlparser.Update:
		return StatementUpdate
	case *sqlparser.Select, *sqlparser.Union:
		return StatementSelect
	case *sqlparser.AlterTable:
		return classifyAlterTable(stmt)
	default:
		return StatementUnknown
	}
}

func classifyAlterTable(stmt *sqlparser.AlterTable) StatementKind {
	if stmt == nil || len(stmt.AlterOptions) != 1 {
		return StatementUnknown
	}

	switch option := stmt.AlterOptions[0].(type) {
	case *sqlparser.AddIndexDefinition:
		return StatementCreateIndex
	case *sqlparser.DropKey:
		if option.Type == sqlparser.NormalKeyType {
			return StatementDropIndex
		}
	}

	return StatementUnknown
}
