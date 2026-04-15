package shared

// DataType is the small set of column types supported.
type DataType string

const (
	TypeInteger DataType = "integer"
	TypeString  DataType = "string"
)

// ColumnDefinition describes one column inside a CREATE TABLE statement.
type ColumnDefinition struct {
	Name string
	Type DataType
}

// ForeignKeyDefinition stores one column-level foreign key.
type ForeignKeyDefinition struct {
	ColumnName string
	RefTable   string
	RefColumn  string
}

// TableDefinition is the in-memory shape of a table schema.
type TableDefinition struct {
	Name        string
	Columns     []ColumnDefinition
	PrimaryKey  []string
	ForeignKeys []ForeignKeyDefinition
}

// IndexDefinition is the in-memory shape of a CREATE INDEX request.
type IndexDefinition struct {
	Name       string
	TableName  string
	ColumnName string
	IsUnique   bool
}
