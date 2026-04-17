package parser

import (
	"errors"
	"testing"

	"dbms-project/internal/shared"
	"dbms-project/internal/statement"
	"dbms-project/internal/storage"
)

func TestRequestKinds(t *testing.T) {
	tests := []struct {
		name string
		req  Request
		want StatementKind
	}{
		{name: "create table", req: CreateTableRequest{}, want: StatementCreateTable},
		{name: "create index", req: CreateIndexRequest{}, want: StatementCreateIndex},
		{name: "drop table", req: DropTableRequest{}, want: StatementDropTable},
		{name: "drop index", req: DropIndexRequest{}, want: StatementDropIndex},
		{name: "insert", req: InsertRequest{}, want: StatementInsert},
		{name: "delete", req: DeleteRequest{}, want: StatementDelete},
		{name: "update", req: UpdateRequest{}, want: StatementUpdate},
		{name: "select", req: SelectRequest{}, want: StatementSelect},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.req.Kind(); got != tc.want {
				t.Fatalf("expected kind %q, got %q", tc.want, got)
			}
		})
	}
}

func TestParseCreateTable(t *testing.T) {
	stmt, err := Parse("create table students (id int, name varchar(255), primary key (id))")
	if err != nil {
		t.Fatal(err)
	}

	if got := stmt.Kind(); got != StatementCreateTable {
		t.Fatalf("expected create-table kind, got %q", got)
	}

	if got := stmt.CanonicalSQL(); got == "" {
		t.Fatal("expected canonical SQL to be populated")
	}
}

func TestParseCreateIndex(t *testing.T) {
	stmt, err := Parse("create index idx_students_name on students (name)")
	if err != nil {
		t.Fatal(err)
	}

	if got := stmt.Kind(); got != StatementCreateIndex {
		t.Fatalf("expected create-index kind, got %q", got)
	}
}

func TestParseInsert(t *testing.T) {
	stmt, err := Parse("insert into students (id, name) values (1, 'Ada')")
	if err != nil {
		t.Fatal(err)
	}

	if got := stmt.Kind(); got != StatementInsert {
		t.Fatalf("expected insert kind, got %q", got)
	}
}

func TestParseSelect(t *testing.T) {
	stmt, err := Parse("select id from students")
	if err != nil {
		t.Fatal(err)
	}

	if got := stmt.Kind(); got != StatementSelect {
		t.Fatalf("expected select kind, got %q", got)
	}
}

func TestParseRejectsInvalidSQL(t *testing.T) {
	if _, err := Parse("this is not sql"); err == nil {
		t.Fatal("expected syntax error, got nil")
	}
}

func TestBuildRequestCreateTableAcceptsBasicShape(t *testing.T) {
	raw, err := Parse("create table students (id int primary key, name varchar(255))")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := req.(CreateTableRequest); !ok {
		t.Fatalf("expected CreateTableRequest, got %T", req)
	}

	createReq := req.(CreateTableRequest)
	if createReq.Definition.Name != "students" {
		t.Fatalf("expected table name students, got %q", createReq.Definition.Name)
	}
	if len(createReq.Definition.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(createReq.Definition.Columns))
	}
	if createReq.Definition.Columns[0] != (shared.ColumnDefinition{Name: "id", Type: shared.TypeInteger}) {
		t.Fatalf("unexpected first column: %+v", createReq.Definition.Columns[0])
	}
	if createReq.Definition.Columns[1] != (shared.ColumnDefinition{Name: "name", Type: shared.TypeString}) {
		t.Fatalf("unexpected second column: %+v", createReq.Definition.Columns[1])
	}
	if len(createReq.Definition.PrimaryKey) != 1 || createReq.Definition.PrimaryKey[0] != "id" {
		t.Fatalf("unexpected primary key: %+v", createReq.Definition.PrimaryKey)
	}
}

func TestBuildRequestCreateTableRejectsLike(t *testing.T) {
	raw, err := Parse("create table students like departments")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestCreateTableRejectsSelect(t *testing.T) {
	raw, err := Parse("create table students select id from departments")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestCreateTableRejectsUnsupportedType(t *testing.T) {
	raw, err := Parse("create table students (created_at datetime)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestCreateTableAcceptsTableLevelPrimaryKey(t *testing.T) {
	raw, err := Parse("create table students (id int, name varchar(255), primary key (id))")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	createReq := req.(CreateTableRequest)
	if len(createReq.Definition.PrimaryKey) != 1 || createReq.Definition.PrimaryKey[0] != "id" {
		t.Fatalf("unexpected primary key: %+v", createReq.Definition.PrimaryKey)
	}
}

func TestBuildRequestCreateTableAcceptsInlineForeignKey(t *testing.T) {
	raw, err := Parse("create table students (dept_id int references departments(id))")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	createReq := req.(CreateTableRequest)
	if len(createReq.Definition.ForeignKeys) != 1 {
		t.Fatalf("expected 1 foreign key, got %d", len(createReq.Definition.ForeignKeys))
	}
	if createReq.Definition.ForeignKeys[0] != (shared.ForeignKeyDefinition{
		ColumnName: "dept_id",
		RefTable:   "departments",
		RefColumn:  "id",
	}) {
		t.Fatalf("unexpected foreign key: %+v", createReq.Definition.ForeignKeys[0])
	}
}

func TestBuildRequestCreateTableAcceptsTableLevelForeignKey(t *testing.T) {
	raw, err := Parse("create table students (dept_id int, foreign key (dept_id) references departments(id))")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	createReq := req.(CreateTableRequest)
	if len(createReq.Definition.ForeignKeys) != 1 {
		t.Fatalf("expected 1 foreign key, got %d", len(createReq.Definition.ForeignKeys))
	}
	if createReq.Definition.ForeignKeys[0] != (shared.ForeignKeyDefinition{
		ColumnName: "dept_id",
		RefTable:   "departments",
		RefColumn:  "id",
	}) {
		t.Fatalf("unexpected foreign key: %+v", createReq.Definition.ForeignKeys[0])
	}
}

func TestBuildRequestCreateTableRejectsCompositePrimaryKey(t *testing.T) {
	raw, err := Parse("create table students (id int, dept_id int, primary key (id, dept_id))")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestCreateTableRejectsCompositeForeignKey(t *testing.T) {
	raw, err := Parse("create table students (dept_id int, school_id int, foreign key (dept_id, school_id) references departments(id, school_id))")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestCreateIndexAcceptsBasicShape(t *testing.T) {
	raw, err := Parse("create index idx_students_name on students (name)")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	createReq, ok := req.(CreateIndexRequest)
	if !ok {
		t.Fatalf("expected CreateIndexRequest, got %T", req)
	}
	if createReq.Definition != (shared.IndexDefinition{
		Name:       "idx_students_name",
		TableName:  "students",
		ColumnName: "name",
		IsUnique:   false,
	}) {
		t.Fatalf("unexpected create-index definition: %+v", createReq.Definition)
	}
}

func TestBuildRequestCreateIndexAcceptsUnique(t *testing.T) {
	raw, err := Parse("create unique index idx_students_name on students (name)")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	createReq := req.(CreateIndexRequest)
	if !createReq.Definition.IsUnique {
		t.Fatalf("expected unique index definition, got %+v", createReq.Definition)
	}
}

func TestBuildRequestCreateIndexRejectsCompositeIndex(t *testing.T) {
	raw, err := Parse("create index idx_students_name on students (name, id)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestCreateIndexRejectsExpressionIndex(t *testing.T) {
	raw, err := Parse("create index idx_students_expr on students ((id + 1))")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestCreateIndexRejectsUnsupportedIndexKind(t *testing.T) {
	raw, err := Parse("create fulltext index idx_students_name on students (name)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestDropTableAcceptsBasicShape(t *testing.T) {
	raw, err := Parse("drop table students")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	dropReq, ok := req.(DropTableRequest)
	if !ok {
		t.Fatalf("expected DropTableRequest, got %T", req)
	}
	if dropReq.TableName != "students" {
		t.Fatalf("expected table name students, got %q", dropReq.TableName)
	}
}

func TestBuildRequestDropTableRejectsMultipleTables(t *testing.T) {
	raw, err := Parse("drop table students, departments")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestDropIndexAcceptsBasicShape(t *testing.T) {
	raw, err := Parse("drop index idx_students_name on students")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	dropReq, ok := req.(DropIndexRequest)
	if !ok {
		t.Fatalf("expected DropIndexRequest, got %T", req)
	}
	if dropReq.IndexName != "idx_students_name" {
		t.Fatalf("expected index name idx_students_name, got %q", dropReq.IndexName)
	}
}

func TestBuildRequestDropIndexRejectsQualifiedTable(t *testing.T) {
	raw, err := Parse("drop index idx_students_name on school.students")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestInsertAcceptsBasicShape(t *testing.T) {
	raw, err := Parse("insert into students values (1, 'Ada')")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	insertReq, ok := req.(InsertRequest)
	if !ok {
		t.Fatalf("expected InsertRequest, got %T", req)
	}
	if insertReq.Statement.TableName != "students" {
		t.Fatalf("expected table name students, got %q", insertReq.Statement.TableName)
	}
	if insertReq.Statement.ColumnNames != nil {
		t.Fatalf("expected nil column list, got %+v", insertReq.Statement.ColumnNames)
	}
	if len(insertReq.Statement.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(insertReq.Statement.Values))
	}
	if insertReq.Statement.Values[0] != storage.NewIntegerValue(1) {
		t.Fatalf("unexpected first value: %+v", insertReq.Statement.Values[0])
	}
	if insertReq.Statement.Values[1] != storage.NewStringValue("Ada") {
		t.Fatalf("unexpected second value: %+v", insertReq.Statement.Values[1])
	}
}

func TestBuildRequestInsertAcceptsColumnList(t *testing.T) {
	raw, err := Parse("insert into students (id, name) values (1, 'Ada')")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	insertReq := req.(InsertRequest)
	if len(insertReq.Statement.ColumnNames) != 2 {
		t.Fatalf("expected 2 column names, got %d", len(insertReq.Statement.ColumnNames))
	}
	if insertReq.Statement.ColumnNames[0] != "id" || insertReq.Statement.ColumnNames[1] != "name" {
		t.Fatalf("unexpected column names: %+v", insertReq.Statement.ColumnNames)
	}
}

func TestBuildRequestInsertRejectsMultiRowValues(t *testing.T) {
	raw, err := Parse("insert into students values (1, 'Ada'), (2, 'Grace')")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestInsertRejectsInsertSelect(t *testing.T) {
	raw, err := Parse("insert into students select id, name from graduates")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestDeleteAcceptsBasicShape(t *testing.T) {
	raw, err := Parse("delete from students")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	deleteReq, ok := req.(DeleteRequest)
	if !ok {
		t.Fatalf("expected DeleteRequest, got %T", req)
	}
	if deleteReq.Statement.TableName != "students" {
		t.Fatalf("expected table name students, got %q", deleteReq.Statement.TableName)
	}
}

func TestBuildRequestDeleteRejectsWhere(t *testing.T) {
	raw, err := Parse("delete from students where id = 1")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestDeleteRejectsAlias(t *testing.T) {
	raw, err := Parse("delete from students as s")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestUpdateAcceptsBasicShape(t *testing.T) {
	raw, err := Parse("update students set name = 'Ada'")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	updateReq, ok := req.(UpdateRequest)
	if !ok {
		t.Fatalf("expected UpdateRequest, got %T", req)
	}
	if updateReq.Statement.TableName != "students" {
		t.Fatalf("expected table name students, got %q", updateReq.Statement.TableName)
	}
	if updateReq.Statement.ColumnName != "name" {
		t.Fatalf("expected column name name, got %q", updateReq.Statement.ColumnName)
	}
	if updateReq.Statement.Value != storage.NewStringValue("Ada") {
		t.Fatalf("unexpected update value: %+v", updateReq.Statement.Value)
	}
}

func TestBuildRequestUpdateRejectsWhere(t *testing.T) {
	raw, err := Parse("update students set name = 'Ada' where id = 1")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestUpdateRejectsMultipleAssignments(t *testing.T) {
	raw, err := Parse("update students set name = 'Ada', id = 1")
	if err != nil {
		t.Fatal(err)
	}

	_, err = BuildRequest(raw)
	if err == nil {
		t.Fatal("expected invalid-definition error, got nil")
	}
	if !errors.Is(err, shared.ErrInvalidDefinition) {
		t.Fatalf("expected invalid-definition error, got %v", err)
	}
}

func TestBuildRequestSelectAcceptsBasicShape(t *testing.T) {
	raw, err := Parse("select id, name as student_name from students s where age = 20 order by name desc")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	selectReq, ok := req.(SelectRequest)
	if !ok {
		t.Fatalf("expected SelectRequest, got %T", req)
	}
	if len(selectReq.Statement.SelectItems) != 2 || len(selectReq.Statement.From) != 1 || len(selectReq.Statement.OrderBy) != 1 {
		t.Fatalf("unexpected select statement shape: %+v", selectReq.Statement)
	}
	if selectReq.Statement.SelectItems[1].Alias != "student_name" {
		t.Fatalf("expected alias student_name, got %q", selectReq.Statement.SelectItems[1].Alias)
	}
	if selectReq.Statement.From[0] != (statement.TableRef{Name: "students", Alias: "s"}) {
		t.Fatalf("unexpected from table: %+v", selectReq.Statement.From[0])
	}
	if _, ok := selectReq.Statement.Where.(statement.ComparisonExpr); !ok {
		t.Fatalf("expected comparison WHERE expression, got %T", selectReq.Statement.Where)
	}
	if !selectReq.Statement.OrderBy[0].Desc {
		t.Fatal("expected descending order-by term")
	}
}

func TestBuildRequestSelectAcceptsAggregatesAndClauses(t *testing.T) {
	raw, err := Parse("select dept_id, count(*) as total from students group by dept_id having count(*) > 1 order by dept_id asc")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	selectReq := req.(SelectRequest)
	aggregate, ok := selectReq.Statement.SelectItems[1].Expr.(statement.AggregateExpr)
	if !ok {
		t.Fatalf("expected aggregate select item, got %T", selectReq.Statement.SelectItems[1].Expr)
	}
	if aggregate.Function != statement.AggCount {
		t.Fatalf("expected COUNT aggregate, got %q", aggregate.Function)
	}
	if _, ok := aggregate.Arg.(statement.StarExpr); !ok {
		t.Fatalf("expected COUNT(*) argument to be StarExpr, got %T", aggregate.Arg)
	}

	havingExpr, ok := selectReq.Statement.Having.(statement.ComparisonExpr)
	if !ok {
		t.Fatalf("expected comparison HAVING expression, got %T", selectReq.Statement.Having)
	}
	if havingExpr.Operator != statement.OpGreaterThan {
		t.Fatalf("expected > HAVING operator, got %q", havingExpr.Operator)
	}
}

func TestBuildRequestSelectAcceptsLogicalChains(t *testing.T) {
	raw, err := Parse("select * from students where age = 20 and dept_id = 1 and id = 5")
	if err != nil {
		t.Fatal(err)
	}

	req, err := BuildRequest(raw)
	if err != nil {
		t.Fatal(err)
	}

	selectReq := req.(SelectRequest)
	logicalExpr, ok := selectReq.Statement.Where.(statement.LogicalExpr)
	if !ok {
		t.Fatalf("expected logical WHERE expression, got %T", selectReq.Statement.Where)
	}
	if logicalExpr.Operator != statement.OpAnd {
		t.Fatalf("expected AND logical operator, got %q", logicalExpr.Operator)
	}
	if len(logicalExpr.Terms) != 3 {
		t.Fatalf("expected 3 logical terms, got %d", len(logicalExpr.Terms))
	}
}
