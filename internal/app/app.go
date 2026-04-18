package app

import (
	"dbms-project/internal/catalog"
	"dbms-project/internal/executor"
	"dbms-project/internal/parser"
	"dbms-project/internal/shared"
	"dbms-project/internal/statement"
	"dbms-project/internal/storage"
	"time"
)

// App owns the long-lived services needed by the DBMS process.
type App struct {
	Store   *storage.Store
	Catalog *catalog.Manager
}

// ResultColumn describes one column in the top-level statement output.
type ResultColumn struct {
	Name string
	Type shared.DataType
}

// Result is the app-layer contract returned for one top-level SQL statement.
//
// It is intentionally broad enough to represent both:
// - row-producing SELECT statements
// - status-producing DDL/DML statements
//
// Later app orchestration will populate the relevant fields depending on the
// statement family while keeping the CLI and formatter insulated from lower
// layer result shapes.
type Result struct {
	Kind         parser.StatementKind
	Columns      []ResultColumn
	Rows         []storage.Row
	Message      string
	AffectedRows int
	Elapsed      time.Duration
}

// HasRows reports whether this result carries tabular output.
func (r Result) HasRows() bool {
	return len(r.Columns) > 0 || len(r.Rows) > 0
}

// IsSelect reports whether the result came from a SELECT statement.
func (r Result) IsSelect() bool {
	return r.Kind == parser.StatementSelect
}

// ExecuteSQL is the app-layer entrypoint for one SQL statement.
//
// In this first app step it establishes the top-level boundary:
// - parse one SQL string into a project-owned request
// - dispatch by statement family
// - return one app-owned result shape with full-statement timing
//
// Later steps will attach transactions and real statement execution behind the
// dispatch switch without changing this method's contract.
func (a *App) ExecuteSQL(sql string) (Result, error) {
	start := time.Now()

	result, err := a.executeSQL(sql)
	if err != nil && result.Kind == "" {
		result.Kind = parser.StatementUnknown
	}
	result.Elapsed = time.Since(start)
	return result, err
}

func (a *App) executeSQL(sql string) (Result, error) {
	request, err := parser.ParseRequest(sql)
	if err != nil {
		return Result{}, err
	}

	return a.dispatchRequest(request)
}

// dispatchRequest is the app-layer routing point from parsed requests to
// statement-specific execution paths.
//
// For now each branch only records the classified statement kind. Future steps
// will replace these placeholders with transaction-backed execution.
func (a *App) dispatchRequest(request parser.Request) (Result, error) {
	switch request := request.(type) {
	case parser.CreateTableRequest:
		return a.executeCreateTable(request)
	case parser.CreateIndexRequest:
		return a.executeCreateIndex(request)
	case parser.DropTableRequest:
		return a.executeDropTable(request)
	case parser.DropIndexRequest:
		return a.executeDropIndex(request)
	case parser.InsertRequest:
		return a.executeInsert(request)
	case parser.DeleteRequest:
		return a.executeDelete(request)
	case parser.UpdateRequest:
		return a.executeUpdate(request)
	case parser.SelectRequest:
		return a.executeSelect(request)
	case nil:
		return Result{}, shared.NewError(shared.ErrInvalidDefinition, "app: request is required")
	default:
		return Result{}, shared.NewError(shared.ErrInvalidDefinition, "app: unsupported request type %T", request)
	}
}

func (a *App) executeCreateTable(request parser.CreateTableRequest) (Result, error) {
	// a.executeWriteRequest -> statement.NewCreateTableExecutor -> statement.CreateTableExecutor.Execute -> catalog.Manager.CreateTable -> storage.Tx.CreateTable
	return a.executeWriteRequest(request.Kind(), func(tx *storage.Tx) (statement.Result, error) {
		return statement.NewCreateTableExecutor(a.catalogManager()).Execute(tx, request.Definition)
	})
}

func (a *App) executeCreateIndex(request parser.CreateIndexRequest) (Result, error) {
	return a.executeWriteRequest(request.Kind(), func(tx *storage.Tx) (statement.Result, error) {
		return statement.NewCreateIndexExecutor(a.catalogManager()).Execute(tx, request.Definition)
	})
}

func (a *App) executeDropTable(request parser.DropTableRequest) (Result, error) {
	return a.executeWriteRequest(request.Kind(), func(tx *storage.Tx) (statement.Result, error) {
		return statement.NewDropTableExecutor(a.catalogManager()).Execute(tx, request.TableName)
	})
}

func (a *App) executeDropIndex(request parser.DropIndexRequest) (Result, error) {
	return a.executeWriteRequest(request.Kind(), func(tx *storage.Tx) (statement.Result, error) {
		return statement.NewDropIndexExecutor(a.catalogManager()).Execute(tx, request.IndexName)
	})
}

func (a *App) executeInsert(request parser.InsertRequest) (Result, error) {
	return a.executeWriteRequest(request.Kind(), func(tx *storage.Tx) (statement.Result, error) {
		return statement.NewInsertExecutor(a.catalogManager()).Execute(tx, request.Statement)
	})
}

func (a *App) executeDelete(request parser.DeleteRequest) (Result, error) {
	return a.executeWriteRequest(request.Kind(), func(tx *storage.Tx) (statement.Result, error) {
		return statement.NewDeleteExecutor(a.catalogManager()).Execute(tx, request.Statement)
	})
}

func (a *App) executeUpdate(request parser.UpdateRequest) (Result, error) {
	return a.executeWriteRequest(request.Kind(), func(tx *storage.Tx) (statement.Result, error) {
		return statement.NewUpdateExecutor(a.catalogManager()).Execute(tx, request.Statement)
	})
}

func (a *App) executeSelect(request parser.SelectRequest) (Result, error) {
	if a == nil || a.Store == nil {
		return Result{}, shared.NewError(shared.ErrInvalidDefinition, "app: store is not initialized")
	}

	var runtimeResult executor.RuntimeResult
	err := a.Store.View(func(tx *storage.Tx) error {
		var execErr error
		runtimeResult, execErr = executor.ExecuteSelect(tx, a.catalogManager(), request.Statement)
		return execErr
	})
	if err != nil {
		return Result{Kind: request.Kind()}, err
	}

	return Result{
		Kind:    request.Kind(),
		Columns: appResultColumns(runtimeResult.Schema),
		Rows:    appResultRows(runtimeResult.Rows),
	}, nil
}

func (a *App) executeWriteRequest(kind parser.StatementKind, fn func(tx *storage.Tx) (statement.Result, error)) (Result, error) {
	if a == nil || a.Store == nil {
		return Result{}, shared.NewError(shared.ErrInvalidDefinition, "app: store is not initialized")
	}

	var statementResult statement.Result
	err := a.Store.Update(func(tx *storage.Tx) error {
		var execErr error
		statementResult, execErr = fn(tx)
		return execErr
	})
	if err != nil {
		return Result{Kind: kind}, err
	}

	return Result{
		Kind:         kind,
		Message:      statementResult.Message,
		AffectedRows: statementResult.AffectedRows,
	}, nil
}

func (a *App) catalogManager() *catalog.Manager {
	if a != nil && a.Catalog != nil {
		return a.Catalog
	}
	return catalog.NewManager()
}

func appResultColumns(schema executor.RuntimeSchema) []ResultColumn {
	if len(schema) == 0 {
		return nil
	}

	columns := make([]ResultColumn, 0, len(schema))
	for _, column := range schema {
		columns = append(columns, ResultColumn{
			Name: column.Name,
			Type: column.Type,
		})
	}
	return columns
}

func appResultRows(rows []executor.RuntimeRow) []storage.Row {
	if len(rows) == 0 {
		return nil
	}

	resultRows := make([]storage.Row, 0, len(rows))
	for _, row := range rows {
		resultRows = append(resultRows, row.Values)
	}
	return resultRows
}

// New opens the database file and bootstraps the system catalog once at startup.
func New(dbPath string) (*App, error) {
	store, err := storage.Open(dbPath)
	if err != nil {
		return nil, err
	}

	app := &App{
		Store:   store,
		Catalog: catalog.NewManager(),
	}

	if err := app.Store.Update(func(tx *storage.Tx) error {
		return app.Catalog.Bootstrap(tx)
	}); err != nil {
		_ = store.Close()
		return nil, err
	}

	return app, nil
}

// Close releases the underlying storage handle.
func (a *App) Close() error {
	if a == nil || a.Store == nil {
		return nil
	}

	return a.Store.Close()
}
