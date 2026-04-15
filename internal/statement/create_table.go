package statement

import (
	"fmt"

	"dbms-project/internal/catalog"
	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

// Result is the simple status payload returned by statement executors for now.
type Result struct {
	Message      string
	AffectedRows int
}

// CreateTableExecutor is a thin wrapper around catalog table creation.
type CreateTableExecutor struct {
	Catalog *catalog.Manager
}

func NewCreateTableExecutor(manager *catalog.Manager) *CreateTableExecutor {
	return &CreateTableExecutor{Catalog: manager}
}

// Execute delegates CREATE TABLE work to the catalog layer.
func (e *CreateTableExecutor) Execute(tx *storage.Tx, def shared.TableDefinition) (Result, error) {
	if e.Catalog == nil {
		e.Catalog = catalog.NewManager()
	}

	if err := e.Catalog.CreateTable(tx, def); err != nil {
		return Result{}, err
	}

	return Result{
		Message: fmt.Sprintf("table %s created", def.Name),
	}, nil
}
