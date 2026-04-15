package statement

import (
	"fmt"

	"dbms-project/internal/catalog"
	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

// CreateIndexExecutor is a thin wrapper around catalog index creation.
type CreateIndexExecutor struct {
	Catalog *catalog.Manager
}

func NewCreateIndexExecutor(manager *catalog.Manager) *CreateIndexExecutor {
	return &CreateIndexExecutor{Catalog: manager}
}

// Execute delegates CREATE INDEX work to the catalog layer.
func (e *CreateIndexExecutor) Execute(tx *storage.Tx, def shared.IndexDefinition) (Result, error) {
	if e.Catalog == nil {
		e.Catalog = catalog.NewManager()
	}

	if err := e.Catalog.CreateIndex(tx, def); err != nil {
		return Result{}, err
	}

	return Result{
		Message: fmt.Sprintf("index %s created", def.Name),
	}, nil
}
