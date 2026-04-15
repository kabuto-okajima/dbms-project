package statement

import (
	"fmt"

	"dbms-project/internal/catalog"
	"dbms-project/internal/storage"
)

// DropIndexExecutor is a thin wrapper around catalog index deletion.
type DropIndexExecutor struct {
	Catalog *catalog.Manager
}

func NewDropIndexExecutor(manager *catalog.Manager) *DropIndexExecutor {
	return &DropIndexExecutor{Catalog: manager}
}

// Execute delegates DROP INDEX work to the catalog layer.
func (e *DropIndexExecutor) Execute(tx *storage.Tx, indexName string) (Result, error) {
	if e.Catalog == nil {
		e.Catalog = catalog.NewManager()
	}

	if err := e.Catalog.DropIndex(tx, indexName); err != nil {
		return Result{}, err
	}

	return Result{
		Message: fmt.Sprintf("index %s dropped", indexName),
	}, nil
}
