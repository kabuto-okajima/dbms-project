package statement

import (
	"fmt"

	"dbms-project/internal/catalog"
	"dbms-project/internal/storage"
)

// DropTableExecutor is a thin wrapper around catalog table deletion.
type DropTableExecutor struct {
	Catalog *catalog.Manager
}

func NewDropTableExecutor(manager *catalog.Manager) *DropTableExecutor {
	return &DropTableExecutor{Catalog: manager}
}

// Execute delegates DROP TABLE work to the catalog layer.
func (e *DropTableExecutor) Execute(tx *storage.Tx, tableName string) (Result, error) {
	if e.Catalog == nil {
		e.Catalog = catalog.NewManager()
	}

	if err := e.Catalog.DropTable(tx, tableName); err != nil {
		return Result{}, err
	}

	return Result{
		Message: fmt.Sprintf("table %s dropped", tableName),
	}, nil
}
