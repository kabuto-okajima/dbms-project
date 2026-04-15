package app

import (
	"dbms-project/internal/catalog"
	"dbms-project/internal/storage"
)

// App owns the long-lived services needed by the DBMS process.
type App struct {
	Store   *storage.Store
	Catalog *catalog.Manager
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
