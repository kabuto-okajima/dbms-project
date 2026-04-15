package app

import (
	"path/filepath"
	"testing"

	"dbms-project/internal/catalog"
	"dbms-project/internal/storage"
)

// TestNewBootstrapsCatalog confirms app startup opens the DB and creates catalog buckets.
func TestNewBootstrapsCatalog(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	app, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	if app.Store == nil {
		t.Fatal("expected app store to be initialized")
	}
	if app.Catalog == nil {
		t.Fatal("expected app catalog manager to be initialized")
	}

	err = app.Store.View(func(tx *storage.Tx) error {
		for _, bucket := range []string{
			catalog.TablesBucket,
			catalog.ColumnsBucket,
			catalog.PrimaryKeysBucket,
			catalog.ForeignKeysBucket,
			catalog.IndexesBucket,
		} {
			if _, err := tx.Get(bucket, []byte("missing")); err != nil && err != storage.ErrBucketNotFound {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
