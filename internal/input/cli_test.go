package input

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"dbms-project/internal/app"
)

func TestRunSessionExecutesStatementsAndFormatsOutput(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	application, err := app.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close()

	in := strings.NewReader("" +
		"create table students (id int primary key, name varchar(255));\n" +
		"insert into students (id, name) values (1, 'Ada');\n" +
		"select name from students;\n" +
		"quit\n")
	var out bytes.Buffer
	var errOut bytes.Buffer

	if err := RunSession(in, &out, &errOut, application); err != nil {
		t.Fatal(err)
	}

	gotOut := out.String()
	if !strings.Contains(gotOut, "db> ") {
		t.Fatalf("expected primary prompt in output, got:\n%s", gotOut)
	}
	if !strings.Contains(gotOut, "table students created") {
		t.Fatalf("expected create-table output, got:\n%s", gotOut)
	}
	if !strings.Contains(gotOut, "1 row inserted") {
		t.Fatalf("expected insert output, got:\n%s", gotOut)
	}
	if !strings.Contains(gotOut, "| name |") || !strings.Contains(gotOut, "| Ada  |") {
		t.Fatalf("expected rendered select table, got:\n%s", gotOut)
	}
	if errOut.Len() != 0 {
		t.Fatalf("expected empty stderr, got:\n%s", errOut.String())
	}
}

func TestRunSessionSupportsMultilineStatementsAndErrors(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	application, err := app.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close()

	in := strings.NewReader("" +
		"create table students (\n" +
		"  id int primary key,\n" +
		"  name varchar(255)\n" +
		");\n" +
		"select missing from students;\n" +
		"exit\n")
	var out bytes.Buffer
	var errOut bytes.Buffer

	if err := RunSession(in, &out, &errOut, application); err != nil {
		t.Fatal(err)
	}

	gotOut := out.String()
	if !strings.Contains(gotOut, "...> ") {
		t.Fatalf("expected continuation prompt for multiline statement, got:\n%s", gotOut)
	}
	if !strings.Contains(gotOut, "table students created") {
		t.Fatalf("expected create-table output, got:\n%s", gotOut)
	}

	gotErr := errOut.String()
	if !strings.Contains(gotErr, "Error:") {
		t.Fatalf("expected formatted error output, got:\n%s", gotErr)
	}
	if !strings.Contains(gotErr, "missing") {
		t.Fatalf("expected select error details, got:\n%s", gotErr)
	}
}
