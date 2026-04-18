package format

import (
	"testing"
	"time"

	"dbms-project/internal/app"
	"dbms-project/internal/parser"
	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

func TestRenderStatusResult(t *testing.T) {
	output := Render(app.Result{
		Kind:         parser.StatementInsert,
		Message:      "1 row inserted",
		AffectedRows: 1,
		Elapsed:      2 * time.Millisecond,
	})

	want := "1 row inserted\nTime: 2ms"
	if output != want {
		t.Fatalf("unexpected status output:\n%s\nwant:\n%s", output, want)
	}
}

func TestRenderSelectResult(t *testing.T) {
	output := Render(app.Result{
		Kind: parser.StatementSelect,
		Columns: []app.ResultColumn{
			{Name: "name", Type: shared.TypeString},
		},
		Rows: []storage.Row{
			{storage.NewStringValue("Carol")},
			{storage.NewStringValue("Bob")},
		},
		Elapsed: 5 * time.Millisecond,
	})

	want := "" +
		"+-------+\n" +
		"| name  |\n" +
		"+-------+\n" +
		"| Carol |\n" +
		"| Bob   |\n" +
		"+-------+\n" +
		"2 rows in set (5ms)"
	if output != want {
		t.Fatalf("unexpected select output:\n%s\nwant:\n%s", output, want)
	}
}

func TestRenderEmptySelectResult(t *testing.T) {
	output := Render(app.Result{
		Kind: parser.StatementSelect,
		Columns: []app.ResultColumn{
			{Name: "id", Type: shared.TypeInteger},
		},
		Rows:    nil,
		Elapsed: time.Millisecond,
	})

	want := "" +
		"+----+\n" +
		"| id |\n" +
		"+----+\n" +
		"+----+\n" +
		"0 rows in set (1ms)"
	if output != want {
		t.Fatalf("unexpected empty select output:\n%s\nwant:\n%s", output, want)
	}
}
