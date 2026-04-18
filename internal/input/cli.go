package input

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"dbms-project/internal/app"
	"dbms-project/internal/format"
)

const (
	primaryPrompt      = "db> "
	continuationPrompt = "...> "
)

// SQLExecutor is the app-layer contract the CLI needs.
type SQLExecutor interface {
	ExecuteSQL(sql string) (app.Result, error)
}

// Run opens the database-backed app and starts the interactive CLI loop.
func Run(dbPath string, in io.Reader, out io.Writer, errOut io.Writer) error {
	application, err := app.New(dbPath)
	if err != nil {
		return err
	}
	defer application.Close()

	return RunSession(in, out, errOut, application)
}

// RunSession executes the interactive SQL loop against one executor.
func RunSession(in io.Reader, out io.Writer, errOut io.Writer, executor SQLExecutor) error {
	scanner := bufio.NewScanner(in)
	var statementLines []string

	writePrompt(out, primaryPrompt)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if len(statementLines) == 0 && isExitCommand(trimmed) {
			return nil
		}
		if trimmed == "" && len(statementLines) == 0 {
			writePrompt(out, primaryPrompt)
			continue
		}

		statementLines = append(statementLines, line)
		statementText := strings.TrimSpace(strings.Join(statementLines, "\n"))

		// If the statement doesn't end with a semicolon, it's not complete yet.
		if !strings.HasSuffix(statementText, ";") {
			writePrompt(out, continuationPrompt)
			continue
		}

		statementText = strings.TrimSpace(strings.TrimSuffix(statementText, ";"))
		statementLines = nil

		if statementText != "" {
			result, err := executor.ExecuteSQL(statementText)
			if err != nil {
				fmt.Fprintf(errOut, "Error: %v\n", err)
			} else {
				fmt.Fprintln(out, format.Render(result))
			}
		}

		writePrompt(out, primaryPrompt)
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func writePrompt(out io.Writer, prompt string) {
	if out != nil {
		fmt.Fprint(out, prompt)
	}
}

func isExitCommand(text string) bool {
	return strings.EqualFold(text, "exit") || strings.EqualFold(text, "quit")
}
