package main

import (
	"fmt"
	"os"

	"dbms-project/internal/input"
)

func main() {
	dbPath := "dbms.db"
	if len(os.Args) > 1 && os.Args[1] != "" {
		dbPath = os.Args[1]
	}

	if err := input.Run(dbPath, os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start DBMS: %v\n", err)
		os.Exit(1)
	}
}
