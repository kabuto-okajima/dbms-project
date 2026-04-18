package format

import (
	"fmt"
	"strconv"
	"strings"

	"dbms-project/internal/app"
	"dbms-project/internal/shared"
	"dbms-project/internal/storage"
)

// Render turns one app-layer result into a user-facing string.
func Render(result app.Result) string {
	if result.HasRows() {
		return renderTableResult(result)
	}
	return renderStatusResult(result)
}

func renderStatusResult(result app.Result) string {
	lines := make([]string, 0, 3)

	if result.Message != "" {
		lines = append(lines, result.Message)
	} else {
		lines = append(lines, "ok")
	}

	lines = append(lines, fmt.Sprintf("Time: %s", formatDuration(result.Elapsed)))
	return strings.Join(lines, "\n")
}

func renderTableResult(result app.Result) string {
	headers := make([]string, 0, len(result.Columns))
	widths := make([]int, 0, len(result.Columns))
	for _, column := range result.Columns {
		headers = append(headers, column.Name)
		widths = append(widths, len(column.Name))
	}

	renderedRows := make([][]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		rendered := make([]string, 0, len(headers))
		for i := 0; i < len(headers); i++ {
			cell := ""
			if i < len(row) {
				cell = formatValue(row[i])
			}
			rendered = append(rendered, cell)
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
		renderedRows = append(renderedRows, rendered)
	}

	var builder strings.Builder
	border := buildBorder(widths)
	builder.WriteString(border)
	builder.WriteByte('\n')
	builder.WriteString(buildLine(headers, widths))
	builder.WriteByte('\n')
	builder.WriteString(border)

	for _, row := range renderedRows {
		builder.WriteByte('\n')
		builder.WriteString(buildLine(row, widths))
	}

	builder.WriteByte('\n')
	builder.WriteString(border)
	builder.WriteByte('\n')
	builder.WriteString(fmt.Sprintf("%d %s in set (%s)", len(result.Rows), pluralize("row", len(result.Rows)), formatDuration(result.Elapsed)))

	return builder.String()
}

func buildBorder(widths []int) string {
	var builder strings.Builder
	builder.WriteByte('+')
	for _, width := range widths {
		builder.WriteString(strings.Repeat("-", width+2))
		builder.WriteByte('+')
	}
	return builder.String()
}

func buildLine(values []string, widths []int) string {
	var builder strings.Builder
	builder.WriteByte('|')
	for i, value := range values {
		builder.WriteByte(' ')
		builder.WriteString(value)
		builder.WriteString(strings.Repeat(" ", widths[i]-len(value)+1))
		builder.WriteByte('|')
	}
	return builder.String()
}

func formatValue(value storage.Value) string {
	switch value.Type {
	case shared.TypeInteger:
		return strconv.FormatInt(value.IntegerValue, 10)
	case shared.TypeString:
		return value.StringValue
	default:
		return ""
	}
}

func formatDuration(duration interface{ String() string }) string {
	return duration.String()
}

func pluralize(word string, count int) string {
	if count == 1 {
		return word
	}
	return word + "s"
}
