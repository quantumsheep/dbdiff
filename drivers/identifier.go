package drivers

import "strings"

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func quoteIdentifiers(names []string) []string {
	quoted := make([]string, len(names))

	for i, name := range names {
		quoted[i] = quoteIdentifier(name)
	}

	return quoted
}
