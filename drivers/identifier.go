package drivers

import "strings"

func QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func QuoteIdentifiers(names []string) []string {
	quoted := make([]string, len(names))

	for i, name := range names {
		quoted[i] = QuoteIdentifier(name)
	}

	return quoted
}
