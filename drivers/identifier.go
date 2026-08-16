package drivers

import "strings"

// SQLite and PostgreSQL both wrap an identifier in double quotes. A double quote inside
// the name becomes two double quotes.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// SQLite and PostgreSQL both wrap a text value in single quotes. A single quote inside
// the value becomes two single quotes.
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
