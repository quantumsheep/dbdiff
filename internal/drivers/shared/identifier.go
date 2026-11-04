package driversshared

import (
	"strings"

	"github.com/samber/lo"
)

func QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func QuoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func QuoteIdentifiers(names []string) []string {
	return lo.Map(names, func(name string, _ int) string {
		return QuoteIdentifier(name)
	})
}
