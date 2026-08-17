package drivers

import (
	"fmt"
	"slices"
	"strings"
)

type PostgresType struct {
	Name   string
	Values []string
}

func (t *PostgresType) String() string {
	quotedValues := make([]string, len(t.Values))

	for i, value := range t.Values {
		quotedValues[i] = quoteLiteral(value)
	}

	return fmt.Sprintf("CREATE TYPE %s AS ENUM (%s);", quoteIdentifier(t.Name), strings.Join(quotedValues, ", "))
}

func (t *PostgresType) StartsWith(other *PostgresType) bool {
	if len(other.Values) > len(t.Values) {
		return false
	}

	return slices.Equal(t.Values[:len(other.Values)], other.Values)
}

func (t *PostgresType) Diff(other *PostgresType) string {
	if slices.Equal(t.Values, other.Values) {
		return ""
	}

	var diff strings.Builder

	// PostgreSQL adds a value to an enum, but it removes none and it moves none.
	if t.StartsWith(other) {
		for _, value := range t.Values[len(other.Values):] {
			fmt.Fprintf(&diff, "ALTER TYPE %s ADD VALUE %s;\n", quoteIdentifier(t.Name), quoteLiteral(value))
		}

		return strings.TrimSpace(diff.String())
	}

	fmt.Fprintf(&diff, "DROP TYPE %s;\n", quoteIdentifier(t.Name))
	fmt.Fprintf(&diff, "%s\n", t.String())

	return strings.TrimSpace(diff.String())
}
