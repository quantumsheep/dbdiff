package drivers

import (
	"database/sql"
	"fmt"
	"strings"
)

type PostgresOperator struct {
	Name          string
	LeftArgument  sql.NullString
	RightArgument sql.NullString
	Function      string
}

func postgresOperatorArgument(argument sql.NullString) string {
	if argument.Valid {
		return argument.String
	}

	return "NONE"
}

func (o *PostgresOperator) Signature() string {
	return fmt.Sprintf("%s(%s, %s)", o.Name, postgresOperatorArgument(o.LeftArgument), postgresOperatorArgument(o.RightArgument))
}

func (o *PostgresOperator) Equal(other *PostgresOperator) bool {
	return *o == *other
}

// The name of an operator holds punctuation marks only, so it takes no quotes.
func (o *PostgresOperator) String() string {
	options := []string{fmt.Sprintf("FUNCTION = %s", quoteIdentifier(o.Function))}

	if o.LeftArgument.Valid {
		options = append(options, fmt.Sprintf("LEFTARG = %s", o.LeftArgument.String))
	}

	if o.RightArgument.Valid {
		options = append(options, fmt.Sprintf("RIGHTARG = %s", o.RightArgument.String))
	}

	return fmt.Sprintf("CREATE OPERATOR %s (%s);", o.Name, strings.Join(options, ", "))
}

func (o *PostgresOperator) StringDrop() string {
	return fmt.Sprintf(
		"DROP OPERATOR %s (%s, %s);",
		o.Name,
		postgresOperatorArgument(o.LeftArgument),
		postgresOperatorArgument(o.RightArgument),
	)
}

// PostgreSQL changes the owner and the support options of an operator only, so a new
// function needs a recreation.
func (o *PostgresOperator) Diff(other *PostgresOperator) string {
	if o.Equal(other) {
		return ""
	}

	var diff strings.Builder

	fmt.Fprintf(&diff, "%s\n", other.StringDrop())
	fmt.Fprintf(&diff, "%s\n", o.String())

	return strings.TrimSpace(diff.String())
}
