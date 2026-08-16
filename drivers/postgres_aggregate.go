package drivers

import (
	"database/sql"
	"fmt"
	"strings"
)

type PostgresAggregate struct {
	Name               string
	Arguments          string
	StateType          string
	TransitionFunction string
	FinalFunction      sql.NullString
	InitialCondition   sql.NullString
}

// Signature identifies the aggregate. PostgreSQL accepts several aggregates with one name
// and different arguments.
func (a *PostgresAggregate) Signature() string {
	return fmt.Sprintf("%s(%s)", a.Name, a.Arguments)
}

func (a *PostgresAggregate) Equal(other *PostgresAggregate) bool {
	return *a == *other
}

func (a *PostgresAggregate) String() string {
	options := []string{
		fmt.Sprintf("SFUNC = %s", quoteIdentifier(a.TransitionFunction)),
		fmt.Sprintf("STYPE = %s", a.StateType),
	}

	if a.FinalFunction.Valid {
		options = append(options, fmt.Sprintf("FINALFUNC = %s", quoteIdentifier(a.FinalFunction.String)))
	}

	if a.InitialCondition.Valid {
		options = append(options, fmt.Sprintf("INITCOND = %s", quoteLiteral(a.InitialCondition.String)))
	}

	return fmt.Sprintf("CREATE AGGREGATE %s(%s) (%s);", quoteIdentifier(a.Name), a.Arguments, strings.Join(options, ", "))
}

func (a *PostgresAggregate) StringDrop() string {
	return fmt.Sprintf("DROP AGGREGATE %s(%s);", quoteIdentifier(a.Name), a.Arguments)
}

// Diff returns the statements that make other equal to a. PostgreSQL changes the name and
// the owner of an aggregate only, so every other change needs a recreation.
func (a *PostgresAggregate) Diff(other *PostgresAggregate) string {
	if a.Equal(other) {
		return ""
	}

	var diff strings.Builder

	fmt.Fprintf(&diff, "%s\n", other.StringDrop())
	fmt.Fprintf(&diff, "%s\n", a.String())

	return strings.TrimSpace(diff.String())
}
