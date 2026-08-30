package driverspostgres

import (
	"fmt"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func castMethodClause(method string, function string) string {
	switch method {
	case "i":
		return "WITH INOUT"
	case "b":
		return "WITHOUT FUNCTION"
	default:
		return "WITH FUNCTION " + function
	}
}

func castContextClause(context string) string {
	switch context {
	case "a":
		return " AS ASSIGNMENT"
	case "i":
		return " AS IMPLICIT"
	default:
		return ""
	}
}

type PostgresCreateCastInstruction struct {
	SourceType string
	TargetType string
	Method     string
	Context    string
	Function   string
}

func (i *PostgresCreateCastInstruction) String() string {
	return fmt.Sprintf("CREATE CAST (%s AS %s) %s%s;",
		i.SourceType, i.TargetType, castMethodClause(i.Method, i.Function), castContextClause(i.Context))
}

func (i *PostgresCreateCastInstruction) Comment() string {
	return "Create the cast " + driversshared.QuoteIdentifier(i.SourceType) + " to " +
		driversshared.QuoteIdentifier(i.TargetType)
}

type PostgresDropCastInstruction struct {
	SourceType string
	TargetType string
}

func (i *PostgresDropCastInstruction) String() string {
	return fmt.Sprintf("DROP CAST (%s AS %s);", i.SourceType, i.TargetType)
}

func (i *PostgresDropCastInstruction) Comment() string {
	return "Drop the cast " + driversshared.QuoteIdentifier(i.SourceType) + " to " +
		driversshared.QuoteIdentifier(i.TargetType)
}
