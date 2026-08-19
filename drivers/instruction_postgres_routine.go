package drivers

import (
	"database/sql"
	"fmt"
	"strings"
)

type PostgresCreateFunctionInstruction struct {
	Definition string
}

func (i *PostgresCreateFunctionInstruction) String() string {
	return i.Definition + ";"
}

type PostgresDropFunctionInstruction struct {
	Name      string
	Arguments string
}

func (i *PostgresDropFunctionInstruction) String() string {
	return fmt.Sprintf("DROP FUNCTION %s(%s);", quoteIdentifier(i.Name), i.Arguments)
}

type PostgresCreateAggregateInstruction struct {
	Name               string
	Arguments          string
	TransitionFunction string
	StateType          string
	FinalFunction      sql.NullString
	InitialCondition   sql.NullString
}

func (i *PostgresCreateAggregateInstruction) String() string {
	options := []string{
		fmt.Sprintf("SFUNC = %s", quoteIdentifier(i.TransitionFunction)),
		fmt.Sprintf("STYPE = %s", i.StateType),
	}

	if i.FinalFunction.Valid {
		options = append(options,
			fmt.Sprintf("FINALFUNC = %s", quoteIdentifier(i.FinalFunction.String)))
	}

	if i.InitialCondition.Valid {
		options = append(options,
			fmt.Sprintf("INITCOND = %s", quoteLiteral(i.InitialCondition.String)))
	}

	return fmt.Sprintf("CREATE AGGREGATE %s(%s) (%s);",
		quoteIdentifier(i.Name), i.Arguments, strings.Join(options, ", "))
}

type PostgresDropAggregateInstruction struct {
	Name      string
	Arguments string
}

func (i *PostgresDropAggregateInstruction) String() string {
	return fmt.Sprintf("DROP AGGREGATE %s(%s);", quoteIdentifier(i.Name), i.Arguments)
}

// The name of an operator holds punctuation marks only, so it takes no quotes.
type PostgresCreateOperatorInstruction struct {
	Name          string
	Function      string
	LeftArgument  sql.NullString
	RightArgument sql.NullString
}

func (i *PostgresCreateOperatorInstruction) String() string {
	options := []string{fmt.Sprintf("FUNCTION = %s", quoteIdentifier(i.Function))}

	if i.LeftArgument.Valid {
		options = append(options, fmt.Sprintf("LEFTARG = %s", i.LeftArgument.String))
	}

	if i.RightArgument.Valid {
		options = append(options, fmt.Sprintf("RIGHTARG = %s", i.RightArgument.String))
	}

	return fmt.Sprintf("CREATE OPERATOR %s (%s);", i.Name, strings.Join(options, ", "))
}

type PostgresDropOperatorInstruction struct {
	Name          string
	LeftArgument  sql.NullString
	RightArgument sql.NullString
}

func (i *PostgresDropOperatorInstruction) String() string {
	return fmt.Sprintf("DROP OPERATOR %s (%s, %s);",
		i.Name,
		postgresOperatorArgument(i.LeftArgument),
		postgresOperatorArgument(i.RightArgument))
}
