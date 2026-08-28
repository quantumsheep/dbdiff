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

func (i *PostgresCreateFunctionInstruction) Comment() string {
	return definitionComment("Create", "function", i.Definition, "FUNCTION")
}

type PostgresDropFunctionInstruction struct {
	Name      string
	Arguments string
}

func (i *PostgresDropFunctionInstruction) String() string {
	return fmt.Sprintf("DROP FUNCTION %s(%s);", QuoteIdentifier(i.Name), i.Arguments)
}

func (i *PostgresDropFunctionInstruction) Comment() string {
	return objectComment("Drop", "function", i.Name)
}

type PostgresCreateProcedureInstruction struct {
	Definition string
}

func (i *PostgresCreateProcedureInstruction) String() string {
	return i.Definition + ";"
}

func (i *PostgresCreateProcedureInstruction) Comment() string {
	return definitionComment("Create", "procedure", i.Definition, "PROCEDURE")
}

type PostgresDropProcedureInstruction struct {
	Name      string
	Arguments string
}

func (i *PostgresDropProcedureInstruction) String() string {
	return fmt.Sprintf("DROP PROCEDURE %s(%s);", QuoteIdentifier(i.Name), i.Arguments)
}

func (i *PostgresDropProcedureInstruction) Comment() string {
	return objectComment("Drop", "procedure", i.Name)
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
		fmt.Sprintf("SFUNC = %s", QuoteIdentifier(i.TransitionFunction)),
		fmt.Sprintf("STYPE = %s", i.StateType),
	}

	if i.FinalFunction.Valid {
		options = append(options,
			fmt.Sprintf("FINALFUNC = %s", QuoteIdentifier(i.FinalFunction.String)))
	}

	if i.InitialCondition.Valid {
		options = append(options,
			fmt.Sprintf("INITCOND = %s", quoteLiteral(i.InitialCondition.String)))
	}

	return fmt.Sprintf("CREATE AGGREGATE %s(%s) (%s);",
		QuoteIdentifier(i.Name), i.Arguments, strings.Join(options, ", "))
}

func (i *PostgresCreateAggregateInstruction) Comment() string {
	return objectComment("Create", "aggregate", i.Name)
}

type PostgresDropAggregateInstruction struct {
	Name      string
	Arguments string
}

func (i *PostgresDropAggregateInstruction) String() string {
	return fmt.Sprintf("DROP AGGREGATE %s(%s);", QuoteIdentifier(i.Name), i.Arguments)
}

func (i *PostgresDropAggregateInstruction) Comment() string {
	return objectComment("Drop", "aggregate", i.Name)
}

// The name of an operator holds punctuation marks only, so it takes no quotes.
type PostgresCreateOperatorInstruction struct {
	Name          string
	Function      string
	LeftArgument  sql.NullString
	RightArgument sql.NullString
}

func (i *PostgresCreateOperatorInstruction) String() string {
	options := []string{fmt.Sprintf("FUNCTION = %s", QuoteIdentifier(i.Function))}

	if i.LeftArgument.Valid {
		options = append(options, fmt.Sprintf("LEFTARG = %s", i.LeftArgument.String))
	}

	if i.RightArgument.Valid {
		options = append(options, fmt.Sprintf("RIGHTARG = %s", i.RightArgument.String))
	}

	return fmt.Sprintf("CREATE OPERATOR %s (%s);", i.Name, strings.Join(options, ", "))
}

func (i *PostgresCreateOperatorInstruction) Comment() string {
	return "Create the operator " + i.Name
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

func (i *PostgresDropOperatorInstruction) Comment() string {
	return "Drop the operator " + i.Name
}
