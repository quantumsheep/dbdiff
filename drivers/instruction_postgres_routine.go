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
	Name                string
	Arguments           string
	TransitionFunction  string
	TransitionSpace     int64
	StateType           string
	FinalFunction       sql.NullString
	FinalFunctionExtra  bool
	FinalFunctionModify string
	CombineFunction     sql.NullString
	SerializeFunction   sql.NullString
	DeserializeFunction sql.NullString
	InitialCondition    sql.NullString

	MovingTransitionFunction        sql.NullString
	MovingInverseTransitionFunction sql.NullString
	MovingStateType                 sql.NullString
	MovingTransitionSpace           int64
	MovingFinalFunction             sql.NullString
	MovingFinalFunctionExtra        bool
	MovingFinalFunctionModify       string
	MovingInitialCondition          sql.NullString

	SortOperator sql.NullString
	Parallel     string
}

func finalFunctionModifyWord(value string) string {
	switch value {
	case "s":
		return "SHAREABLE"
	case "w":
		return "READ_WRITE"
	}

	return "READ_ONLY"
}

func parallelWord(value string) string {
	switch value {
	case "s":
		return "SAFE"
	case "r":
		return "RESTRICTED"
	}

	return "UNSAFE"
}

func (i *PostgresCreateAggregateInstruction) String() string {
	options := []string{
		// The value comes from a regproc cast, which quotes and qualifies the name.
		fmt.Sprintf("SFUNC = %s", i.TransitionFunction),
		fmt.Sprintf("STYPE = %s", i.StateType),
	}

	if i.TransitionSpace != 0 {
		options = append(options, fmt.Sprintf("SSPACE = %d", i.TransitionSpace))
	}

	if i.FinalFunction.Valid {
		options = append(options,
			fmt.Sprintf("FINALFUNC = %s", i.FinalFunction.String))
	}

	if i.FinalFunctionExtra {
		options = append(options, "FINALFUNC_EXTRA")
	}

	if i.FinalFunctionModify != "" {
		options = append(options,
			fmt.Sprintf("FINALFUNC_MODIFY = %s", finalFunctionModifyWord(i.FinalFunctionModify)))
	}

	if i.CombineFunction.Valid {
		options = append(options,
			fmt.Sprintf("COMBINEFUNC = %s", i.CombineFunction.String))
	}

	if i.SerializeFunction.Valid {
		options = append(options,
			fmt.Sprintf("SERIALFUNC = %s", i.SerializeFunction.String))
	}

	if i.DeserializeFunction.Valid {
		options = append(options,
			fmt.Sprintf("DESERIALFUNC = %s", i.DeserializeFunction.String))
	}

	if i.InitialCondition.Valid {
		options = append(options,
			fmt.Sprintf("INITCOND = %s", quoteLiteral(i.InitialCondition.String)))
	}

	if i.MovingTransitionFunction.Valid {
		options = append(options,
			fmt.Sprintf("MSFUNC = %s", i.MovingTransitionFunction.String))
	}

	if i.MovingInverseTransitionFunction.Valid {
		options = append(options,
			fmt.Sprintf("MINVFUNC = %s", i.MovingInverseTransitionFunction.String))
	}

	if i.MovingStateType.Valid {
		options = append(options,
			fmt.Sprintf("MSTYPE = %s", i.MovingStateType.String))
	}

	if i.MovingTransitionSpace != 0 {
		options = append(options, fmt.Sprintf("MSSPACE = %d", i.MovingTransitionSpace))
	}

	if i.MovingFinalFunction.Valid {
		options = append(options,
			fmt.Sprintf("MFINALFUNC = %s", i.MovingFinalFunction.String))
	}

	if i.MovingFinalFunctionExtra {
		options = append(options, "MFINALFUNC_EXTRA")
	}

	if i.MovingFinalFunctionModify != "" {
		options = append(options,
			fmt.Sprintf("MFINALFUNC_MODIFY = %s", finalFunctionModifyWord(i.MovingFinalFunctionModify)))
	}

	if i.MovingInitialCondition.Valid {
		options = append(options,
			fmt.Sprintf("MINITCOND = %s", quoteLiteral(i.MovingInitialCondition.String)))
	}

	if i.SortOperator.Valid {
		options = append(options,
			fmt.Sprintf("SORTOP = OPERATOR(%s)", i.SortOperator.String))
	}

	if i.Parallel != "" {
		options = append(options,
			fmt.Sprintf("PARALLEL = %s", parallelWord(i.Parallel)))
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
	Name             string
	Function         string
	LeftArgument     sql.NullString
	RightArgument    sql.NullString
	Commutator       sql.NullString
	Negator          sql.NullString
	RestrictFunction sql.NullString
	JoinFunction     sql.NullString
	CanHash          bool
	CanMerge         bool
}

func (i *PostgresCreateOperatorInstruction) String() string {
	options := []string{fmt.Sprintf("FUNCTION = %s", QuoteIdentifier(i.Function))}

	if i.LeftArgument.Valid {
		options = append(options, fmt.Sprintf("LEFTARG = %s", i.LeftArgument.String))
	}

	if i.RightArgument.Valid {
		options = append(options, fmt.Sprintf("RIGHTARG = %s", i.RightArgument.String))
	}

	if i.Commutator.Valid {
		options = append(options, fmt.Sprintf("COMMUTATOR = %s", i.Commutator.String))
	}

	if i.Negator.Valid {
		options = append(options, fmt.Sprintf("NEGATOR = %s", i.Negator.String))
	}

	if i.RestrictFunction.Valid {
		options = append(options, fmt.Sprintf("RESTRICT = %s", i.RestrictFunction.String))
	}

	if i.JoinFunction.Valid {
		options = append(options, fmt.Sprintf("JOIN = %s", i.JoinFunction.String))
	}

	if i.CanHash {
		options = append(options, "HASHES")
	}

	if i.CanMerge {
		options = append(options, "MERGES")
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
