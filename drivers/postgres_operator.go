package drivers

import (
	"database/sql"
	"fmt"
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

func (o *PostgresOperator) CreateInstruction() *PostgresCreateOperatorInstruction {
	return &PostgresCreateOperatorInstruction{
		Name:          o.Name,
		Function:      o.Function,
		LeftArgument:  o.LeftArgument,
		RightArgument: o.RightArgument,
	}
}

func (o *PostgresOperator) DropInstruction() *PostgresDropOperatorInstruction {
	return &PostgresDropOperatorInstruction{
		Name:          o.Name,
		LeftArgument:  o.LeftArgument,
		RightArgument: o.RightArgument,
	}
}

// PostgreSQL changes the owner and the support options of an operator only, so a new
// function needs a recreation.
func (o *PostgresOperator) Diff(other *PostgresOperator) []Instruction {
	if o.Equal(other) {
		return nil
	}

	return []Instruction{other.DropInstruction(), o.CreateInstruction()}
}
