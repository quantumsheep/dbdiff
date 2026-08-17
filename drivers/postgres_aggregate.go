package drivers

import (
	"database/sql"
	"fmt"
)

type PostgresAggregate struct {
	Name               string
	Arguments          string
	StateType          string
	TransitionFunction string
	FinalFunction      sql.NullString
	InitialCondition   sql.NullString
}

func (a *PostgresAggregate) Signature() string {
	return fmt.Sprintf("%s(%s)", a.Name, a.Arguments)
}

func (a *PostgresAggregate) Equal(other *PostgresAggregate) bool {
	return *a == *other
}

func (a *PostgresAggregate) CreateInstruction() *PostgresCreateAggregateInstruction {
	return &PostgresCreateAggregateInstruction{
		Name:               a.Name,
		Arguments:          a.Arguments,
		TransitionFunction: a.TransitionFunction,
		StateType:          a.StateType,
		FinalFunction:      a.FinalFunction,
		InitialCondition:   a.InitialCondition,
	}
}

func (a *PostgresAggregate) DropInstruction() *PostgresDropAggregateInstruction {
	return &PostgresDropAggregateInstruction{Name: a.Name, Arguments: a.Arguments}
}

// PostgreSQL changes the name and the owner of an aggregate only, so every other change
// needs a recreation.
func (a *PostgresAggregate) Diff(other *PostgresAggregate) []Instruction {
	if a.Equal(other) {
		return nil
	}

	return []Instruction{other.DropInstruction(), a.CreateInstruction()}
}
