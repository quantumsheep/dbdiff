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
	TransitionSpace    int64
	FinalFunction      sql.NullString
	FinalFunctionExtra bool

	// An empty value is the default READ_ONLY, so an equal pair compares as equal.
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

	// An empty value is the default UNSAFE, so an equal pair compares as equal.
	Parallel string
}

func (a *PostgresAggregate) Signature() string {
	return fmt.Sprintf("%s(%s)", a.Name, a.Arguments)
}

func (a *PostgresAggregate) Equal(other *PostgresAggregate) bool {
	return *a == *other
}

func (a *PostgresAggregate) CreateInstruction() *PostgresCreateAggregateInstruction {
	return &PostgresCreateAggregateInstruction{
		Name:                            a.Name,
		Arguments:                       a.Arguments,
		TransitionFunction:              a.TransitionFunction,
		TransitionSpace:                 a.TransitionSpace,
		StateType:                       a.StateType,
		FinalFunction:                   a.FinalFunction,
		FinalFunctionExtra:              a.FinalFunctionExtra,
		FinalFunctionModify:             a.FinalFunctionModify,
		CombineFunction:                 a.CombineFunction,
		SerializeFunction:               a.SerializeFunction,
		DeserializeFunction:             a.DeserializeFunction,
		InitialCondition:                a.InitialCondition,
		MovingTransitionFunction:        a.MovingTransitionFunction,
		MovingInverseTransitionFunction: a.MovingInverseTransitionFunction,
		MovingStateType:                 a.MovingStateType,
		MovingTransitionSpace:           a.MovingTransitionSpace,
		MovingFinalFunction:             a.MovingFinalFunction,
		MovingFinalFunctionExtra:        a.MovingFinalFunctionExtra,
		MovingFinalFunctionModify:       a.MovingFinalFunctionModify,
		MovingInitialCondition:          a.MovingInitialCondition,
		SortOperator:                    a.SortOperator,
		Parallel:                        a.Parallel,
	}
}

func (a *PostgresAggregate) DropInstruction() *PostgresDropAggregateInstruction {
	return &PostgresDropAggregateInstruction{
		Name:      a.Name,
		Arguments: a.Arguments,
	}
}

// PostgreSQL changes the name and the owner of an aggregate only, so every other change
// needs a recreation.
func (a *PostgresAggregate) Diff(other *PostgresAggregate) []Instruction {
	if a.Equal(other) {
		return nil
	}

	return []Instruction{other.DropInstruction(), a.CreateInstruction()}
}
