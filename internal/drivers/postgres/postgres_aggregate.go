package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
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
func (a *PostgresAggregate) Diff(other *PostgresAggregate) []driversshared.Instruction {
	if a.Equal(other) {
		return nil
	}

	return []driversshared.Instruction{other.DropInstruction(), a.CreateInstruction()}
}

func (d *PostgresDriver) DiffAggregates(ctx context.Context) (*SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresAggregate]{
		Get: d.GetAggregates,
		Key: func(aggregate *PostgresAggregate) string {
			return aggregate.Signature()
		},
		Create: func(aggregate *PostgresAggregate) []driversshared.Instruction {
			return []driversshared.Instruction{aggregate.CreateInstruction()}
		},
		Change: func(desired *PostgresAggregate, current *PostgresAggregate) []driversshared.Instruction {
			return desired.Diff(current)
		},
		Drop: func(current *PostgresAggregate) driversshared.Instruction {
			return current.DropInstruction()
		},
	})
}

func (d *PostgresDriver) GetAggregates(ctx context.Context, db *sql.DB) ([]*PostgresAggregate, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT
			p.proname,
			pg_get_function_identity_arguments(p.oid),
			format_type(a.aggtranstype, NULL),
			a.aggtransfn::regproc::text,
			a.aggtransspace,
			CASE WHEN a.aggfinalfn <> 0 THEN a.aggfinalfn::regproc::text END,
			a.aggfinalextra,
			CASE WHEN a.aggfinalmodify <> 'r' THEN a.aggfinalmodify::text ELSE '' END,
			CASE WHEN a.aggcombinefn <> 0 THEN a.aggcombinefn::regproc::text END,
			CASE WHEN a.aggserialfn <> 0 THEN a.aggserialfn::regproc::text END,
			CASE WHEN a.aggdeserialfn <> 0 THEN a.aggdeserialfn::regproc::text END,
			a.agginitval,
			CASE WHEN a.aggmtransfn <> 0 THEN a.aggmtransfn::regproc::text END,
			CASE WHEN a.aggminvtransfn <> 0 THEN a.aggminvtransfn::regproc::text END,
			CASE WHEN a.aggmtranstype <> 0 THEN format_type(a.aggmtranstype, NULL) END,
			a.aggmtransspace,
			CASE WHEN a.aggmfinalfn <> 0 THEN a.aggmfinalfn::regproc::text END,
			a.aggmfinalextra,
			CASE WHEN a.aggmfinalmodify <> 'r' THEN a.aggmfinalmodify::text ELSE '' END,
			a.aggminitval,
			CASE WHEN a.aggsortop <> 0 THEN (SELECT s.oprname FROM pg_operator s WHERE s.oid = a.aggsortop) END,
			CASE WHEN p.proparallel <> 'u' THEN p.proparallel::text ELSE '' END
		FROM pg_aggregate a
		JOIN pg_proc p ON p.oid = a.aggfnoid
		WHERE p.pronamespace = current_schema()::regnamespace
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e'
		)
		ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
	`, func(rows *sql.Rows) (*PostgresAggregate, error) {
		aggregate := &PostgresAggregate{}

		err := rows.Scan(
			&aggregate.Name,
			&aggregate.Arguments,
			&aggregate.StateType,
			&aggregate.TransitionFunction,
			&aggregate.TransitionSpace,
			&aggregate.FinalFunction,
			&aggregate.FinalFunctionExtra,
			&aggregate.FinalFunctionModify,
			&aggregate.CombineFunction,
			&aggregate.SerializeFunction,
			&aggregate.DeserializeFunction,
			&aggregate.InitialCondition,
			&aggregate.MovingTransitionFunction,
			&aggregate.MovingInverseTransitionFunction,
			&aggregate.MovingStateType,
			&aggregate.MovingTransitionSpace,
			&aggregate.MovingFinalFunction,
			&aggregate.MovingFinalFunctionExtra,
			&aggregate.MovingFinalFunctionModify,
			&aggregate.MovingInitialCondition,
			&aggregate.SortOperator,
			&aggregate.Parallel,
		)

		return aggregate, err
	})
}
