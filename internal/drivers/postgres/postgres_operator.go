package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type PostgresOperator struct {
	Name             string
	LeftArgument     sql.NullString
	RightArgument    sql.NullString
	Function         string
	Commutator       sql.NullString
	Negator          sql.NullString
	RestrictFunction sql.NullString
	JoinFunction     sql.NullString
	CanHash          bool
	CanMerge         bool
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
		Name:             o.Name,
		Function:         o.Function,
		LeftArgument:     o.LeftArgument,
		RightArgument:    o.RightArgument,
		Commutator:       o.Commutator,
		Negator:          o.Negator,
		RestrictFunction: o.RestrictFunction,
		JoinFunction:     o.JoinFunction,
		CanHash:          o.CanHash,
		CanMerge:         o.CanMerge,
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
func (o *PostgresOperator) Diff(other *PostgresOperator) []driversshared.Instruction {
	if o.Equal(other) {
		return nil
	}

	return []driversshared.Instruction{other.DropInstruction(), o.CreateInstruction()}
}

func (d *PostgresDriver) DiffOperators(ctx context.Context) (*SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresOperator]{
		Get: d.GetOperators,
		Key: func(operator *PostgresOperator) string {
			return operator.Signature()
		},
		Create: func(operator *PostgresOperator) []driversshared.Instruction {
			return []driversshared.Instruction{operator.CreateInstruction()}
		},
		Change: func(desired *PostgresOperator, current *PostgresOperator) []driversshared.Instruction {
			return desired.Diff(current)
		},
		Drop: func(current *PostgresOperator) driversshared.Instruction {
			return current.DropInstruction()
		},
	})
}

func (d *PostgresDriver) GetOperators(ctx context.Context, db *sql.DB) ([]*PostgresOperator, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT
			o.oprname,
			CASE WHEN o.oprleft <> 0 THEN format_type(o.oprleft, NULL) END,
			CASE WHEN o.oprright <> 0 THEN format_type(o.oprright, NULL) END,
			o.oprcode::regproc::text,
			CASE WHEN o.oprcom <> 0 THEN (SELECT c.oprname FROM pg_operator c WHERE c.oid = o.oprcom) END,
			CASE WHEN o.oprnegate <> 0 THEN (SELECT n.oprname FROM pg_operator n WHERE n.oid = o.oprnegate) END,
			CASE WHEN o.oprrest <> 0 THEN o.oprrest::regproc::text END,
			CASE WHEN o.oprjoin <> 0 THEN o.oprjoin::regproc::text END,
			o.oprcanhash,
			o.oprcanmerge
		FROM pg_operator o
		WHERE o.oprnamespace = current_schema()::regnamespace
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = o.oid AND d.deptype = 'e'
		)
		ORDER BY o.oprname, o.oprleft, o.oprright
	`, func(rows *sql.Rows) (*PostgresOperator, error) {
		operator := &PostgresOperator{}

		err := rows.Scan(
			&operator.Name,
			&operator.LeftArgument,
			&operator.RightArgument,
			&operator.Function,
			&operator.Commutator,
			&operator.Negator,
			&operator.RestrictFunction,
			&operator.JoinFunction,
			&operator.CanHash,
			&operator.CanMerge,
		)

		return operator, err
	})
}
