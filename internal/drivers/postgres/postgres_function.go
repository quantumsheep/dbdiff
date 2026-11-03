package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type PostgresFunction struct {
	Name          string
	Arguments     string
	ArgumentTypes string
	ReturnType    string
	Kind          string
	Def           string
	Comment       string
}

func (f *PostgresFunction) CommentInstruction() *PostgresCommentOnRoutineInstruction {
	return &PostgresCommentOnRoutineInstruction{
		Name:      f.Name,
		Arguments: f.Arguments,
		Text:      f.Comment,
	}
}

func (f *PostgresFunction) Signature() string {
	return fmt.Sprintf("%s(%s)", f.Name, f.Arguments)
}

func (f *PostgresFunction) MatchKey() string {
	return fmt.Sprintf("%s %s(%s)", f.Kind, f.Name, f.ArgumentTypes)
}

func (f *PostgresFunction) CreateInstruction() driversshared.Instruction {
	if f.Kind == "PROCEDURE" {
		return &PostgresCreateProcedureInstruction{
			Definition: f.Def,
		}
	}

	return &PostgresCreateFunctionInstruction{
		Definition: f.Def,
	}
}

func (f *PostgresFunction) DropInstruction() driversshared.Instruction {
	if f.Kind == "PROCEDURE" {
		return &PostgresDropProcedureInstruction{
			Name:      f.Name,
			Arguments: f.Arguments,
		}
	}

	return &PostgresDropFunctionInstruction{
		Name:      f.Name,
		Arguments: f.Arguments,
	}
}

func (d *PostgresDriver) DiffFunctions(ctx context.Context) (*driversshared.SectionDiff, error) {
	var additions []driversshared.Instruction
	var removals []driversshared.Instruction

	targetFunctions, err := d.GetFunctions(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceFunctions, err := d.GetFunctions(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	// The match key holds the argument types and no argument names, so a rename of a
	// parameter stays one function. CREATE OR REPLACE FUNCTION covers a new function and
	// a modified function. PostgreSQL refuses that statement when the return type or the
	// name of a parameter changes.
	for _, targetFunction := range targetFunctions {
		sourceFunction, found := lo.Find(sourceFunctions, func(function *PostgresFunction) bool {
			return function.MatchKey() == targetFunction.MatchKey()
		})
		if !found {
			additions = append(additions, targetFunction.CreateInstruction())

			if targetFunction.Comment != "" {
				additions = append(additions, targetFunction.CommentInstruction())
			}

			continue
		}

		// CREATE OR REPLACE keeps the comment, and only a DROP statement removes it.
		dropped := false

		if targetFunction.Def != sourceFunction.Def {
			if targetFunction.ReturnType != sourceFunction.ReturnType ||
				targetFunction.Arguments != sourceFunction.Arguments {
				additions = append(additions, sourceFunction.DropInstruction())
				dropped = true
			}

			additions = append(additions, targetFunction.CreateInstruction())
		}

		if targetFunction.Comment != sourceFunction.Comment ||
			(dropped && targetFunction.Comment != "") {
			additions = append(additions, targetFunction.CommentInstruction())
		}
	}

	for _, sourceFunction := range sourceFunctions {
		_, found := lo.Find(targetFunctions, func(function *PostgresFunction) bool {
			return function.MatchKey() == sourceFunction.MatchKey()
		})
		if !found {
			removals = append(removals, sourceFunction.DropInstruction())
		}
	}

	return &driversshared.SectionDiff{
		Additions: additions,
		Removals:  removals,
	}, nil
}

func (d *PostgresDriver) GetFunctions(ctx context.Context, db *sql.DB) ([]*PostgresFunction, error) {
	// The regexp_replace call removes the schema prefix of the header. The target schema
	// and the source schema hold a different name, and the diff compares the two texts.
	// pg_get_function_result gives NULL for a procedure.
	rows, err := db.QueryContext(ctx, `
		SELECT
			p.proname,
			pg_get_function_identity_arguments(p.oid),
			oidvectortypes(p.proargtypes),
			coalesce(pg_get_function_result(p.oid), ''),
			CASE p.prokind WHEN 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END,
			regexp_replace(
				pg_get_functiondef(p.oid),
				'^CREATE OR REPLACE (FUNCTION|PROCEDURE) ' || quote_ident(current_schema()) || '\.',
				'CREATE OR REPLACE \1 '
			),
			coalesce(obj_description(p.oid, 'pg_proc'), '')
		FROM pg_proc p
		WHERE p.pronamespace = current_schema()::regnamespace
		AND p.prokind IN ('f', 'p')
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e'
		)
		ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var functions []*PostgresFunction

	for rows.Next() {
		function := &PostgresFunction{}

		err := rows.Scan(&function.Name, &function.Arguments, &function.ArgumentTypes,
			&function.ReturnType, &function.Kind, &function.Def, &function.Comment)
		if err != nil {
			return nil, err
		}

		function.Def = strings.TrimSpace(function.Def)

		functions = append(functions, function)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return functions, nil
}
