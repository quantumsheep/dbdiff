package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type PostgresFunction struct {
	Name             string
	Arguments        string
	ArgumentTypes    string
	ReturnType       string
	Kind             string
	Def              string
	Comment          string
	UsesTableRowType bool
}

func (f *PostgresFunction) CommentInstruction() *PostgresCommentOnRoutineInstruction {
	return &PostgresCommentOnRoutineInstruction{
		Name:      f.Name,
		Arguments: f.Arguments,
		Text:      f.Comment,
	}
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

func (d *PostgresDriver) DiffFunctions(ctx context.Context) (*SectionDiff, error) {
	return d.diffFunctions(ctx, false)
}

func (d *PostgresDriver) DiffTableRowTypeFunctions(ctx context.Context) (*SectionDiff, error) {
	return d.diffFunctions(ctx, true)
}

func (d *PostgresDriver) diffFunctions(ctx context.Context, usesTableRowType bool) (*SectionDiff, error) {
	var additions []driversshared.Instruction
	var removals []driversshared.Instruction

	desiredFunctions, err := d.GetFunctions(ctx, d.DesiredDatabaseConnection)
	if err != nil {
		return nil, err
	}

	currentFunctions, err := d.GetFunctions(ctx, d.CurrentDatabaseConnection)
	if err != nil {
		return nil, err
	}

	// A function and its match on the other side go into one section, so that the DROP
	// statement of a changed signature comes before the CREATE statement.
	tableRowTypeKeys := map[string]bool{}

	for _, function := range slices.Concat(desiredFunctions, currentFunctions) {
		if function.UsesTableRowType {
			tableRowTypeKeys[function.MatchKey()] = true
		}
	}

	inSection := func(function *PostgresFunction) bool {
		return tableRowTypeKeys[function.MatchKey()] == usesTableRowType
	}

	desiredFunctions = lo.Filter(desiredFunctions, func(function *PostgresFunction, _ int) bool {
		return inSection(function)
	})
	currentFunctions = lo.Filter(currentFunctions, func(function *PostgresFunction, _ int) bool {
		return inSection(function)
	})

	// The match key holds the argument types and no argument names, so a rename of a
	// parameter stays one function. CREATE OR REPLACE FUNCTION covers a new function and
	// a modified function. PostgreSQL refuses that statement when the return type or the
	// name of a parameter changes.
	for _, desiredFunction := range desiredFunctions {
		currentFunction, found := lo.Find(currentFunctions, func(function *PostgresFunction) bool {
			return function.MatchKey() == desiredFunction.MatchKey()
		})
		if !found {
			additions = append(additions, desiredFunction.CreateInstruction())

			if desiredFunction.Comment != "" {
				additions = append(additions, desiredFunction.CommentInstruction())
			}

			continue
		}

		// CREATE OR REPLACE keeps the comment, and only a DROP statement removes it.
		dropped := false

		if desiredFunction.Def != currentFunction.Def {
			if desiredFunction.ReturnType != currentFunction.ReturnType ||
				desiredFunction.Arguments != currentFunction.Arguments {
				additions = append(additions, currentFunction.DropInstruction())
				dropped = true
			}

			additions = append(additions, desiredFunction.CreateInstruction())
		}

		if desiredFunction.Comment != currentFunction.Comment ||
			(dropped && desiredFunction.Comment != "") {
			additions = append(additions, desiredFunction.CommentInstruction())
		}
	}

	for _, currentFunction := range currentFunctions {
		_, found := lo.Find(desiredFunctions, func(function *PostgresFunction) bool {
			return function.MatchKey() == currentFunction.MatchKey()
		})
		if !found {
			removals = append(removals, currentFunction.DropInstruction())
		}
	}

	return &SectionDiff{
		Additions: additions,
		Removals:  removals,
	}, nil
}

func (d *PostgresDriver) GetFunctions(ctx context.Context, db *sql.DB) ([]*PostgresFunction, error) {
	// The regexp_replace call removes the schema prefix of the header. The desired schema
	// and the current schema hold a different name, and the diff compares the two texts.
	// pg_get_function_result gives NULL for a procedure.
	return driversshared.QueryAll(ctx, db, `
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
			coalesce(obj_description(p.oid, 'pg_proc'), ''),
			EXISTS (
				SELECT 1
				FROM pg_type t
				LEFT JOIN pg_type e ON e.oid = t.typelem
				JOIN pg_class c ON c.oid IN (t.typrelid, e.typrelid)
				WHERE t.oid = ANY (array_append(coalesce(p.proallargtypes, p.proargtypes::oid[]), p.prorettype))
				AND c.relkind IN ('r', 'p', 'f')
			)
		FROM pg_proc p
		WHERE p.pronamespace = current_schema()::regnamespace
		AND p.prokind IN ('f', 'p')
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e'
		)
		ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
	`, func(rows *sql.Rows) (*PostgresFunction, error) {
		function := &PostgresFunction{}

		err := rows.Scan(&function.Name, &function.Arguments, &function.ArgumentTypes,
			&function.ReturnType, &function.Kind, &function.Def, &function.Comment, &function.UsesTableRowType)
		if err != nil {
			return nil, err
		}

		function.Def = strings.TrimSpace(function.Def)

		return function, nil
	})
}
