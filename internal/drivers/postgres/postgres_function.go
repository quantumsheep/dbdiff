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
	Name          string
	Arguments     string
	ArgumentTypes string
	ReturnType    string
	Kind          string
	Def           string
	Comment       string
	Section       functionSection

	CalledByARelation bool
}

// A function that names the row type of a relation runs after the section that creates
// that relation.
type functionSection int

const (
	plainFunctionSection functionSection = iota
	tableRowTypeFunctionSection
	viewRowTypeFunctionSection
)

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
	return d.diffFunctions(ctx, plainFunctionSection)
}

func (d *PostgresDriver) DiffTableRowTypeFunctions(ctx context.Context) (*SectionDiff, error) {
	return d.diffFunctions(ctx, tableRowTypeFunctionSection)
}

func (d *PostgresDriver) DiffViewRowTypeFunctions(ctx context.Context) (*SectionDiff, error) {
	return d.diffFunctions(ctx, viewRowTypeFunctionSection)
}

func (d *PostgresDriver) diffFunctions(ctx context.Context, section functionSection) (*SectionDiff, error) {
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
	sectionByKey := map[string]functionSection{}

	for _, function := range slices.Concat(desiredFunctions, currentFunctions) {
		sectionByKey[function.MatchKey()] = max(sectionByKey[function.MatchKey()], function.Section)
	}

	inSection := func(function *PostgresFunction) bool {
		return sectionByKey[function.MatchKey()] == section
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
	currentFunctionsByKey := driversshared.FirstByKey(currentFunctions, (*PostgresFunction).MatchKey)
	desiredFunctionsByKey := driversshared.FirstByKey(desiredFunctions, (*PostgresFunction).MatchKey)

	for _, desiredFunction := range desiredFunctions {
		currentFunction, found := currentFunctionsByKey[desiredFunction.MatchKey()]
		if !found {
			additions = append(additions, desiredFunction.CreateInstruction())

			if desiredFunction.Comment != "" {
				additions = append(additions, desiredFunction.CommentInstruction())
			}

			continue
		}

		// CREATE OR REPLACE keeps the comment, and only a DROP statement removes it.
		dropped := d.droppedFunctionKeys[currentFunction.MatchKey()]

		if !dropped && desiredFunction.Def != currentFunction.Def &&
			(desiredFunction.ReturnType != currentFunction.ReturnType ||
				desiredFunction.Arguments != currentFunction.Arguments) {
			dropInstructions, err := d.dropFunction(ctx, currentFunction)
			if err != nil {
				return nil, err
			}

			additions = append(additions, dropInstructions...)
			dropped = true
		}

		if dropped || desiredFunction.Def != currentFunction.Def {
			additions = append(additions, desiredFunction.CreateInstruction())
		}

		if desiredFunction.Comment != currentFunction.Comment ||
			(dropped && desiredFunction.Comment != "") {
			additions = append(additions, desiredFunction.CommentInstruction())
		}
	}

	var earlyRemovals []driversshared.Instruction

	for _, currentFunction := range currentFunctions {
		_, found := desiredFunctionsByKey[currentFunction.MatchKey()]
		if found || d.droppedFunctionKeys[currentFunction.MatchKey()] {
			continue
		}

		// The views section drops a view before every addition, and PostgreSQL refuses that
		// drop while a function names the row type of the view. A view that calls the function
		// must go first, so that function keeps the late drop.
		if section == viewRowTypeFunctionSection && !currentFunction.CalledByARelation {
			earlyRemovals = append(earlyRemovals, currentFunction.DropInstruction())

			continue
		}

		removals = append(removals, currentFunction.DropInstruction())
	}

	return &SectionDiff{
		EarlyRemovals: earlyRemovals,
		Additions:     additions,
		Removals:      removals,
	}, nil
}

// diffFunctions creates each dropped function again. If an earlier section dropped the
// function, the result is empty.
func (d *PostgresDriver) dropFunction(
	ctx context.Context,
	function *PostgresFunction,
) ([]driversshared.Instruction, error) {
	if d.droppedFunctionKeys[function.MatchKey()] {
		return nil, nil
	}

	instructions, err := d.dropFunctionDependents(ctx, function)
	if err != nil {
		return nil, err
	}

	if d.droppedFunctionKeys == nil {
		d.droppedFunctionKeys = map[string]bool{}
	}

	d.droppedFunctionKeys[function.MatchKey()] = true

	return append(instructions, function.DropInstruction()), nil
}

// PostgreSQL refuses to drop a function while another object uses it. The returned instructions
// drop each default, check constraint, index, and policy that uses the function, and DiffTables
// creates them again. DiffViewsAndMaterializedViews drops and creates again each relation that
// this function marks. The kind blocker names an object that the diff cannot drop and create
// again. A table object of a function that uses a row type is a blocker, because
// DiffTables runs before the section of that function. A parent statement reaches an inherited
// check constraint, but a child table that also declares the constraint keeps it, so that
// constraint is a blocker.
func (d *PostgresDriver) dropFunctionDependents(
	ctx context.Context,
	function *PostgresFunction,
) ([]driversshared.Instruction, error) {
	dependents, err := driversshared.QueryAll(ctx, d.CurrentDatabaseConnection, `
		SELECT DISTINCT
			kind,
			table_name,
			column_name,
			CASE
				WHEN kind = 'relation' THEN table_name
				WHEN kind IN ('constraint', 'index', 'policy') THEN object_name
				ELSE description
			END AS name
		FROM (
			SELECT
				CASE
					WHEN rule_class.relkind IN ('v', 'm') AND r.rulename = '_RETURN'
						AND rule_class.relnamespace = quote_ident(current_schema())::regnamespace THEN 'relation'
					WHEN table_class.relkind = 'm'
						AND table_class.relnamespace = quote_ident(current_schema())::regnamespace THEN 'relation'
					WHEN $3 OR table_class.relnamespace <> quote_ident(current_schema())::regnamespace THEN 'blocker'
					WHEN column_default.oid IS NOT NULL AND default_attribute.attgenerated = '' THEN 'default'
					WHEN table_constraint.contype = 'c' AND table_constraint.coninhcount = 0 THEN 'constraint'
					WHEN table_constraint.contype = 'c' AND NOT table_constraint.conislocal THEN NULL
					WHEN table_index.indexrelid IS NOT NULL AND NOT EXISTS (
						SELECT 1 FROM pg_constraint index_constraint WHERE index_constraint.conindid = table_index.indexrelid
					) THEN
						CASE WHEN NOT index_class.relispartition THEN 'index' END
					WHEN table_policy.oid IS NOT NULL THEN 'policy'
					ELSE 'blocker'
				END AS kind,
				coalesce(rule_class.relname, table_class.relname, '') AS table_name,
				coalesce(default_attribute.attname, '') AS column_name,
				coalesce(table_constraint.conname, index_class.relname, table_policy.polname) AS object_name,
				pg_describe_object(d.classid, d.objid, d.objsubid) AS description
			FROM pg_depend d
			JOIN pg_proc p ON p.oid = d.refobjid
			LEFT JOIN pg_rewrite r ON d.classid = 'pg_rewrite'::regclass AND r.oid = d.objid
			LEFT JOIN pg_class rule_class ON rule_class.oid = r.ev_class
			LEFT JOIN pg_attrdef column_default ON d.classid = 'pg_attrdef'::regclass
				AND column_default.oid = d.objid
			LEFT JOIN pg_attribute default_attribute ON default_attribute.attrelid = column_default.adrelid
				AND default_attribute.attnum = column_default.adnum
			LEFT JOIN pg_constraint table_constraint ON d.classid = 'pg_constraint'::regclass
				AND table_constraint.oid = d.objid
				AND table_constraint.conrelid <> 0
			LEFT JOIN pg_index table_index ON d.classid = 'pg_class'::regclass
				AND table_index.indexrelid = d.objid
			LEFT JOIN pg_class index_class ON index_class.oid = table_index.indexrelid
			LEFT JOIN pg_policy table_policy ON d.classid = 'pg_policy'::regclass
				AND table_policy.oid = d.objid
			LEFT JOIN pg_class table_class ON table_class.oid = coalesce(
				column_default.adrelid, table_constraint.conrelid, table_index.indrelid, table_policy.polrelid)
			WHERE d.refclassid = 'pg_proc'::regclass
			AND d.deptype IN ('n', 'a')
			AND p.pronamespace = quote_ident(current_schema())::regnamespace
			AND p.proname = $1
			AND oidvectortypes(p.proargtypes) = $2
		) dependents
		WHERE kind IS NOT NULL
		ORDER BY kind, table_name, column_name, name
	`, func(rows *sql.Rows) (*PostgresColumnDependent, error) {
		dependent := &PostgresColumnDependent{}

		err := rows.Scan(&dependent.Kind, &dependent.TableName, &dependent.ColumnName, &dependent.Name)

		return dependent, err
	}, function.Name, function.ArgumentTypes, function.Section != plainFunctionSection)
	if err != nil {
		return nil, err
	}

	var instructions []driversshared.Instruction

	for _, dependent := range dependents {
		switch dependent.Kind {
		case "blocker":
			return nil, fmt.Errorf("the %s uses the function %q. dbdiff must drop and create this function again. %s",
				dependent.Name, function.Name, blockerAdvice)
		case "relation":
			d.markRecreated(dependent.TableName)
		case "default":
			instructions = append(instructions, d.dropColumnDefault(dependent.TableName, dependent.ColumnName)...)
		case "constraint":
			instructions = append(instructions, &PostgresAlterTableInstruction{
				Name: dependent.TableName,
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: dependent.Name,
					},
				},
			})
		case "index":
			instructions = append(instructions, &driversshared.SQLDropIndexInstruction{
				Name: dependent.Name,
			})
		case "policy":
			instructions = append(instructions, &PostgresDropPolicyInstruction{
				Name:      dependent.Name,
				TableName: dependent.TableName,
			})
		}

		if dependent.Kind != "relation" && dependent.Kind != "default" {
			if d.droppedTableDependents == nil {
				d.droppedTableDependents = map[string][]*PostgresColumnDependent{}
			}

			d.droppedTableDependents[dependent.TableName] = append(
				d.droppedTableDependents[dependent.TableName], dependent)
		}
	}

	return instructions, nil
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
				'^CREATE OR REPLACE (FUNCTION|PROCEDURE) ' || `+currentSchemaPrefixPattern+`,
				'CREATE OR REPLACE \1 '
			),
			coalesce(obj_description(p.oid, 'pg_proc'), ''),
			coalesce((
				SELECT max(CASE WHEN c.relkind IN ('v', 'm') THEN 2 ELSE 1 END)
				FROM pg_type t
				LEFT JOIN pg_type e ON e.oid = t.typelem
				JOIN pg_class c ON c.oid IN (t.typrelid, e.typrelid)
				WHERE t.oid = ANY (array_append(coalesce(p.proallargtypes, p.proargtypes::oid[]), p.prorettype))
				AND c.relkind IN ('r', 'p', 'f', 'v', 'm')
			), 0),
			EXISTS (
				SELECT 1
				FROM pg_depend d
				JOIN pg_rewrite r ON r.oid = d.objid
				WHERE d.classid = 'pg_rewrite'::regclass
				AND d.refclassid = 'pg_proc'::regclass
				AND d.refobjid = p.oid
			)
		FROM pg_proc p
		WHERE p.pronamespace = quote_ident(current_schema())::regnamespace
		AND p.prokind IN ('f', 'p')
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e'
		)
		ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
	`, func(rows *sql.Rows) (*PostgresFunction, error) {
		function := &PostgresFunction{}

		err := rows.Scan(&function.Name, &function.Arguments, &function.ArgumentTypes,
			&function.ReturnType, &function.Kind, &function.Def, &function.Comment, &function.Section, &function.CalledByARelation)
		if err != nil {
			return nil, err
		}

		function.Def = strings.TrimSpace(function.Def)

		return function, nil
	})
}
