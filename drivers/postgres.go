package drivers

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/samber/lo"
)

type PostgresDriverConfig struct {
	SourceConnectionString string
	TargetConnectionString string
	SourceSchema           string
	TargetSchema           string
	CompareData            bool
}

type PostgresDriver struct {
	SourceDatabaseConnection *sql.DB
	TargetDatabaseConnection *sql.DB
	SourceSchema             string
	TargetSchema             string
	CompareData              bool
}

func NewPostgresDriver(config *PostgresDriverConfig) (*PostgresDriver, error) {
	sourceDatabaseConnection, err := openPostgresConnection(config.SourceConnectionString, config.SourceSchema)
	if err != nil {
		return nil, err
	}

	targetDatabaseConnection, err := openPostgresConnection(config.TargetConnectionString, config.TargetSchema)
	if err != nil {
		return nil, err
	}

	driver := &PostgresDriver{
		SourceDatabaseConnection: sourceDatabaseConnection,
		TargetDatabaseConnection: targetDatabaseConnection,
		SourceSchema:             config.SourceSchema,
		TargetSchema:             config.TargetSchema,
		CompareData:              config.CompareData,
	}

	return driver, nil
}

func openPostgresConnection(connectionString string, schema string) (*sql.DB, error) {
	if schema == "" {
		return sql.Open("pgx", connectionString)
	}

	connectionConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}

	connectionConfig.RuntimeParams["search_path"] = quoteIdentifier(schema)

	return stdlib.OpenDB(*connectionConfig), nil
}

// PostgreSQL accepts a search path that names no schema, and each query then reads an
// empty schema. Without this check the diff drops every object of the target.
func (d *PostgresDriver) VerifySchema(ctx context.Context, db *sql.DB, schema string, role string) error {
	if schema == "" {
		return nil
	}

	row := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, schema)

	var found bool

	err := row.Scan(&found)
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("the %s database has no schema with the name %q", role, schema)
	}

	return nil
}

func (d *PostgresDriver) Close() error {
	var err error

	err = d.SourceDatabaseConnection.Close()
	if err != nil {
		return err
	}

	err = d.TargetDatabaseConnection.Close()
	if err != nil {
		return err
	}

	return nil
}

func (d *PostgresDriver) Diff(ctx context.Context) ([]Instruction, error) {
	err := d.VerifySchema(ctx, d.SourceDatabaseConnection, d.SourceSchema, "source")
	if err != nil {
		return nil, err
	}

	err = d.VerifySchema(ctx, d.TargetDatabaseConnection, d.TargetSchema, "target")
	if err != nil {
		return nil, err
	}

	// A table uses an extension, a type, a domain, a composite type, a sequence, or a
	// function. An aggregate and an operator use a function. Keep this order.
	sections := []func(ctx context.Context) (*SectionDiff, error){
		d.DiffExtensions,
		d.DiffTypes,
		d.DiffDomains,
		d.DiffCompositeTypes,
		d.DiffSequences,
		d.DiffFunctions,
		d.DiffAggregates,
		d.DiffOperators,
		d.DiffTables,
		d.DiffViews,
	}

	sectionDiffs := make([]*SectionDiff, 0, len(sections))

	for _, section := range sections {
		sectionDiff, err := section(ctx)
		if err != nil {
			return nil, err
		}

		sectionDiffs = append(sectionDiffs, sectionDiff)
	}

	var instructions []Instruction

	// PostgreSQL refuses a DROP statement while another object uses the object, so every
	// removal takes the reverse section order. An early removal runs before every addition,
	// because a view blocks a change of the column that it reads.
	for _, sectionDiff := range slices.Backward(sectionDiffs) {
		instructions = append(instructions, sectionDiff.EarlyRemovals...)
	}

	for _, sectionDiff := range sectionDiffs {
		instructions = append(instructions, sectionDiff.Additions...)
	}

	for _, sectionDiff := range slices.Backward(sectionDiffs) {
		instructions = append(instructions, sectionDiff.Removals...)
	}

	if d.CompareData {
		dataInstructions, err := d.DiffData(ctx)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, dataInstructions...)
	}

	return instructions, nil
}

// sectionRules holds the parts that differ between two kinds of schema object.
type sectionRules[T any] struct {
	Get    func(ctx context.Context, db *sql.DB) ([]T, error)
	Key    func(object T) string
	Create func(source T) Instruction
	Change func(source T, target T) []Instruction
	Drop   func(target T) Instruction
}

// diffSection compares the objects of one kind. It creates an object that the target does
// not hold, it changes an object that differs, and it drops an object that the source does
// not hold.
func diffSection[T any](ctx context.Context, driver *PostgresDriver, rules sectionRules[T]) (*SectionDiff, error) {
	var additions []Instruction
	var removals []Instruction

	sourceObjects, err := rules.Get(ctx, driver.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetObjects, err := rules.Get(ctx, driver.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceObject := range sourceObjects {
		targetObject, found := lo.Find(targetObjects, func(object T) bool {
			return rules.Key(object) == rules.Key(sourceObject)
		})
		if !found {
			additions = append(additions, rules.Create(sourceObject))
			continue
		}

		additions = append(additions, rules.Change(sourceObject, targetObject)...)
	}

	for _, targetObject := range targetObjects {
		_, found := lo.Find(sourceObjects, func(object T) bool {
			return rules.Key(object) == rules.Key(targetObject)
		})
		if !found {
			removals = append(removals, rules.Drop(targetObject))
		}
	}

	return &SectionDiff{
		Additions: additions,
		Removals:  removals,
	}, nil
}

func (d *PostgresDriver) DiffExtensions(ctx context.Context) (*SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresExtension]{
		Get: d.GetExtensions,
		Key: func(extension *PostgresExtension) string {
			return extension.Name
		},
		Create: func(extension *PostgresExtension) Instruction {
			return extension.CreateInstruction()
		},
		Change: func(source *PostgresExtension, target *PostgresExtension) []Instruction {
			if source.Version == target.Version {
				return nil
			}

			return []Instruction{source.UpdateInstruction()}
		},
		Drop: func(target *PostgresExtension) Instruction {
			return target.DropInstruction()
		},
	})
}

func (d *PostgresDriver) DiffTypes(ctx context.Context) (*SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresType]{
		Get: d.GetTypes,
		Key: func(enumType *PostgresType) string {
			return enumType.Name
		},
		Create: func(enumType *PostgresType) Instruction {
			return enumType.CreateInstruction()
		},
		Change: func(source *PostgresType, target *PostgresType) []Instruction {
			return source.Diff(target)
		},
		Drop: func(target *PostgresType) Instruction {
			return &PostgresDropTypeInstruction{Name: target.Name}
		},
	})
}

func (d *PostgresDriver) DiffDomains(ctx context.Context) (*SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresDomain]{
		Get: d.GetDomains,
		Key: func(domain *PostgresDomain) string {
			return domain.Name
		},
		Create: func(domain *PostgresDomain) Instruction {
			return domain.CreateInstruction()
		},
		Change: func(source *PostgresDomain, target *PostgresDomain) []Instruction {
			return source.Diff(target)
		},
		Drop: func(target *PostgresDomain) Instruction {
			return target.DropInstruction()
		},
	})
}

func (d *PostgresDriver) DiffCompositeTypes(ctx context.Context) (*SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresCompositeType]{
		Get: d.GetCompositeTypes,
		Key: func(compositeType *PostgresCompositeType) string {
			return compositeType.Name
		},
		Create: func(compositeType *PostgresCompositeType) Instruction {
			return compositeType.CreateInstruction()
		},
		Change: func(source *PostgresCompositeType, target *PostgresCompositeType) []Instruction {
			return source.Diff(target)
		},
		Drop: func(target *PostgresCompositeType) Instruction {
			return target.DropInstruction()
		},
	})
}

func (d *PostgresDriver) DiffAggregates(ctx context.Context) (*SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresAggregate]{
		Get: d.GetAggregates,
		Key: func(aggregate *PostgresAggregate) string {
			return aggregate.Signature()
		},
		Create: func(aggregate *PostgresAggregate) Instruction {
			return aggregate.CreateInstruction()
		},
		Change: func(source *PostgresAggregate, target *PostgresAggregate) []Instruction {
			return source.Diff(target)
		},
		Drop: func(target *PostgresAggregate) Instruction {
			return target.DropInstruction()
		},
	})
}

func (d *PostgresDriver) DiffOperators(ctx context.Context) (*SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresOperator]{
		Get: d.GetOperators,
		Key: func(operator *PostgresOperator) string {
			return operator.Signature()
		},
		Create: func(operator *PostgresOperator) Instruction {
			return operator.CreateInstruction()
		},
		Change: func(source *PostgresOperator, target *PostgresOperator) []Instruction {
			return source.Diff(target)
		},
		Drop: func(target *PostgresOperator) Instruction {
			return target.DropInstruction()
		},
	})
}

func (d *PostgresDriver) DiffSequences(ctx context.Context) (*SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresSequence]{
		Get: d.GetSequences,
		Key: func(sequence *PostgresSequence) string {
			return sequence.Name
		},
		Create: func(sequence *PostgresSequence) Instruction {
			return sequence.CreateInstruction()
		},
		Change: func(source *PostgresSequence, target *PostgresSequence) []Instruction {
			return source.Diff(target)
		},
		Drop: func(target *PostgresSequence) Instruction {
			return &PostgresDropSequenceInstruction{Name: target.Name}
		},
	})
}

func (d *PostgresDriver) DiffFunctions(ctx context.Context) (*SectionDiff, error) {
	var additions []Instruction
	var removals []Instruction

	sourceFunctions, err := d.GetFunctions(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetFunctions, err := d.GetFunctions(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	// The definition starts with CREATE OR REPLACE FUNCTION, so a new function and a
	// modified function take the same statement. PostgreSQL refuses that statement when the
	// return type changes.
	for _, sourceFunction := range sourceFunctions {
		targetFunction, found := lo.Find(targetFunctions, func(function *PostgresFunction) bool {
			return function.Signature() == sourceFunction.Signature()
		})
		if !found {
			additions = append(additions, sourceFunction.CreateInstruction())
			continue
		}

		if sourceFunction.Def != targetFunction.Def {
			if sourceFunction.ReturnType != targetFunction.ReturnType {
				additions = append(additions, targetFunction.DropInstruction())
			}

			additions = append(additions, sourceFunction.CreateInstruction())
		}
	}

	for _, targetFunction := range targetFunctions {
		_, found := lo.Find(sourceFunctions, func(function *PostgresFunction) bool {
			return function.Signature() == targetFunction.Signature()
		})
		if !found {
			removals = append(removals, targetFunction.DropInstruction())
		}
	}

	return &SectionDiff{
		Additions: additions,
		Removals:  removals,
	}, nil
}

func (d *PostgresDriver) DiffTables(ctx context.Context) (*SectionDiff, error) {
	var additions []Instruction
	var removals []Instruction

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	hasAutomaticCast := func(oldType string, newType string) (bool, error) {
		return d.HasAutomaticCast(ctx, d.TargetDatabaseConnection, oldType, newType)
	}

	for _, sourceTable := range sourceTables {
		targetTable, found := lo.Find(targetTables, func(table *PostgresTable) bool {
			return table.Name == sourceTable.Name
		})
		if !found {
			additions = append(additions, sourceTable.Instructions()...)
			continue
		}

		subInstructions, err := sourceTable.DiffTable(targetTable, hasAutomaticCast)
		if err != nil {
			return nil, err
		}

		additions = append(additions, subInstructions...)
	}

	for _, targetTable := range targetTables {
		_, found := lo.Find(sourceTables, func(table *PostgresTable) bool {
			return table.Name == targetTable.Name
		})
		if !found {
			removals = append(removals, &SQLDropTableInstruction{Name: targetTable.Name})
		}
	}

	return &SectionDiff{
		Additions: additions,
		Removals:  removals,
	}, nil
}

// DiffViews writes every DROP VIEW statement into the early removals. PostgreSQL refuses a
// change of a column while a view that reads it exists.
func (d *PostgresDriver) DiffViews(ctx context.Context) (*SectionDiff, error) {
	var earlyRemovals []Instruction
	var additions []Instruction

	sourceViews, err := d.GetViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetViews, err := d.GetViews(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	// sourceViews holds a view after every view that it reads, so a forward walk creates
	// each view after the views that it depends on.
	for _, sourceView := range sourceViews {
		targetView, found := lo.Find(targetViews, func(view *PostgresView) bool {
			return view.Name == sourceView.Name
		})
		if !found {
			additions = append(additions, sourceView.CreateInstruction())
			continue
		}

		// A type change of a column that the view reads keeps the definition text equal,
		// so the columns give the second condition.
		if sourceView.Def != targetView.Def || !sourceView.HasEqualColumns(targetView) {
			additions = append(additions, sourceView.CreateInstruction())
		}
	}

	// PostgreSQL refuses a DROP VIEW statement while another view still reads the view, so
	// a backward walk drops each dependent view first.
	for _, targetView := range slices.Backward(targetViews) {
		sourceView, found := lo.Find(sourceViews, func(view *PostgresView) bool {
			return view.Name == targetView.Name
		})
		if !found {
			earlyRemovals = append(earlyRemovals, &SQLDropViewInstruction{Name: targetView.Name})
			continue
		}

		if sourceView.Def != targetView.Def || !sourceView.HasEqualColumns(targetView) {
			earlyRemovals = append(earlyRemovals, &SQLDropViewInstruction{Name: targetView.Name})
		}
	}

	return &SectionDiff{
		EarlyRemovals: earlyRemovals,
		Additions:     additions,
	}, nil
}

func (d *PostgresDriver) GetExtensions(ctx context.Context, db *sql.DB) ([]*PostgresExtension, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT extname, extversion
		FROM pg_extension
		WHERE extnamespace = current_schema()::regnamespace
		ORDER BY extname
	`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var extensions []*PostgresExtension

	for rows.Next() {
		extension := &PostgresExtension{}

		err := rows.Scan(&extension.Name, &extension.Version)
		if err != nil {
			return nil, err
		}

		extensions = append(extensions, extension)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return extensions, nil
}

func (d *PostgresDriver) GetTypes(ctx context.Context, db *sql.DB) ([]*PostgresType, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT t.typname, e.enumlabel
		FROM pg_type t
		JOIN pg_enum e ON e.enumtypid = t.oid
		WHERE t.typnamespace = current_schema()::regnamespace
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = t.oid AND d.deptype = 'e'
		)
		ORDER BY t.typname, e.enumsortorder
	`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var types []*PostgresType

	for rows.Next() {
		var typeName, value string

		err := rows.Scan(&typeName, &value)
		if err != nil {
			return nil, err
		}

		lastType, _ := lo.Last(types)
		if lastType == nil || lastType.Name != typeName {
			lastType = &PostgresType{Name: typeName}
			types = append(types, lastType)
		}

		lastType.Values = append(lastType.Values, value)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return types, nil
}

func (d *PostgresDriver) GetDomains(ctx context.Context, db *sql.DB) ([]*PostgresDomain, error) {
	// PostgreSQL 17 keeps the NOT NULL flag of a domain in pg_constraint too. This query
	// reads the check constraints only, because typnotnull holds that flag already.
	rows, err := db.QueryContext(ctx, `
		SELECT
			t.typname,
			format_type(t.typbasetype, t.typtypmod),
			t.typnotnull,
			t.typdefault,
			c.conname,
			pg_get_constraintdef(c.oid)
		FROM pg_type t
		LEFT JOIN pg_constraint c ON c.contypid = t.oid AND c.contype = 'c'
		WHERE t.typnamespace = current_schema()::regnamespace
		AND t.typtype = 'd'
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = t.oid AND d.deptype = 'e'
		)
		ORDER BY t.typname, c.conname
	`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var domains []*PostgresDomain

	for rows.Next() {
		var domainName, baseType string
		var notNull bool
		var defaultValue, constraintName, constraintDef sql.NullString

		err := rows.Scan(&domainName, &baseType, &notNull, &defaultValue, &constraintName, &constraintDef)
		if err != nil {
			return nil, err
		}

		lastDomain, _ := lo.Last(domains)
		if lastDomain == nil || lastDomain.Name != domainName {
			lastDomain = &PostgresDomain{
				Name:     domainName,
				BaseType: baseType,
				NotNull:  notNull,
				Default:  defaultValue,
			}
			domains = append(domains, lastDomain)
		}

		if constraintName.Valid {
			constraint := &PostgresDomainConstraint{
				Name: constraintName.String,
				Def:  constraintDef.String,
			}
			lastDomain.Constraints = append(lastDomain.Constraints, constraint)
		}
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return domains, nil
}

func (d *PostgresDriver) GetCompositeTypes(ctx context.Context, db *sql.DB) ([]*PostgresCompositeType, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT t.typname, a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_type t
		JOIN pg_class c ON c.oid = t.typrelid AND c.relkind = 'c'
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
		WHERE t.typnamespace = current_schema()::regnamespace
		AND t.typtype = 'c'
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = t.oid AND d.deptype = 'e'
		)
		ORDER BY t.typname, a.attnum
	`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var compositeTypes []*PostgresCompositeType

	for rows.Next() {
		var typeName, attributeName, attributeType string

		err := rows.Scan(&typeName, &attributeName, &attributeType)
		if err != nil {
			return nil, err
		}

		lastCompositeType, _ := lo.Last(compositeTypes)
		if lastCompositeType == nil || lastCompositeType.Name != typeName {
			lastCompositeType = &PostgresCompositeType{Name: typeName}
			compositeTypes = append(compositeTypes, lastCompositeType)
		}

		attribute := &PostgresCompositeTypeAttribute{
			Name: attributeName,
			Type: attributeType,
		}
		lastCompositeType.Attributes = append(lastCompositeType.Attributes, attribute)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return compositeTypes, nil
}

func (d *PostgresDriver) GetAggregates(ctx context.Context, db *sql.DB) ([]*PostgresAggregate, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			p.proname,
			pg_get_function_identity_arguments(p.oid),
			format_type(a.aggtranstype, NULL),
			a.aggtransfn::regproc::text,
			CASE WHEN a.aggfinalfn <> 0 THEN a.aggfinalfn::regproc::text END,
			a.agginitval
		FROM pg_aggregate a
		JOIN pg_proc p ON p.oid = a.aggfnoid
		WHERE p.pronamespace = current_schema()::regnamespace
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e'
		)
		ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
	`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var aggregates []*PostgresAggregate

	for rows.Next() {
		aggregate := &PostgresAggregate{}

		err := rows.Scan(
			&aggregate.Name,
			&aggregate.Arguments,
			&aggregate.StateType,
			&aggregate.TransitionFunction,
			&aggregate.FinalFunction,
			&aggregate.InitialCondition,
		)
		if err != nil {
			return nil, err
		}

		aggregates = append(aggregates, aggregate)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return aggregates, nil
}

func (d *PostgresDriver) GetOperators(ctx context.Context, db *sql.DB) ([]*PostgresOperator, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			o.oprname,
			CASE WHEN o.oprleft <> 0 THEN format_type(o.oprleft, NULL) END,
			CASE WHEN o.oprright <> 0 THEN format_type(o.oprright, NULL) END,
			o.oprcode::regproc::text
		FROM pg_operator o
		WHERE o.oprnamespace = current_schema()::regnamespace
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = o.oid AND d.deptype = 'e'
		)
		ORDER BY o.oprname, o.oprleft, o.oprright
	`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var operators []*PostgresOperator

	for rows.Next() {
		operator := &PostgresOperator{}

		err := rows.Scan(
			&operator.Name,
			&operator.LeftArgument,
			&operator.RightArgument,
			&operator.Function,
		)
		if err != nil {
			return nil, err
		}

		operators = append(operators, operator)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return operators, nil
}

func (d *PostgresDriver) GetSequences(ctx context.Context, db *sql.DB) ([]*PostgresSequence, error) {
	// A serial column and an identity column own their sequence, and the table creates it.
	// s.last_value is NULL until the first call of nextval.
	rows, err := db.QueryContext(ctx, `
		SELECT s.sequencename, s.data_type::text, s.start_value, s.min_value, s.max_value, s.increment_by, s.cycle, s.last_value
		FROM pg_sequences s
		JOIN pg_class c ON c.relname = s.sequencename
			AND c.relkind = 'S'
			AND c.relnamespace = current_schema()::regnamespace
		WHERE s.schemaname = current_schema()
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype IN ('a', 'e', 'i')
		)
		ORDER BY s.sequencename
	`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var sequences []*PostgresSequence

	for rows.Next() {
		sequence := &PostgresSequence{}

		err := rows.Scan(
			&sequence.Name,
			&sequence.DataType,
			&sequence.Start,
			&sequence.Min,
			&sequence.Max,
			&sequence.Increment,
			&sequence.Cycle,
			&sequence.CurrentValue,
		)
		if err != nil {
			return nil, err
		}

		sequences = append(sequences, sequence)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return sequences, nil
}

func (d *PostgresDriver) GetFunctions(ctx context.Context, db *sql.DB) ([]*PostgresFunction, error) {
	// The regexp_replace call removes the schema prefix of the header. The source schema
	// and the target schema hold a different name, and the diff compares the two texts.
	rows, err := db.QueryContext(ctx, `
		SELECT
			p.proname,
			pg_get_function_identity_arguments(p.oid),
			pg_get_function_result(p.oid),
			regexp_replace(
				pg_get_functiondef(p.oid),
				'^CREATE OR REPLACE FUNCTION ' || quote_ident(current_schema()) || '\.',
				'CREATE OR REPLACE FUNCTION '
			)
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

	defer rows.Close()

	var functions []*PostgresFunction

	for rows.Next() {
		function := &PostgresFunction{}

		err := rows.Scan(&function.Name, &function.Arguments, &function.ReturnType, &function.Def)
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

func (d *PostgresDriver) GetViews(ctx context.Context, db *sql.DB) ([]*PostgresView, error) {
	viewRows, err := db.QueryContext(ctx, `
		SELECT table_name, view_definition
		FROM information_schema.views
		WHERE table_schema = current_schema()
		ORDER BY table_name
	`)
	if err != nil {
		return nil, err
	}

	defer viewRows.Close()

	var views []*PostgresView

	for viewRows.Next() {
		view := &PostgresView{}

		err := viewRows.Scan(&view.Name, &view.Def)
		if err != nil {
			return nil, err
		}

		views = append(views, view)
	}

	err = viewRows.Err()
	if err != nil {
		return nil, err
	}

	for _, view := range views {
		view.Columns, err = d.GetViewColumns(ctx, db, view.Name)
		if err != nil {
			return nil, err
		}
	}

	return sortViewsByDependency(views), nil
}

// sortViewsByDependency orders the views so that a view comes after every view that it
// reads. Two independent views keep the order that the query gives.
func sortViewsByDependency(views []*PostgresView) []*PostgresView {
	viewByName := make(map[string]*PostgresView, len(views))

	for _, view := range views {
		viewByName[view.Name] = view
	}

	sorted := make([]*PostgresView, 0, len(views))
	visited := make(map[string]bool, len(views))

	var visit func(view *PostgresView)

	visit = func(view *PostgresView) {
		if visited[view.Name] {
			return
		}

		visited[view.Name] = true

		for _, column := range view.Columns {
			dependency, isView := viewByName[column.Table]
			if isView {
				visit(dependency)
			}
		}

		sorted = append(sorted, view)
	}

	for _, view := range views {
		visit(view)
	}

	return sorted
}

// GetViewColumns returns the columns of the tables and of the views that one view reads.
// The type of each column comes with it, because a type change makes the view invalid.
func (d *PostgresDriver) GetViewColumns(ctx context.Context, db *sql.DB, viewName string) ([]*PostgresViewColumn, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT
			source_class.relname,
			source_attribute.attname,
			format_type(source_attribute.atttypid, source_attribute.atttypmod)
		FROM pg_depend d
		JOIN pg_rewrite view_rule ON view_rule.oid = d.objid
		JOIN pg_class view_class ON view_class.oid = view_rule.ev_class
		JOIN pg_class source_class ON source_class.oid = d.refobjid
		JOIN pg_attribute source_attribute
			ON source_attribute.attrelid = d.refobjid
			AND source_attribute.attnum = d.refobjsubid
		WHERE d.classid = 'pg_rewrite'::regclass
		AND d.refclassid = 'pg_class'::regclass
		AND d.refobjsubid > 0
		AND view_class.relnamespace = current_schema()::regnamespace
		AND view_class.relname = $1
		AND view_class.oid <> d.refobjid
		ORDER BY source_class.relname, source_attribute.attname
	`, viewName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var columns []*PostgresViewColumn

	for rows.Next() {
		column := &PostgresViewColumn{}

		err := rows.Scan(&column.Table, &column.Column, &column.Type)
		if err != nil {
			return nil, err
		}

		columns = append(columns, column)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (d *PostgresDriver) GetTables(ctx context.Context, db *sql.DB) ([]*PostgresTable, error) {
	tableRows, err := db.QueryContext(ctx, `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = current_schema() 
		AND table_type = 'BASE TABLE'
	`)
	if err != nil {
		return nil, err
	}

	defer tableRows.Close()

	var tables []*PostgresTable

	for tableRows.Next() {
		var tableName string

		err := tableRows.Scan(&tableName)
		if err != nil {
			return nil, err
		}

		table, err := d.GetTable(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	err = tableRows.Err()
	if err != nil {
		return nil, err
	}

	return tables, nil
}

func (d *PostgresDriver) GetTable(ctx context.Context, db *sql.DB, tableName string) (*PostgresTable, error) {
	table := &PostgresTable{Name: tableName}

	// information_schema.columns gives the text ARRAY for an array column, and the text
	// USER-DEFINED for an enum column or a composite column. format_type gives the exact
	// type name, for example integer[]. It also adds a prefix to a type of another schema,
	// so the query removes the prefix of the current schema.
	columnRows, err := db.QueryContext(ctx, `
			SELECT
				a.attname,
				regexp_replace(
					format_type(a.atttypid, a.atttypmod),
					'^' || quote_ident(current_schema()) || '\.',
					''
				),
				a.attnotnull,
				pg_get_expr(d.adbin, d.adrelid)
			FROM pg_attribute a
			LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
			WHERE a.attrelid = $1::regclass
			AND a.attnum > 0
			AND NOT a.attisdropped
			ORDER BY a.attnum
		`, quoteIdentifier(tableName))
	if err != nil {
		return nil, err
	}

	defer columnRows.Close()

	for columnRows.Next() {
		var columnName, columnType string
		var notNull bool
		var columnDefault sql.NullString

		err := columnRows.Scan(&columnName, &columnType, &notNull, &columnDefault)
		if err != nil {
			return nil, err
		}

		column := &PostgresColumn{
			Name:    columnName,
			Type:    columnType,
			NotNull: notNull,
			Default: columnDefault,
		}
		table.Columns = append(table.Columns, column)
	}

	err = columnRows.Err()
	if err != nil {
		return nil, err
	}

	constraintRows, err := db.QueryContext(ctx, `
			SELECT conname, contype, pg_get_constraintdef(oid)
			FROM pg_constraint
			WHERE conrelid = $1::regclass
		`, quoteIdentifier(tableName))
	if err != nil {
		return nil, err
	}

	defer constraintRows.Close()

	for constraintRows.Next() {
		constraint := &PostgresConstraint{}

		err := constraintRows.Scan(&constraint.Name, &constraint.Type, &constraint.Def)
		if err != nil {
			return nil, err
		}

		table.Constraints = append(table.Constraints, constraint)
	}

	err = constraintRows.Err()
	if err != nil {
		return nil, err
	}

	// The regexp_replace call removes the schema prefix of the table. Without it the
	// statement builds the index in the source schema.
	indexRows, err := db.QueryContext(ctx, `
			SELECT
				indexname,
				regexp_replace(
					indexdef,
					' ON ' || quote_ident(current_schema()) || '\.',
					' ON '
				)
			FROM pg_indexes
			WHERE schemaname = current_schema() AND tablename = $1
			AND indexname NOT IN (
				SELECT conname FROM pg_constraint WHERE conrelid = $2::regclass
			)
		`, tableName, quoteIdentifier(tableName))
	if err != nil {
		return nil, err
	}

	defer indexRows.Close()

	for indexRows.Next() {
		index := &PostgresIndex{}

		err := indexRows.Scan(&index.Name, &index.Def)
		if err != nil {
			return nil, err
		}

		table.Indexes = append(table.Indexes, index)
	}

	err = indexRows.Err()
	if err != nil {
		return nil, err
	}

	// The regexp_replace call removes the schema prefix of the table. Without it the
	// statement builds the trigger on the table of the source schema.
	triggerRows, err := db.QueryContext(ctx, `
			SELECT
				tgname,
				regexp_replace(
					pg_get_triggerdef(oid),
					' ON ' || quote_ident(current_schema()) || '\.',
					' ON '
				)
			FROM pg_trigger
			WHERE tgrelid = $1::regclass AND tgisinternal = false
		`, quoteIdentifier(tableName))
	if err != nil {
		return nil, err
	}

	defer triggerRows.Close()

	for triggerRows.Next() {
		trigger := &PostgresTrigger{}

		err := triggerRows.Scan(&trigger.Name, &trigger.Def)
		if err != nil {
			return nil, err
		}

		table.Triggers = append(table.Triggers, trigger)
	}

	err = triggerRows.Err()
	if err != nil {
		return nil, err
	}

	return table, nil
}
