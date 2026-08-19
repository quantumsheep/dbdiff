package drivers

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
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
	ComparePrivileges      bool
}

type PostgresDriver struct {
	SourceDatabaseConnection *sql.DB
	TargetDatabaseConnection *sql.DB
	SourceSchema             string
	TargetSchema             string
	CompareData              bool
	ComparePrivileges        bool

	ScratchVersion embeddedpostgres.PostgresVersion

	scratchServer *PostgresScratchServer
}

func NewPostgresDriver(ctx context.Context, config *PostgresDriverConfig) (*PostgresDriver, error) {
	driver := &PostgresDriver{
		SourceSchema:      config.SourceSchema,
		TargetSchema:      config.TargetSchema,
		CompareData:       config.CompareData,
		ComparePrivileges: config.ComparePrivileges,
		ScratchVersion:    postgresScratchVersionOfConfig(ctx, config),
	}

	sourceDatabaseConnection, err := driver.OpenSide(ctx, config.SourceConnectionString, config.SourceSchema, "source")
	if err != nil {
		driver.StopScratchServer()
		return nil, err
	}

	driver.SourceDatabaseConnection = sourceDatabaseConnection

	targetDatabaseConnection, err := driver.OpenSide(ctx, config.TargetConnectionString, config.TargetSchema, "target")
	if err != nil {
		driver.SourceDatabaseConnection.Close()
		driver.StopScratchServer()

		return nil, err
	}

	driver.TargetDatabaseConnection = targetDatabaseConnection

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
	sourceError := d.SourceDatabaseConnection.Close()
	targetError := d.TargetDatabaseConnection.Close()
	stopError := d.StopScratchServer()

	return firstError(sourceError, targetError, stopError)
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
	// function. An aggregate and an operator use a function. A materialized view reads a
	// table or a view. Keep this order.
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
		d.DiffStatistics,
		d.DiffViews,
		d.DiffMaterializedViews,
		d.DiffPrivileges,
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

type sectionRules[T any] struct {
	Get    func(ctx context.Context, db *sql.DB) ([]T, error)
	Key    func(object T) string
	Create func(source T) Instruction
	Change func(source T, target T) []Instruction
	Drop   func(target T) Instruction
}

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

func isTableDropped(name string, sourceTables []*PostgresTable) bool {
	_, found := lo.Find(sourceTables, func(table *PostgresTable) bool {
		return table.Name == name
	})

	return !found
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

	// The action of a rule can name a second table, so every rule comes after every table.
	var ruleInstructions []Instruction

	for _, sourceTable := range sourceTables {
		targetTable, found := lo.Find(targetTables, func(table *PostgresTable) bool {
			return table.Name == sourceTable.Name
		})
		if !found {
			additions = append(additions, sourceTable.Instructions()...)
			ruleInstructions = append(ruleInstructions, sourceTable.RuleInstructions()...)

			continue
		}

		subInstructions, err := sourceTable.DiffTable(targetTable, hasAutomaticCast)
		if err != nil {
			return nil, err
		}

		additions = append(additions, subInstructions...)
		ruleInstructions = append(ruleInstructions, sourceTable.DiffRules(targetTable)...)
	}

	additions = append(additions, ruleInstructions...)

	for _, targetTable := range targetTables {
		_, found := lo.Find(sourceTables, func(table *PostgresTable) bool {
			return table.Name == targetTable.Name
		})
		if found {
			continue
		}

		// A DROP TABLE statement of a parent removes every partition of it, so a second
		// statement for the partition fails.
		if targetTable.IsPartition() && isTableDropped(targetTable.PartitionParent, sourceTables) {
			continue
		}

		removals = append(removals, &SQLDropTableInstruction{Name: targetTable.Name})
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
		if sourceView.Def != targetView.Def || sourceView.CheckOption != targetView.CheckOption ||
			!sourceView.HasEqualColumns(targetView) {
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

		if sourceView.Def != targetView.Def || sourceView.CheckOption != targetView.CheckOption ||
			!sourceView.HasEqualColumns(targetView) {
			earlyRemovals = append(earlyRemovals, &SQLDropViewInstruction{Name: targetView.Name})
		}
	}

	return &SectionDiff{
		EarlyRemovals: earlyRemovals,
		Additions:     additions,
	}, nil
}

func (d *PostgresDriver) DiffMaterializedViews(ctx context.Context) (*SectionDiff, error) {
	var earlyRemovals []Instruction
	var additions []Instruction

	sourceViews, err := d.GetMaterializedViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetViews, err := d.GetMaterializedViews(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceView := range sourceViews {
		targetView, found := lo.Find(targetViews, func(view *PostgresMaterializedView) bool {
			return view.Name == sourceView.Name
		})
		if !found {
			additions = append(additions, sourceView.Instructions()...)
			continue
		}

		// A DROP statement of the view removes every index of it, so a changed view builds
		// each index again.
		if sourceView.Def != targetView.Def || !sourceView.HasEqualColumns(targetView) {
			additions = append(additions, sourceView.Instructions()...)
			continue
		}

		additions = append(additions, diffMaterializedViewIndexes(sourceView, targetView)...)
	}

	// PostgreSQL refuses a DROP statement while another view still reads the view, so a
	// backward walk drops each dependent view first.
	for _, targetView := range slices.Backward(targetViews) {
		sourceView, found := lo.Find(sourceViews, func(view *PostgresMaterializedView) bool {
			return view.Name == targetView.Name
		})
		if !found {
			earlyRemovals = append(earlyRemovals, targetView.DropInstruction())
			continue
		}

		if sourceView.Def != targetView.Def || !sourceView.HasEqualColumns(targetView) {
			earlyRemovals = append(earlyRemovals, targetView.DropInstruction())
		}
	}

	return &SectionDiff{
		EarlyRemovals: earlyRemovals,
		Additions:     additions,
	}, nil
}

func diffMaterializedViewIndexes(sourceView *PostgresMaterializedView, targetView *PostgresMaterializedView) []Instruction {
	var instructions []Instruction

	for _, sourceIndex := range sourceView.Indexes {
		targetIndex, found := targetView.IndexByName(sourceIndex.Name)
		if !found {
			instructions = append(instructions, sourceIndex.CreateInstruction())
			continue
		}

		if sourceIndex.Def != targetIndex.Def {
			instructions = append(instructions,
				&SQLDropIndexInstruction{Name: targetIndex.Name},
				sourceIndex.CreateInstruction())
		}
	}

	for _, targetIndex := range targetView.Indexes {
		_, found := sourceView.IndexByName(targetIndex.Name)
		if !found {
			instructions = append(instructions, &SQLDropIndexInstruction{Name: targetIndex.Name})
		}
	}

	return instructions
}

func (d *PostgresDriver) GetMaterializedViews(ctx context.Context, db *sql.DB) ([]*PostgresMaterializedView, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT m.matviewname, m.definition
		FROM pg_matviews m
		JOIN pg_class c ON c.relname = m.matviewname
			AND c.relnamespace = current_schema()::regnamespace
		WHERE m.schemaname = current_schema()
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = c.oid AND d.deptype = 'e'
		)
		ORDER BY m.matviewname
	`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var views []*PostgresMaterializedView

	for rows.Next() {
		view := &PostgresMaterializedView{}

		err := rows.Scan(&view.Name, &view.Def)
		if err != nil {
			return nil, err
		}

		views = append(views, view)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	for _, view := range views {
		columns, err := d.GetViewColumns(ctx, db, view.Name)
		if err != nil {
			return nil, err
		}

		view.Columns = columns

		indexes, err := d.GetMaterializedViewIndexes(ctx, db, view.Name)
		if err != nil {
			return nil, err
		}

		view.Indexes = indexes
	}

	return sortMaterializedViewsByDependency(views), nil
}

func (d *PostgresDriver) GetMaterializedViewIndexes(ctx context.Context, db *sql.DB, viewName string) ([]*PostgresIndex, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			indexname,
			regexp_replace(
				indexdef,
				' ON (ONLY )?' || quote_ident(current_schema()) || '\.',
				' ON '
			)
		FROM pg_indexes
		WHERE schemaname = current_schema() AND tablename = $1
		ORDER BY indexname
	`, viewName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var indexes []*PostgresIndex

	for rows.Next() {
		index := &PostgresIndex{}

		err := rows.Scan(&index.Name, &index.Def)
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, index)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return indexes, nil
}

func (d *PostgresDriver) GetTablePolicies(ctx context.Context, db *sql.DB, tableName string) ([]*PostgresPolicy, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			policyname,
			permissive,
			cmd,
			array_to_string(roles, ','),
			coalesce(qual, ''),
			coalesce(with_check, '')
		FROM pg_policies
		WHERE schemaname = current_schema() AND tablename = $1
		ORDER BY policyname
	`, tableName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var policies []*PostgresPolicy

	for rows.Next() {
		policy := &PostgresPolicy{Table: tableName}

		var roles string

		err := rows.Scan(&policy.Name, &policy.Permissive, &policy.Command, &roles,
			&policy.Using, &policy.WithCheck)
		if err != nil {
			return nil, err
		}

		if roles != "" {
			policy.Roles = strings.Split(roles, ",")
		}

		policies = append(policies, policy)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return policies, nil
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

// GetTableRules returns the rules of one table. pg_rules writes the name of the schema
// into the definition, so the query removes the prefix of the current schema. Without that
// step the statement builds the rule in the source schema.
//
// pg_rules reports no _RETURN rule, which is the implicit rule of a view.
func (d *PostgresDriver) GetTableRules(ctx context.Context, db *sql.DB, tableName string) ([]*PostgresRule, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			rulename,
			regexp_replace(
				definition,
				'\m' || quote_ident(current_schema()) || '\.',
				'',
				'g'
			)
		FROM pg_rules
		WHERE schemaname = current_schema() AND tablename = $1
		ORDER BY rulename
	`, tableName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var rules []*PostgresRule

	for rows.Next() {
		rule := &PostgresRule{Table: tableName}

		err := rows.Scan(&rule.Name, &rule.Def)
		if err != nil {
			return nil, err
		}

		rules = append(rules, rule)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return rules, nil
}

// An extended statistics object names a table, so this section comes after the tables.
func (d *PostgresDriver) DiffStatistics(ctx context.Context) (*SectionDiff, error) {
	var additions []Instruction
	var removals []Instruction

	sourceStatistics, err := d.GetStatistics(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetStatistics, err := d.GetStatistics(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceObject := range sourceStatistics {
		targetObject, found := lo.Find(targetStatistics, func(object *PostgresStatistics) bool {
			return object.Name == sourceObject.Name
		})
		if !found {
			additions = append(additions, sourceObject.CreateInstruction())
			continue
		}

		if sourceObject.Def != targetObject.Def {
			additions = append(additions,
				targetObject.DropInstruction(), sourceObject.CreateInstruction())
		}
	}

	for _, targetObject := range targetStatistics {
		_, found := lo.Find(sourceStatistics, func(object *PostgresStatistics) bool {
			return object.Name == targetObject.Name
		})
		if !found {
			removals = append(removals, targetObject.DropInstruction())
		}
	}

	return &SectionDiff{
		Additions: additions,
		Removals:  removals,
	}, nil
}

// pg_get_statisticsobjdef writes the name of the schema, so the query removes the prefix of
// the current schema. Without that step the statement builds the object in the source
// schema.
func (d *PostgresDriver) GetStatistics(ctx context.Context, db *sql.DB) ([]*PostgresStatistics, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			stxname,
			regexp_replace(
				pg_get_statisticsobjdef(oid),
				'\m' || quote_ident(current_schema()) || '\.',
				'',
				'g'
			)
		FROM pg_statistic_ext
		WHERE stxnamespace = current_schema()::regnamespace
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = pg_statistic_ext.oid AND d.deptype = 'e'
		)
		ORDER BY stxname
	`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var statistics []*PostgresStatistics

	for rows.Next() {
		object := &PostgresStatistics{}

		err := rows.Scan(&object.Name, &object.Def)
		if err != nil {
			return nil, err
		}

		statistics = append(statistics, object)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return statistics, nil
}

func (d *PostgresDriver) DiffPrivileges(ctx context.Context) (*SectionDiff, error) {
	if !d.ComparePrivileges {
		return &SectionDiff{}, nil
	}

	var additions []Instruction

	sourceOwners, err := d.GetOwners(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetOwners, err := d.GetOwners(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceOwner := range sourceOwners {
		targetOwner, found := lo.Find(targetOwners, func(owner *PostgresOwner) bool {
			return owner.ObjectName == sourceOwner.ObjectName
		})
		if found && targetOwner.Owner == sourceOwner.Owner {
			continue
		}

		additions = append(additions, sourceOwner.SetInstruction())
	}

	sourcePrivileges, err := d.GetPrivileges(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetPrivileges, err := d.GetPrivileges(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourcePrivilege := range sourcePrivileges {
		targetPrivilege, found := lo.Find(targetPrivileges, func(privilege *PostgresPrivilege) bool {
			return privilege.Key() == sourcePrivilege.Key()
		})
		if !found {
			additions = append(additions,
				sourcePrivilege.GrantInstruction(sourcePrivilege.Privileges))

			continue
		}

		granted := missingPrivileges(sourcePrivilege.Privileges, targetPrivilege.Privileges)
		if len(granted) > 0 {
			additions = append(additions, sourcePrivilege.GrantInstruction(granted))
		}

		revoked := missingPrivileges(targetPrivilege.Privileges, sourcePrivilege.Privileges)
		if len(revoked) > 0 {
			additions = append(additions, sourcePrivilege.RevokeInstruction(revoked))
		}
	}

	for _, targetPrivilege := range targetPrivileges {
		_, found := lo.Find(sourcePrivileges, func(privilege *PostgresPrivilege) bool {
			return privilege.Key() == targetPrivilege.Key()
		})
		if !found {
			additions = append(additions,
				targetPrivilege.RevokeInstruction(targetPrivilege.Privileges))
		}
	}

	return &SectionDiff{Additions: additions}, nil
}

func (d *PostgresDriver) GetOwners(ctx context.Context, db *sql.DB) ([]*PostgresOwner, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT c.relname, c.relkind, pg_get_userbyid(c.relowner)
		FROM pg_class c
		WHERE c.relnamespace = current_schema()::regnamespace
		AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
		AND NOT c.relispartition
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = c.oid AND d.deptype IN ('e', 'a', 'i')
		)
		ORDER BY c.relname
	`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var owners []*PostgresOwner

	for rows.Next() {
		var name, relkind, owner string

		err := rows.Scan(&name, &relkind, &owner)
		if err != nil {
			return nil, err
		}

		owners = append(owners, &PostgresOwner{
			ObjectType: ownerObjectType(relkind),
			ObjectName: name,
			Owner:      owner,
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return owners, nil
}

// This method reads no privilege of the owner, because PostgreSQL gives those with the
// object.
func (d *PostgresDriver) GetPrivileges(ctx context.Context, db *sql.DB) ([]*PostgresPrivilege, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			c.relname,
			c.relkind,
			pg_get_userbyid(acl.grantee),
			array_to_string(array_agg(acl.privilege_type ORDER BY acl.privilege_type), ',')
		FROM pg_class c, aclexplode(c.relacl) acl
		WHERE c.relnamespace = current_schema()::regnamespace
		AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
		AND acl.grantee <> c.relowner
		AND acl.grantee <> 0
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = c.oid AND d.deptype IN ('e', 'a', 'i')
		)
		GROUP BY c.relname, c.relkind, acl.grantee
		ORDER BY c.relname, pg_get_userbyid(acl.grantee)
	`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var privileges []*PostgresPrivilege

	for rows.Next() {
		var name, relkind, grantee, granted string

		err := rows.Scan(&name, &relkind, &grantee, &granted)
		if err != nil {
			return nil, err
		}

		privileges = append(privileges, &PostgresPrivilege{
			ObjectType: privilegeObjectType(relkind),
			ObjectName: name,
			Grantee:    grantee,
			Privileges: sortedPrivileges(strings.Split(granted, ",")),
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return privileges, nil
}

func (d *PostgresDriver) GetViews(ctx context.Context, db *sql.DB) ([]*PostgresView, error) {
	viewRows, err := db.QueryContext(ctx, `
		SELECT table_name, view_definition,
			CASE WHEN check_option = 'NONE' THEN '' ELSE check_option END
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

		err := viewRows.Scan(&view.Name, &view.Def, &view.CheckOption)
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

// GetTables returns each table of the schema. relkind names a partitioned table with the
// value p, and relispartition names a partition. The query reads the key of a partitioned
// table and the bound of a partition, because information_schema reports neither.
//
// relreplident names the mode of the replica identity. The mode i names an index, and
// pg_index reports that index with the flag indisreplident.
//
// pg_inherits names the parent of a partition and the parent of a table of INHERITS, so
// relispartition separates the two. Without that test a table of INHERITS takes the
// statement of a partition, and that statement holds no bound.
func (d *PostgresDriver) GetTables(ctx context.Context, db *sql.DB) ([]*PostgresTable, error) {
	tableRows, err := db.QueryContext(ctx, `
		SELECT
			c.relname,
			coalesce(pg_get_partkeydef(c.oid), ''),
			CASE WHEN c.relispartition THEN coalesce((
				SELECT parent.relname
				FROM pg_inherits i
				JOIN pg_class parent ON parent.oid = i.inhparent
				WHERE i.inhrelid = c.oid
			), '') ELSE '' END,
			coalesce(pg_get_expr(c.relpartbound, c.oid), ''),
			coalesce((
				SELECT array_to_string(array_agg(parent.relname ORDER BY i.inhseqno), ',')
				FROM pg_inherits i
				JOIN pg_class parent ON parent.oid = i.inhparent
				WHERE i.inhrelid = c.oid AND NOT c.relispartition
			), ''),
			coalesce(obj_description(c.oid, 'pg_class'), ''),
			c.relrowsecurity,
			c.relforcerowsecurity,
			c.relpersistence = 'u',
			coalesce(array_to_string(c.reloptions, ','), ''),
			CASE c.relreplident
				WHEN 'd' THEN 'DEFAULT'
				WHEN 'n' THEN 'NOTHING'
				WHEN 'f' THEN 'FULL'
				WHEN 'i' THEN 'USING INDEX'
				ELSE ''
			END,
			coalesce((
				SELECT replica_index.relname
				FROM pg_index i
				JOIN pg_class replica_index ON replica_index.oid = i.indexrelid
				WHERE i.indrelid = c.oid AND i.indisreplident
			), '')
		FROM pg_class c
		WHERE c.relnamespace = current_schema()::regnamespace
		AND c.relkind IN ('r', 'p')
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = c.oid AND d.deptype = 'e'
		)
		ORDER BY c.relname
	`)
	if err != nil {
		return nil, err
	}

	defer tableRows.Close()

	var tables []*PostgresTable

	for tableRows.Next() {
		var tableName, partitionKey, partitionParent, partitionBound, inherits, comment string
		var rowLevelSecurity, forceRowLevelSecurity, unlogged bool
		var storageParameters, replicaIdentity, replicaIdentityIndex string

		err := tableRows.Scan(&tableName, &partitionKey, &partitionParent, &partitionBound,
			&inherits, &comment, &rowLevelSecurity, &forceRowLevelSecurity, &unlogged,
			&storageParameters, &replicaIdentity, &replicaIdentityIndex)
		if err != nil {
			return nil, err
		}

		table, err := d.GetTable(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		table.PartitionKey = partitionKey
		table.PartitionParent = partitionParent
		table.PartitionBound = partitionBound
		table.Comment = comment

		if inherits != "" {
			table.Inherits = strings.Split(inherits, ",")
		}
		table.Unlogged = unlogged

		if storageParameters != "" {
			table.StorageParameters = strings.Split(storageParameters, ",")
		}
		table.ReplicaIdentity = replicaIdentity
		table.ReplicaIdentityIndex = replicaIdentityIndex
		table.RowLevelSecurity = rowLevelSecurity
		table.ForceRowLevelSecurity = forceRowLevelSecurity

		policies, err := d.GetTablePolicies(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		table.Policies = policies

		tables = append(tables, table)
	}

	err = tableRows.Err()
	if err != nil {
		return nil, err
	}

	return sortTablesByPartitionParent(tables), nil
}

func (d *PostgresDriver) GetTable(ctx context.Context, db *sql.DB, tableName string) (*PostgresTable, error) {
	table := &PostgresTable{Name: tableName}

	// information_schema.columns gives the text ARRAY for an array column, and the text
	// USER-DEFINED for an enum column or a composite column. format_type gives the exact
	// type name, for example integer[]. It also adds a prefix to a type of another schema,
	// so the query removes the prefix of the current schema.
	//
	// The sequence of an identity column holds the options of that column. The query builds
	// the text of the options that differ from the default of the type, so a column that
	// keeps every default reads an empty text.
	//
	// pg_attrdef holds the expression of a stored generated column, and it holds the
	// default value of every other column. The two CASE expressions separate the two, so
	// a generated column never becomes a column with a default value. PostgreSQL refuses
	// a DEFAULT expression that reads another column.
	//
	// attstorage names the storage mode of the column, and typstorage names the mode that
	// a CREATE TABLE statement gives. The query gives an empty text when the two are equal,
	// because such a column needs no statement of the mode.
	//
	// attstattarget holds the statistics target. PostgreSQL 16 writes -1 for the default
	// target, and PostgreSQL 17 writes NULL. The CASE expression gives NULL for both.
	columnRows, err := db.QueryContext(ctx, `
			SELECT
				a.attname,
				regexp_replace(
					format_type(a.atttypid, a.atttypmod),
					'^' || quote_ident(current_schema()) || '\.',
					''
				),
				a.attnotnull,
				CASE WHEN a.attgenerated = '' THEN pg_get_expr(d.adbin, d.adrelid) END,
				CASE a.attidentity WHEN 'a' THEN 'ALWAYS' WHEN 'd' THEN 'BY DEFAULT' ELSE '' END,
				CASE WHEN a.attgenerated = 's' THEN pg_get_expr(d.adbin, d.adrelid) ELSE '' END,
				coalesce(column_collation.collname, ''),
				coalesce(col_description(a.attrelid, a.attnum), ''),
				CASE WHEN a.attstorage <> base_type.typstorage THEN
					CASE a.attstorage
						WHEN 'p' THEN 'PLAIN'
						WHEN 'e' THEN 'EXTERNAL'
						WHEN 'm' THEN 'MAIN'
						WHEN 'x' THEN 'EXTENDED'
					END
				ELSE '' END,
				CASE WHEN a.attstattarget >= 0 THEN a.attstattarget END,
				coalesce((
					SELECT nullif(concat_ws(' ',
						CASE WHEN s.seqstart <> s.seqmin THEN 'START WITH ' || s.seqstart END,
						CASE WHEN s.seqincrement <> 1 THEN 'INCREMENT BY ' || s.seqincrement END,
						CASE WHEN s.seqmin <> 1 THEN 'MINVALUE ' || s.seqmin END,
						CASE WHEN s.seqmax <> CASE
							WHEN s.seqtypid = 'smallint'::regtype THEN 32767
							WHEN s.seqtypid = 'integer'::regtype THEN 2147483647
							ELSE 9223372036854775807 END
							THEN 'MAXVALUE ' || s.seqmax END,
						CASE WHEN s.seqcache <> 1 THEN 'CACHE ' || s.seqcache END,
						CASE WHEN s.seqcycle THEN 'CYCLE' END
					), '')
					FROM pg_sequence s
					JOIN pg_depend dep ON dep.objid = s.seqrelid AND dep.deptype = 'i'
					WHERE dep.refobjid = a.attrelid AND dep.refobjsubid = a.attnum
				), '')
			FROM pg_attribute a
			LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
			LEFT JOIN pg_type base_type ON base_type.oid = a.atttypid
			LEFT JOIN pg_collation column_collation ON column_collation.oid = a.attcollation
				AND a.attcollation <> base_type.typcollation
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
		var columnName, columnType, identity, generatedExpression, collation, comment string
		var identityOptions, storage string
		var notNull bool
		var columnDefault sql.NullString
		var statisticsTarget sql.NullInt64

		err := columnRows.Scan(&columnName, &columnType, &notNull, &columnDefault,
			&identity, &generatedExpression, &collation, &comment, &storage,
			&statisticsTarget, &identityOptions)
		if err != nil {
			return nil, err
		}

		column := &PostgresColumn{
			Name:                columnName,
			Type:                columnType,
			NotNull:             notNull,
			Default:             columnDefault,
			Identity:            identity,
			GeneratedExpression: generatedExpression,
			Collation:           collation,
			Comment:             comment,
			IdentityOptions:     identityOptions,
			Storage:             storage,
			StatisticsTarget:    statisticsTarget,
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
	// statement builds the index in the source schema. It removes the keyword ONLY too.
	// PostgreSQL writes that keyword for the index of a partitioned table, and a statement
	// that holds it builds no index on the partitions.
	indexRows, err := db.QueryContext(ctx, `
			SELECT
				indexname,
				regexp_replace(
					indexdef,
					' ON (ONLY )?' || quote_ident(current_schema()) || '\.',
					' ON '
				)
			FROM pg_indexes
			WHERE schemaname = current_schema() AND tablename = $1
			AND indexname NOT IN (
				SELECT conname FROM pg_constraint WHERE conrelid = $2::regclass
			)
			AND NOT EXISTS (
				SELECT 1
				FROM pg_inherits i
				JOIN pg_class child ON child.oid = i.inhrelid
				WHERE child.relname = pg_indexes.indexname
				AND child.relnamespace = current_schema()::regnamespace
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
	//
	// pg_get_triggerdef writes no mode, so the query reads tgenabled apart. The value O
	// names the mode of every new trigger.
	triggerRows, err := db.QueryContext(ctx, `
			SELECT
				tgname,
				regexp_replace(
					pg_get_triggerdef(oid),
					' ON ' || quote_ident(current_schema()) || '\.',
					' ON '
				),
				CASE tgenabled
					WHEN 'O' THEN 'ENABLE'
					WHEN 'D' THEN 'DISABLE'
					WHEN 'R' THEN 'ENABLE REPLICA'
					WHEN 'A' THEN 'ENABLE ALWAYS'
					ELSE 'ENABLE'
				END
			FROM pg_trigger
			WHERE tgrelid = $1::regclass AND tgisinternal = false
		`, quoteIdentifier(tableName))
	if err != nil {
		return nil, err
	}

	defer triggerRows.Close()

	for triggerRows.Next() {
		trigger := &PostgresTrigger{}

		err := triggerRows.Scan(&trigger.Name, &trigger.Def, &trigger.EnableMode)
		if err != nil {
			return nil, err
		}

		table.Triggers = append(table.Triggers, trigger)
	}

	err = triggerRows.Err()
	if err != nil {
		return nil, err
	}

	rules, err := d.GetTableRules(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	table.Rules = rules

	return table, nil
}
