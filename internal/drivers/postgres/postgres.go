package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type PostgresDriverConfig struct {
	Schema               string
	ComparePrivileges    bool
	ScratchServerVersion string
	IgnoreTables         []string
}

type PostgresDriver struct {
	TargetDatabaseConnection *sql.DB
	SourceDatabaseConnection *sql.DB
	TargetSchema             string
	SourceSchema             string
	ComparePrivileges        bool
	IgnoreTables             []string
	ScratchServerVersion     string

	ScratchVersion embeddedpostgres.PostgresVersion

	scratchServer *PostgresScratchServer

	recreatedObjectNames map[string]bool
}

func NewPostgresDriver(config *PostgresDriverConfig) *PostgresDriver {
	return &PostgresDriver{
		TargetSchema:         config.Schema,
		SourceSchema:         config.Schema,
		ComparePrivileges:    config.ComparePrivileges,
		IgnoreTables:         config.IgnoreTables,
		ScratchServerVersion: config.ScratchServerVersion,
	}
}

func OpenPostgresConnection(connectionString string, schema string) (*sql.DB, error) {
	if schema == "" {
		return sql.Open("pgx", connectionString)
	}

	connectionConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}

	connectionConfig.RuntimeParams["search_path"] = driversshared.QuoteIdentifier(schema)

	return stdlib.OpenDB(*connectionConfig), nil
}

// PostgreSQL accepts a search path that names no schema, and each query then reads an
// empty schema. Without this check the diff drops every object of the source.
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

func (d *PostgresDriver) Diff(ctx context.Context, source driversshared.DataSource,
	target driversshared.DataSource, options driversshared.DiffOptions) (driversshared.Instructions, error) {
	release, err := d.openSides(ctx, source, target)
	if err != nil {
		return nil, err
	}

	defer release()

	d.recreatedObjectNames = nil

	err = d.VerifySchema(ctx, d.TargetDatabaseConnection, d.TargetSchema, "target")
	if err != nil {
		return nil, err
	}

	err = d.VerifySchema(ctx, d.SourceDatabaseConnection, d.SourceSchema, "source")
	if err != nil {
		return nil, err
	}

	// Every section comes after the sections that its objects use. Keep this order.
	sections := []func(ctx context.Context) (*driversshared.SectionDiff, error){
		d.DiffExtensions,
		d.DiffTypes,
		d.DiffDomains,
		d.DiffCompositeTypes,
		d.DiffSequences,
		d.DiffFunctions,
		d.DiffCasts,
		d.DiffAggregates,
		d.DiffOperators,
		d.DiffTables,
		d.DiffStatistics,
		d.DiffViewsAndMaterializedViews,
		d.DiffPrivileges,
	}

	sectionDiffs := make([]*driversshared.SectionDiff, 0, len(sections))

	for _, section := range sections {
		sectionDiff, err := section(ctx)
		if err != nil {
			return nil, err
		}

		sectionDiffs = append(sectionDiffs, sectionDiff)
	}

	var instructions []driversshared.Instruction

	// PostgreSQL refuses a DROP statement while another object uses the object, so a removal
	// takes the reverse section order. An early removal runs before every addition.
	for _, sectionDiff := range slices.Backward(sectionDiffs) {
		instructions = append(instructions, sectionDiff.EarlyRemovals...)
	}

	for _, sectionDiff := range sectionDiffs {
		instructions = append(instructions, sectionDiff.Additions...)
	}

	for _, sectionDiff := range slices.Backward(sectionDiffs) {
		instructions = append(instructions, sectionDiff.Removals...)
	}

	if options.CompareData {
		dataInstructions, err := d.DiffData(ctx)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, dataInstructions...)
	}

	return instructions, nil
}

func (d *PostgresDriver) openSides(ctx context.Context, source driversshared.DataSource,
	target driversshared.DataSource) (func(), error) {
	version, err := d.scratchVersionOfSources(ctx, source, target)
	if err != nil {
		return nil, err
	}

	d.ScratchVersion = version

	targetDatabaseConnection, err := d.OpenSide(ctx, target, d.TargetSchema, "target")
	if err != nil {
		_ = d.StopScratchServer()
		return nil, err
	}

	d.TargetDatabaseConnection = targetDatabaseConnection

	sourceDatabaseConnection, err := d.OpenSide(ctx, source, d.SourceSchema, "source")
	if err != nil {
		_ = d.TargetDatabaseConnection.Close()
		_ = d.StopScratchServer()

		return nil, err
	}

	d.SourceDatabaseConnection = sourceDatabaseConnection

	return func() {
		_ = d.TargetDatabaseConnection.Close()
		_ = d.SourceDatabaseConnection.Close()
		d.TargetDatabaseConnection = nil
		d.SourceDatabaseConnection = nil
		_ = d.StopScratchServer()
	}, nil
}

type sectionRules[T any] struct {
	Get    func(ctx context.Context, db *sql.DB) ([]T, error)
	Key    func(object T) string
	Create func(target T) []driversshared.Instruction
	Change func(target T, source T) []driversshared.Instruction
	Drop   func(source T) driversshared.Instruction
}

func diffSection[T any](ctx context.Context, driver *PostgresDriver, rules sectionRules[T]) (*driversshared.SectionDiff, error) {
	targetObjects, err := rules.Get(ctx, driver.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceObjects, err := rules.Get(ctx, driver.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	additions, removals, err := driversshared.DiffByKey(targetObjects, sourceObjects, driversshared.DiffRules[T]{
		Key:    rules.Key,
		Create: rules.Create,
		Change: func(target T, source T) ([]driversshared.Instruction, error) {
			return rules.Change(target, source), nil
		},
		Drop: func(source T) []driversshared.Instruction {
			return []driversshared.Instruction{rules.Drop(source)}
		},
	})
	if err != nil {
		return nil, err
	}

	return &driversshared.SectionDiff{
		Additions: additions,
		Removals:  removals,
	}, nil
}

func (d *PostgresDriver) DiffExtensions(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresExtension]{
		Get: d.GetExtensions,
		Key: func(extension *PostgresExtension) string {
			return extension.Name
		},
		Create: func(extension *PostgresExtension) []driversshared.Instruction {
			return []driversshared.Instruction{extension.CreateInstruction()}
		},
		Change: func(target *PostgresExtension, source *PostgresExtension) []driversshared.Instruction {
			if target.Version == source.Version {
				return nil
			}

			return []driversshared.Instruction{target.UpdateInstruction()}
		},
		Drop: func(source *PostgresExtension) driversshared.Instruction {
			return source.DropInstruction()
		},
	})
}

func (d *PostgresDriver) DiffTypes(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresType]{
		Get: d.GetTypes,
		Key: func(enumType *PostgresType) string {
			return enumType.Name
		},
		Create: func(enumType *PostgresType) []driversshared.Instruction {
			return enumType.Instructions()
		},
		Change: func(target *PostgresType, source *PostgresType) []driversshared.Instruction {
			return target.Diff(source)
		},
		Drop: func(source *PostgresType) driversshared.Instruction {
			return &PostgresDropTypeInstruction{Name: source.Name}
		},
	})
}

func (d *PostgresDriver) DiffDomains(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresDomain]{
		Get: d.GetDomains,
		Key: func(domain *PostgresDomain) string {
			return domain.Name
		},
		Create: func(domain *PostgresDomain) []driversshared.Instruction {
			return []driversshared.Instruction{domain.CreateInstruction()}
		},
		Change: func(target *PostgresDomain, source *PostgresDomain) []driversshared.Instruction {
			return target.Diff(source)
		},
		Drop: func(source *PostgresDomain) driversshared.Instruction {
			return source.DropInstruction()
		},
	})
}

func (d *PostgresDriver) DiffCompositeTypes(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresCompositeType]{
		Get: d.GetCompositeTypes,
		Key: func(compositeType *PostgresCompositeType) string {
			return compositeType.Name
		},
		Create: func(compositeType *PostgresCompositeType) []driversshared.Instruction {
			return []driversshared.Instruction{compositeType.CreateInstruction()}
		},
		Change: func(target *PostgresCompositeType, source *PostgresCompositeType) []driversshared.Instruction {
			return target.Diff(source)
		},
		Drop: func(source *PostgresCompositeType) driversshared.Instruction {
			return source.DropInstruction()
		},
	})
}

func (d *PostgresDriver) DiffAggregates(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresAggregate]{
		Get: d.GetAggregates,
		Key: func(aggregate *PostgresAggregate) string {
			return aggregate.Signature()
		},
		Create: func(aggregate *PostgresAggregate) []driversshared.Instruction {
			return []driversshared.Instruction{aggregate.CreateInstruction()}
		},
		Change: func(target *PostgresAggregate, source *PostgresAggregate) []driversshared.Instruction {
			return target.Diff(source)
		},
		Drop: func(source *PostgresAggregate) driversshared.Instruction {
			return source.DropInstruction()
		},
	})
}

func (d *PostgresDriver) DiffOperators(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresOperator]{
		Get: d.GetOperators,
		Key: func(operator *PostgresOperator) string {
			return operator.Signature()
		},
		Create: func(operator *PostgresOperator) []driversshared.Instruction {
			return []driversshared.Instruction{operator.CreateInstruction()}
		},
		Change: func(target *PostgresOperator, source *PostgresOperator) []driversshared.Instruction {
			return target.Diff(source)
		},
		Drop: func(source *PostgresOperator) driversshared.Instruction {
			return source.DropInstruction()
		},
	})
}

func (d *PostgresDriver) DiffSequences(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresSequence]{
		Get: d.GetSequences,
		Key: func(sequence *PostgresSequence) string {
			return sequence.Name
		},
		Create: func(sequence *PostgresSequence) []driversshared.Instruction {
			return []driversshared.Instruction{sequence.CreateInstruction()}
		},
		Change: func(target *PostgresSequence, source *PostgresSequence) []driversshared.Instruction {
			return target.Diff(source)
		},
		Drop: func(source *PostgresSequence) driversshared.Instruction {
			return &PostgresDropSequenceInstruction{Name: source.Name}
		},
	})
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

func (d *PostgresDriver) DiffCasts(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresCast]{
		Get: d.GetCasts,
		Key: func(cast *PostgresCast) string {
			return cast.SourceType + " AS " + cast.TargetType
		},
		Create: func(cast *PostgresCast) []driversshared.Instruction {
			return []driversshared.Instruction{cast.CreateInstruction()}
		},
		Change: func(target *PostgresCast, source *PostgresCast) []driversshared.Instruction {
			if target.Equal(source) {
				return nil
			}

			return []driversshared.Instruction{source.DropInstruction(), target.CreateInstruction()}
		},
		Drop: func(source *PostgresCast) driversshared.Instruction {
			return source.DropInstruction()
		},
	})
}

func isTableDropped(name string, targetTables []*PostgresTable) bool {
	_, found := lo.Find(targetTables, func(table *PostgresTable) bool {
		return table.Name == name
	})

	return !found
}

func (d *PostgresDriver) DiffTables(ctx context.Context) (*driversshared.SectionDiff, error) {
	var additions []driversshared.Instruction
	var removals []driversshared.Instruction

	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	hasAutomaticCast := func(oldType string, newType string) (bool, error) {
		return d.HasAutomaticCast(ctx, d.SourceDatabaseConnection, oldType, newType)
	}

	// The action of a rule can name a second table, so every rule comes after every table.
	var ruleInstructions []driversshared.Instruction

	// Two tables can name each other, so every foreign key comes after every table.
	var foreignKeyInstructions []driversshared.Instruction

	for _, targetTable := range targetTables {
		sourceTable, found := lo.Find(sourceTables, func(table *PostgresTable) bool {
			return table.Name == targetTable.Name
		})
		if !found {
			additions = append(additions, targetTable.Instructions()...)
			foreignKeyInstructions = append(foreignKeyInstructions,
				targetTable.ForeignKeyInstructions()...)
			ruleInstructions = append(ruleInstructions, targetTable.RuleInstructions()...)

			continue
		}

		subInstructions, err := targetTable.DiffTable(sourceTable, hasAutomaticCast)
		if err != nil {
			return nil, err
		}

		additions = append(additions, subInstructions...)
		ruleInstructions = append(ruleInstructions, targetTable.DiffRules(sourceTable)...)
	}

	additions = append(additions, foreignKeyInstructions...)
	additions = append(additions, ruleInstructions...)

	// The reverse order gives each DROP TABLE statement before the table that it names.
	for _, sourceTable := range slices.Backward(sourceTables) {
		_, found := lo.Find(targetTables, func(table *PostgresTable) bool {
			return table.Name == sourceTable.Name
		})
		if found {
			continue
		}

		// A DROP TABLE statement of a parent removes every partition of it.
		if sourceTable.IsPartition() && isTableDropped(sourceTable.PartitionParent, targetTables) {
			continue
		}

		removals = append(removals, &driversshared.SQLDropTableInstruction{Name: sourceTable.Name})
	}

	return &driversshared.SectionDiff{
		Additions: additions,
		Removals:  removals,
	}, nil
}

// PostgreSQL refuses a change of a column while a view that reads it exists, so every DROP
// VIEW statement goes into the early removals.
// A recreated object loses its privileges and its owner, and DiffPrivileges reads this
// set to give them back.
func (d *PostgresDriver) markRecreated(name string) {
	if d.recreatedObjectNames == nil {
		d.recreatedObjectNames = map[string]bool{}
	}

	d.recreatedObjectNames[name] = true
}

// A view can read a materialized view, and a materialized view can read a view, so the
// two kinds share one section and one dependency order.
func (d *PostgresDriver) DiffViewsAndMaterializedViews(ctx context.Context) (*driversshared.SectionDiff, error) {
	var earlyRemovals []driversshared.Instruction
	var additions []driversshared.Instruction

	targetViews, err := d.GetViews(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceViews, err := d.GetViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetMaterializedViews, err := d.GetMaterializedViews(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceMaterializedViews, err := d.GetMaterializedViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetRelations := sortRelationsByDependency(targetViews, targetMaterializedViews)
	sourceRelations := sortRelationsByDependency(sourceViews, sourceMaterializedViews)

	for _, relation := range targetRelations {
		if relation.View != nil {
			targetView := relation.View

			sourceView, found := lo.Find(sourceViews, func(view *PostgresView) bool {
				return view.Name == targetView.Name
			})
			if !found {
				additions = append(additions, targetView.Instructions()...)
				continue
			}

			// A type change of a column that the view reads keeps the definition text equal.
			if targetView.Def != sourceView.Def || targetView.CheckOption != sourceView.CheckOption ||
				!targetView.HasEqualColumns(sourceView) {
				d.markRecreated(targetView.Name)
				additions = append(additions, targetView.Instructions()...)
				continue
			}

			if targetView.Comment != sourceView.Comment {
				additions = append(additions, &PostgresCommentOnViewInstruction{
					Name: targetView.Name,
					Text: targetView.Comment,
				})
			}

			continue
		}

		targetView := relation.MaterializedView

		sourceView, found := lo.Find(sourceMaterializedViews, func(view *PostgresMaterializedView) bool {
			return view.Name == targetView.Name
		})
		if !found {
			additions = append(additions, targetView.Instructions()...)
			continue
		}

		// A DROP statement of the view removes every index of it.
		if targetView.Def != sourceView.Def || !targetView.HasEqualColumns(sourceView) {
			d.markRecreated(targetView.Name)
			additions = append(additions, targetView.Instructions()...)
			continue
		}

		if targetView.Comment != sourceView.Comment {
			additions = append(additions, &PostgresCommentOnMaterializedViewInstruction{
				Name: targetView.Name,
				Text: targetView.Comment,
			})
		}

		additions = append(additions, diffMaterializedViewIndexes(targetView, sourceView)...)
	}

	// PostgreSQL refuses a DROP statement while another view still reads the object, so
	// a backward walk drops each dependent view first.
	for _, relation := range slices.Backward(sourceRelations) {
		if relation.View != nil {
			sourceView := relation.View

			targetView, found := lo.Find(targetViews, func(view *PostgresView) bool {
				return view.Name == sourceView.Name
			})
			if !found {
				earlyRemovals = append(earlyRemovals, &driversshared.SQLDropViewInstruction{Name: sourceView.Name})
				continue
			}

			if targetView.Def != sourceView.Def || targetView.CheckOption != sourceView.CheckOption ||
				!targetView.HasEqualColumns(sourceView) {
				earlyRemovals = append(earlyRemovals, &driversshared.SQLDropViewInstruction{Name: sourceView.Name})
			}

			continue
		}

		sourceView := relation.MaterializedView

		targetView, found := lo.Find(targetMaterializedViews, func(view *PostgresMaterializedView) bool {
			return view.Name == sourceView.Name
		})
		if !found {
			earlyRemovals = append(earlyRemovals, sourceView.DropInstruction())
			continue
		}

		if targetView.Def != sourceView.Def || !targetView.HasEqualColumns(sourceView) {
			earlyRemovals = append(earlyRemovals, sourceView.DropInstruction())
		}
	}

	return &driversshared.SectionDiff{
		EarlyRemovals: earlyRemovals,
		Additions:     additions,
	}, nil
}

func diffMaterializedViewIndexes(targetView *PostgresMaterializedView, sourceView *PostgresMaterializedView) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	for _, targetIndex := range targetView.Indexes {
		sourceIndex, found := sourceView.IndexByName(targetIndex.Name)
		if !found {
			instructions = append(instructions, targetIndex.Instructions()...)
			continue
		}

		if targetIndex.Def != sourceIndex.Def {
			instructions = append(instructions, &driversshared.SQLDropIndexInstruction{Name: sourceIndex.Name})
			instructions = append(instructions, targetIndex.Instructions()...)
			continue
		}

		if targetIndex.Comment != sourceIndex.Comment {
			instructions = append(instructions, &PostgresCommentOnIndexInstruction{
				Name: targetIndex.Name,
				Text: targetIndex.Comment,
			})
		}
	}

	for _, sourceIndex := range sourceView.Indexes {
		_, found := targetView.IndexByName(sourceIndex.Name)
		if !found {
			instructions = append(instructions, &driversshared.SQLDropIndexInstruction{Name: sourceIndex.Name})
		}
	}

	return instructions
}

func (d *PostgresDriver) GetMaterializedViews(ctx context.Context, db *sql.DB) ([]*PostgresMaterializedView, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT m.matviewname, m.definition,
			coalesce(obj_description(c.oid, 'pg_class'), '')
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

	defer func() { _ = rows.Close() }()

	var views []*PostgresMaterializedView

	for rows.Next() {
		view := &PostgresMaterializedView{}

		err := rows.Scan(&view.Name, &view.Def, &view.Comment)
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
			),
			coalesce((
				SELECT obj_description(c.oid, 'pg_class')
				FROM pg_class c
				WHERE c.relnamespace = current_schema()::regnamespace
				AND c.relname = pg_indexes.indexname
			), '')
		FROM pg_indexes
		WHERE schemaname = current_schema() AND tablename = $1
		ORDER BY indexname
	`, viewName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var indexes []*PostgresIndex

	for rows.Next() {
		index := &PostgresIndex{}

		err := rows.Scan(&index.Name, &index.Def, &index.Comment)
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

	defer func() { _ = rows.Close() }()

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

	defer func() { _ = rows.Close() }()

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
		SELECT t.typname, e.enumlabel, coalesce(obj_description(t.oid, 'pg_type'), '')
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

	defer func() { _ = rows.Close() }()

	var types []*PostgresType

	for rows.Next() {
		var typeName, value, comment string

		err := rows.Scan(&typeName, &value, &comment)
		if err != nil {
			return nil, err
		}

		lastType, _ := lo.Last(types)
		if lastType == nil || lastType.Name != typeName {
			lastType = &PostgresType{
				Name:    typeName,
				Comment: comment,
			}
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

	defer func() { _ = rows.Close() }()

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

	defer func() { _ = rows.Close() }()

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
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var aggregates []*PostgresAggregate

	for rows.Next() {
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
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var operators []*PostgresOperator

	for rows.Next() {
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
		SELECT s.sequencename, s.data_type::text, s.start_value, s.min_value, s.max_value, s.increment_by, s.cache_size, s.cycle, s.last_value
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

	defer func() { _ = rows.Close() }()

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
			&sequence.Cache,
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

// A cast holds no schema, so the query scopes it to the schema of its source type or its
// target type. c.oid >= 16384 excludes the casts that PostgreSQL creates at initdb.
func (d *PostgresDriver) GetCasts(ctx context.Context, db *sql.DB) ([]*PostgresCast, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			format_type(c.castsource, NULL),
			format_type(c.casttarget, NULL),
			c.castmethod,
			c.castcontext,
			CASE WHEN c.castfunc <> 0
				THEN p.proname || '(' || pg_get_function_identity_arguments(c.castfunc) || ')'
			END
		FROM pg_cast c
		LEFT JOIN pg_proc p ON p.oid = c.castfunc
		LEFT JOIN pg_type source_type ON source_type.oid = c.castsource
		LEFT JOIN pg_type target_type ON target_type.oid = c.casttarget
		WHERE c.oid >= 16384
		AND (source_type.typnamespace = current_schema()::regnamespace
			OR target_type.typnamespace = current_schema()::regnamespace)
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e'
		)
		ORDER BY format_type(c.castsource, NULL), format_type(c.casttarget, NULL)
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var casts []*PostgresCast

	for rows.Next() {
		cast := &PostgresCast{}

		var function sql.NullString

		err := rows.Scan(&cast.SourceType, &cast.TargetType, &cast.Method, &cast.Context, &function)
		if err != nil {
			return nil, err
		}

		cast.Function = function.String

		casts = append(casts, cast)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return casts, nil
}

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

	defer func() { _ = rows.Close() }()

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
func (d *PostgresDriver) DiffStatistics(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresStatistics]{
		Get: d.GetStatistics,
		Key: func(statistics *PostgresStatistics) string {
			return statistics.Name
		},
		Create: func(statistics *PostgresStatistics) []driversshared.Instruction {
			return []driversshared.Instruction{statistics.CreateInstruction()}
		},
		Change: func(target *PostgresStatistics, source *PostgresStatistics) []driversshared.Instruction {
			if target.Def == source.Def {
				return nil
			}

			return []driversshared.Instruction{source.DropInstruction(), target.CreateInstruction()}
		},
		Drop: func(source *PostgresStatistics) driversshared.Instruction {
			return source.DropInstruction()
		},
	})
}

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

	defer func() { _ = rows.Close() }()

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

func (d *PostgresDriver) DiffPrivileges(ctx context.Context) (*driversshared.SectionDiff, error) {
	if !d.ComparePrivileges {
		return &driversshared.SectionDiff{}, nil
	}

	var additions []driversshared.Instruction

	targetOwners, err := d.GetOwners(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceOwners, err := d.GetOwners(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, targetOwner := range targetOwners {
		sourceOwner, found := lo.Find(sourceOwners, func(owner *PostgresOwner) bool {
			return owner.ObjectName == targetOwner.ObjectName
		})
		if found && sourceOwner.Owner == targetOwner.Owner &&
			!d.recreatedObjectNames[targetOwner.ObjectName] {
			continue
		}

		additions = append(additions, targetOwner.SetInstruction())
	}

	targetPrivileges, err := d.GetPrivileges(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourcePrivileges, err := d.GetPrivileges(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, targetPrivilege := range targetPrivileges {
		sourcePrivilege, found := lo.Find(sourcePrivileges, func(privilege *PostgresPrivilege) bool {
			return privilege.Key() == targetPrivilege.Key()
		})

		// The diff drops and creates the object again, and the new object holds the
		// default privileges, so every grant of the target comes back.
		if !found || d.recreatedObjectNames[targetPrivilege.ObjectName] {
			additions = append(additions,
				targetPrivilege.GrantInstruction(targetPrivilege.Privileges))

			continue
		}

		granted := missingPrivileges(targetPrivilege.Privileges, sourcePrivilege.Privileges)
		if len(granted) > 0 {
			additions = append(additions, targetPrivilege.GrantInstruction(granted))
		}

		revoked := missingPrivileges(sourcePrivilege.Privileges, targetPrivilege.Privileges)
		if len(revoked) > 0 {
			additions = append(additions, targetPrivilege.RevokeInstruction(revoked))
		}
	}

	targetObjectNames := lo.Map(targetOwners, func(owner *PostgresOwner, _ int) string {
		return owner.ObjectName
	})

	for _, sourcePrivilege := range sourcePrivileges {
		// The diff drops an object that the target does not hold, and the drop removes
		// every privilege of the object.
		if !slices.Contains(targetObjectNames, sourcePrivilege.ObjectName) ||
			d.recreatedObjectNames[sourcePrivilege.ObjectName] {
			continue
		}

		_, found := lo.Find(targetPrivileges, func(privilege *PostgresPrivilege) bool {
			return privilege.Key() == sourcePrivilege.Key()
		})
		if !found {
			additions = append(additions,
				sourcePrivilege.RevokeInstruction(sourcePrivilege.Privileges))
		}
	}

	return &driversshared.SectionDiff{Additions: additions}, nil
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
		AND c.relname <> $1
		ORDER BY c.relname
	`, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var owners []*PostgresOwner

	for rows.Next() {
		var name, relkind, owner string

		err := rows.Scan(&name, &relkind, &owner)
		if err != nil {
			return nil, err
		}

		if slices.Contains(d.IgnoreTables, name) {
			continue
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

// PostgreSQL gives the privileges of the owner with the object.
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
		AND c.relname <> $1
		GROUP BY c.relname, c.relkind, acl.grantee
		ORDER BY c.relname, pg_get_userbyid(acl.grantee)
	`, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var privileges []*PostgresPrivilege

	for rows.Next() {
		var name, relkind, grantee, granted string

		err := rows.Scan(&name, &relkind, &grantee, &granted)
		if err != nil {
			return nil, err
		}

		if slices.Contains(d.IgnoreTables, name) {
			continue
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
			CASE WHEN check_option = 'NONE' THEN '' ELSE check_option END,
			coalesce((
				SELECT obj_description(c.oid, 'pg_class')
				FROM pg_class c
				WHERE c.relnamespace = current_schema()::regnamespace
				AND c.relname = views.table_name
			), '')
		FROM information_schema.views
		WHERE table_schema = current_schema()
		AND NOT EXISTS (
			SELECT 1
			FROM pg_depend dep
			JOIN pg_class c ON c.oid = dep.objid
			WHERE dep.deptype = 'e'
			AND c.relnamespace = current_schema()::regnamespace
			AND c.relname = views.table_name
		)
		ORDER BY table_name
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = viewRows.Close() }()

	var views []*PostgresView

	for viewRows.Next() {
		view := &PostgresView{}

		err := viewRows.Scan(&view.Name, &view.Def, &view.CheckOption, &view.Comment)
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

// Two independent views keep the order that the query gives.
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

	defer func() { _ = rows.Close() }()

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

// information_schema reports no partition key, no partition bound, and no replica
// identity, so the query reads pg_class.
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
		AND c.relname <> $1
		ORDER BY c.relname
	`, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = tableRows.Close() }()

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

		if slices.Contains(d.IgnoreTables, tableName) {
			continue
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

	return sortTablesByDependency(tables), nil
}

func (d *PostgresDriver) GetTable(ctx context.Context, db *sql.DB, tableName string) (*PostgresTable, error) {
	table := &PostgresTable{Name: tableName}

	// format_type gives the exact type name, because information_schema.columns gives ARRAY
	// for an array column and USER-DEFINED for an enum column. The query removes the prefix
	// of the current schema from that name.
	//
	// pg_attrdef holds the expression of a stored generated column, and it holds the default
	// value of every other column. The CASE expressions separate the two, because PostgreSQL
	// refuses a DEFAULT expression that reads another column.
	//
	// An empty text of the identity options, of the storage mode, or of the statistics source
	// names a column that keeps the default of its type. PostgreSQL 16 writes -1 for the
	// default statistics source, and PostgreSQL 17 writes NULL. GetSequences excludes the
	// sequence of a serial column, so the word of that column builds it again.
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
				), ''),
				coalesce((
					SELECT CASE a.atttypid
						WHEN 'smallint'::regtype THEN 'smallserial'
						WHEN 'integer'::regtype THEN 'serial'
						WHEN 'bigint'::regtype THEN 'bigserial'
					END
					FROM pg_depend dep
					JOIN pg_class sequence_class ON sequence_class.oid = dep.objid
						AND sequence_class.relkind = 'S'
					WHERE dep.refobjid = a.attrelid
					AND dep.refobjsubid = a.attnum
					AND dep.deptype = 'a'
				), ''),
				coalesce((
					SELECT sequence_class.relname
					FROM pg_depend dep
					JOIN pg_class sequence_class ON sequence_class.oid = dep.objid
						AND sequence_class.relkind = 'S'
					WHERE dep.refobjid = a.attrelid
					AND dep.refobjsubid = a.attnum
					AND dep.deptype = 'a'
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
		`, driversshared.QuoteIdentifier(tableName))
	if err != nil {
		return nil, err
	}

	defer func() { _ = columnRows.Close() }()

	for columnRows.Next() {
		var columnName, columnType, identity, generatedExpression, collation, comment string
		var identityOptions, storage, serial, serialSequenceName string
		var notNull bool
		var columnDefault sql.NullString
		var statisticsTarget sql.NullInt64

		err := columnRows.Scan(&columnName, &columnType, &notNull, &columnDefault,
			&identity, &generatedExpression, &collation, &comment, &storage,
			&statisticsTarget, &identityOptions, &serial, &serialSequenceName)
		if err != nil {
			return nil, err
		}

		// The word serial holds the default, so the definition writes no DEFAULT clause.
		if serial != "" {
			columnDefault = sql.NullString{}
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
			SerialSequenceName:  serialSequenceName,
			Serial:              serial,
		}
		table.Columns = append(table.Columns, column)
	}

	err = columnRows.Err()
	if err != nil {
		return nil, err
	}

	// PostgreSQL 18 keeps the NOT NULL flag of a column in pg_constraint too, and the column
	// holds that flag already. Without this filter a renamed column names one flag with two
	// names, and the ADD statement fails, because a column holds one not-null constraint only.
	constraintRows, err := db.QueryContext(ctx, `
			SELECT
				conname,
				contype,
				pg_get_constraintdef(oid),
				coalesce((SELECT relname FROM pg_class WHERE oid = confrelid), '')
			FROM pg_constraint
			WHERE conrelid = $1::regclass
			AND contype <> 'n'
			ORDER BY conname
		`, driversshared.QuoteIdentifier(tableName))
	if err != nil {
		return nil, err
	}

	defer func() { _ = constraintRows.Close() }()

	for constraintRows.Next() {
		constraint := &PostgresConstraint{}

		var referencedTable string

		err := constraintRows.Scan(&constraint.Name, &constraint.Type, &constraint.Def,
			&referencedTable)
		if err != nil {
			return nil, err
		}

		if referencedTable != "" && referencedTable != tableName {
			table.References = append(table.References, referencedTable)
		}

		table.Constraints = append(table.Constraints, constraint)
	}

	err = constraintRows.Err()
	if err != nil {
		return nil, err
	}

	// The query removes the keyword ONLY. PostgreSQL writes it for the index of a partitioned
	// table, and a statement that holds it builds no index on the partitions.
	indexRows, err := db.QueryContext(ctx, `
			SELECT
				indexname,
				regexp_replace(
					indexdef,
					' ON (ONLY )?' || quote_ident(current_schema()) || '\.',
					' ON '
				),
				coalesce((
					SELECT obj_description(c.oid, 'pg_class')
					FROM pg_class c
					WHERE c.relnamespace = current_schema()::regnamespace
					AND c.relname = pg_indexes.indexname
				), '')
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
			ORDER BY indexname
		`, tableName, driversshared.QuoteIdentifier(tableName))
	if err != nil {
		return nil, err
	}

	defer func() { _ = indexRows.Close() }()

	for indexRows.Next() {
		index := &PostgresIndex{}

		err := indexRows.Scan(&index.Name, &index.Def, &index.Comment)
		if err != nil {
			return nil, err
		}

		table.Indexes = append(table.Indexes, index)
	}

	err = indexRows.Err()
	if err != nil {
		return nil, err
	}

	// pg_get_triggerdef writes no mode, so the query reads tgenabled apart. The value O names
	// the mode of every new trigger.
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
			ORDER BY tgname
		`, driversshared.QuoteIdentifier(tableName))
	if err != nil {
		return nil, err
	}

	defer func() { _ = triggerRows.Close() }()

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
