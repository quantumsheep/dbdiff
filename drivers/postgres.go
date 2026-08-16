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

	// CompareData turns the comparison of the rows on. The default value is false, so the
	// diff holds the schema only.
	CompareData bool
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

// Every query reads the first schema of the search path. An empty schema name keeps the
// search path of the connection string. A schema name replaces that search path on each
// connection of the pool.
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
// empty schema. This check gives an error instead.
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

func (d *PostgresDriver) Diff(ctx context.Context) (string, error) {
	err := d.VerifySchema(ctx, d.SourceDatabaseConnection, d.SourceSchema, "source")
	if err != nil {
		return "", err
	}

	err = d.VerifySchema(ctx, d.TargetDatabaseConnection, d.TargetSchema, "target")
	if err != nil {
		return "", err
	}

	// A table can use an extension, a type, a domain, a composite type, a sequence, or a
	// function, so these come first. An aggregate and an operator use a function, so these
	// come after the functions.
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
			return "", err
		}

		sectionDiffs = append(sectionDiffs, sectionDiff)
	}

	var diff strings.Builder

	// An early removal takes the reverse order too, and it runs before every addition. A
	// view reads a column of a table, and PostgreSQL refuses a change of that column while
	// the view exists.
	for _, sectionDiff := range slices.Backward(sectionDiffs) {
		if sectionDiff.EarlyRemovals != "" {
			fmt.Fprintln(&diff, sectionDiff.EarlyRemovals)
		}
	}

	for _, sectionDiff := range sectionDiffs {
		if sectionDiff.Additions != "" {
			fmt.Fprintln(&diff, sectionDiff.Additions)
		}
	}

	// A removal takes the reverse order. PostgreSQL refuses a DROP statement while
	// another object uses the object.
	for _, sectionDiff := range slices.Backward(sectionDiffs) {
		if sectionDiff.Removals != "" {
			fmt.Fprintln(&diff, sectionDiff.Removals)
		}
	}

	// A new row needs its table and its column, so the data section comes after the whole
	// schema section.
	if d.CompareData {
		dataDiff, err := d.DiffData(ctx)
		if err != nil {
			return "", err
		}

		if dataDiff != "" {
			fmt.Fprintln(&diff, dataDiff)
		}
	}

	return strings.TrimSpace(diff.String()), nil
}

func (d *PostgresDriver) DiffExtensions(ctx context.Context) (*SectionDiff, error) {
	var additions strings.Builder
	var removals strings.Builder

	sourceExtensions, err := d.GetExtensions(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetExtensions, err := d.GetExtensions(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceExtension := range sourceExtensions {
		targetExtension, found := lo.Find(targetExtensions, func(e *PostgresExtension) bool {
			return e.Name == sourceExtension.Name
		})

		if !found {
			fmt.Fprintf(&additions, "%s\n", sourceExtension.String())
			continue
		}

		if sourceExtension.Version != targetExtension.Version {
			fmt.Fprintf(&additions, "%s\n", sourceExtension.StringUpdate())
		}
	}

	for _, targetExtension := range targetExtensions {
		_, found := lo.Find(sourceExtensions, func(e *PostgresExtension) bool {
			return e.Name == targetExtension.Name
		})

		if !found {
			fmt.Fprintf(&removals, "%s\n", targetExtension.StringDrop())
		}
	}

	return newSectionDiff(&additions, &removals), nil
}

func (d *PostgresDriver) DiffTypes(ctx context.Context) (*SectionDiff, error) {
	var additions strings.Builder
	var removals strings.Builder

	sourceTypes, err := d.GetTypes(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetTypes, err := d.GetTypes(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceType := range sourceTypes {
		targetType, found := lo.Find(targetTypes, func(t *PostgresType) bool {
			return t.Name == sourceType.Name
		})

		if !found {
			fmt.Fprintf(&additions, "%s\n", sourceType.String())
			continue
		}

		subDiff := sourceType.Diff(targetType)
		if subDiff != "" {
			fmt.Fprintf(&additions, "%s\n", subDiff)
		}
	}

	for _, targetType := range targetTypes {
		_, found := lo.Find(sourceTypes, func(t *PostgresType) bool {
			return t.Name == targetType.Name
		})

		if !found {
			fmt.Fprintf(&removals, "DROP TYPE %s;\n", quoteIdentifier(targetType.Name))
		}
	}

	return newSectionDiff(&additions, &removals), nil
}

func (d *PostgresDriver) DiffDomains(ctx context.Context) (*SectionDiff, error) {
	var additions strings.Builder
	var removals strings.Builder

	sourceDomains, err := d.GetDomains(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetDomains, err := d.GetDomains(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceDomain := range sourceDomains {
		targetDomain, found := lo.Find(targetDomains, func(domain *PostgresDomain) bool {
			return domain.Name == sourceDomain.Name
		})

		if !found {
			fmt.Fprintf(&additions, "%s\n", sourceDomain.String())
			continue
		}

		subDiff := sourceDomain.Diff(targetDomain)
		if subDiff != "" {
			fmt.Fprintf(&additions, "%s\n", subDiff)
		}
	}

	for _, targetDomain := range targetDomains {
		_, found := lo.Find(sourceDomains, func(domain *PostgresDomain) bool {
			return domain.Name == targetDomain.Name
		})

		if !found {
			fmt.Fprintf(&removals, "%s\n", targetDomain.StringDrop())
		}
	}

	return newSectionDiff(&additions, &removals), nil
}

func (d *PostgresDriver) DiffCompositeTypes(ctx context.Context) (*SectionDiff, error) {
	var additions strings.Builder
	var removals strings.Builder

	sourceCompositeTypes, err := d.GetCompositeTypes(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetCompositeTypes, err := d.GetCompositeTypes(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceCompositeType := range sourceCompositeTypes {
		targetCompositeType, found := lo.Find(targetCompositeTypes, func(t *PostgresCompositeType) bool {
			return t.Name == sourceCompositeType.Name
		})

		if !found {
			fmt.Fprintf(&additions, "%s\n", sourceCompositeType.String())
			continue
		}

		subDiff := sourceCompositeType.Diff(targetCompositeType)
		if subDiff != "" {
			fmt.Fprintf(&additions, "%s\n", subDiff)
		}
	}

	for _, targetCompositeType := range targetCompositeTypes {
		_, found := lo.Find(sourceCompositeTypes, func(t *PostgresCompositeType) bool {
			return t.Name == targetCompositeType.Name
		})

		if !found {
			fmt.Fprintf(&removals, "%s\n", targetCompositeType.StringDrop())
		}
	}

	return newSectionDiff(&additions, &removals), nil
}

func (d *PostgresDriver) DiffAggregates(ctx context.Context) (*SectionDiff, error) {
	var additions strings.Builder
	var removals strings.Builder

	sourceAggregates, err := d.GetAggregates(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetAggregates, err := d.GetAggregates(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceAggregate := range sourceAggregates {
		targetAggregate, found := lo.Find(targetAggregates, func(a *PostgresAggregate) bool {
			return a.Signature() == sourceAggregate.Signature()
		})

		if !found {
			fmt.Fprintf(&additions, "%s\n", sourceAggregate.String())
			continue
		}

		subDiff := sourceAggregate.Diff(targetAggregate)
		if subDiff != "" {
			fmt.Fprintf(&additions, "%s\n", subDiff)
		}
	}

	for _, targetAggregate := range targetAggregates {
		_, found := lo.Find(sourceAggregates, func(a *PostgresAggregate) bool {
			return a.Signature() == targetAggregate.Signature()
		})

		if !found {
			fmt.Fprintf(&removals, "%s\n", targetAggregate.StringDrop())
		}
	}

	return newSectionDiff(&additions, &removals), nil
}

func (d *PostgresDriver) DiffOperators(ctx context.Context) (*SectionDiff, error) {
	var additions strings.Builder
	var removals strings.Builder

	sourceOperators, err := d.GetOperators(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetOperators, err := d.GetOperators(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceOperator := range sourceOperators {
		targetOperator, found := lo.Find(targetOperators, func(o *PostgresOperator) bool {
			return o.Signature() == sourceOperator.Signature()
		})

		if !found {
			fmt.Fprintf(&additions, "%s\n", sourceOperator.String())
			continue
		}

		subDiff := sourceOperator.Diff(targetOperator)
		if subDiff != "" {
			fmt.Fprintf(&additions, "%s\n", subDiff)
		}
	}

	for _, targetOperator := range targetOperators {
		_, found := lo.Find(sourceOperators, func(o *PostgresOperator) bool {
			return o.Signature() == targetOperator.Signature()
		})

		if !found {
			fmt.Fprintf(&removals, "%s\n", targetOperator.StringDrop())
		}
	}

	return newSectionDiff(&additions, &removals), nil
}

func (d *PostgresDriver) DiffSequences(ctx context.Context) (*SectionDiff, error) {
	var additions strings.Builder
	var removals strings.Builder

	sourceSequences, err := d.GetSequences(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetSequences, err := d.GetSequences(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceSequence := range sourceSequences {
		targetSequence, found := lo.Find(targetSequences, func(s *PostgresSequence) bool {
			return s.Name == sourceSequence.Name
		})

		if !found {
			fmt.Fprintf(&additions, "%s\n", sourceSequence.String())
			continue
		}

		subDiff := sourceSequence.Diff(targetSequence)
		if subDiff != "" {
			fmt.Fprintf(&additions, "%s\n", subDiff)
		}
	}

	for _, targetSequence := range targetSequences {
		_, found := lo.Find(sourceSequences, func(s *PostgresSequence) bool {
			return s.Name == targetSequence.Name
		})

		if !found {
			fmt.Fprintf(&removals, "DROP SEQUENCE %s;\n", quoteIdentifier(targetSequence.Name))
		}
	}

	return newSectionDiff(&additions, &removals), nil
}

func (d *PostgresDriver) DiffFunctions(ctx context.Context) (*SectionDiff, error) {
	var additions strings.Builder
	var removals strings.Builder

	sourceFunctions, err := d.GetFunctions(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetFunctions, err := d.GetFunctions(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	// The definition of PostgreSQL starts with CREATE OR REPLACE FUNCTION, so a new
	// function and a modified function take the same statement. PostgreSQL refuses that
	// statement when the return type changes, so the diff drops the target function
	// first in that case.
	for _, sourceFunction := range sourceFunctions {
		targetFunction, found := lo.Find(targetFunctions, func(f *PostgresFunction) bool {
			return f.Signature() == sourceFunction.Signature()
		})

		if found && sourceFunction.ReturnType != targetFunction.ReturnType {
			fmt.Fprintf(&additions, "%s\n", targetFunction.StringDrop())
		}

		if !found || sourceFunction.Def != targetFunction.Def {
			fmt.Fprintf(&additions, "%s\n", sourceFunction.String())
		}
	}

	for _, targetFunction := range targetFunctions {
		_, found := lo.Find(sourceFunctions, func(f *PostgresFunction) bool {
			return f.Signature() == targetFunction.Signature()
		})

		if !found {
			fmt.Fprintf(&removals, "%s\n", targetFunction.StringDrop())
		}
	}

	return newSectionDiff(&additions, &removals), nil
}

func (d *PostgresDriver) DiffTables(ctx context.Context) (*SectionDiff, error) {
	var additions strings.Builder
	var removals strings.Builder

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	// The target database holds the rows that the diff casts, so it answers the question
	// of the cast.
	hasAutomaticCast := func(oldType string, newType string) (bool, error) {
		return d.HasAutomaticCast(ctx, d.TargetDatabaseConnection, oldType, newType)
	}

	// Added or modified tables
	for _, sourceTable := range sourceTables {
		targetTable, found := lo.Find(targetTables, func(t *PostgresTable) bool {
			return t.Name == sourceTable.Name
		})

		// Table not found in target database
		if !found {
			fmt.Fprintf(&additions, "%s\n", sourceTable.String())
			continue
		}

		subDiff, err := sourceTable.DiffTable(targetTable, hasAutomaticCast)
		if err != nil {
			return nil, err
		}

		if subDiff != "" {
			fmt.Fprintf(&additions, "%s\n", subDiff)
		}
	}

	// Removed tables
	for _, targetTable := range targetTables {
		_, found := lo.Find(sourceTables, func(t *PostgresTable) bool {
			return t.Name == targetTable.Name
		})

		// Table not found in source database
		if !found {
			fmt.Fprintf(&removals, "DROP TABLE %s;\n", quoteIdentifier(targetTable.Name))
		}
	}

	return newSectionDiff(&additions, &removals), nil
}

// DiffViews writes every DROP VIEW statement into the early removals. A view reads the
// columns of a table, and PostgreSQL refuses a change of such a column while the view
// exists. The table section prints the change into its additions, so the view must go away
// before every addition.
func (d *PostgresDriver) DiffViews(ctx context.Context) (*SectionDiff, error) {
	var earlyRemovals strings.Builder
	var additions strings.Builder

	sourceViews, err := d.GetViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetViews, err := d.GetViews(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	// Added or modified views
	for _, sourceView := range sourceViews {
		targetView, found := lo.Find(targetViews, func(v *PostgresView) bool {
			return v.Name == sourceView.Name
		})

		if !found {
			fmt.Fprintf(&additions, "%s\n", sourceView.String())
			continue
		}

		// A column that the view reads can change its type. The definition text stays
		// equal in that case, so the columns give the second condition.
		if sourceView.Def != targetView.Def || !sourceView.HasEqualColumns(targetView) {
			fmt.Fprintf(&earlyRemovals, "DROP VIEW %s;\n", quoteIdentifier(targetView.Name))
			fmt.Fprintf(&additions, "%s\n", sourceView.String())
		}
	}

	// Removed views
	for _, targetView := range targetViews {
		_, found := lo.Find(sourceViews, func(v *PostgresView) bool {
			return v.Name == targetView.Name
		})

		if !found {
			fmt.Fprintf(&earlyRemovals, "DROP VIEW %s;\n", quoteIdentifier(targetView.Name))
		}
	}

	sectionDiff := &SectionDiff{
		EarlyRemovals: strings.TrimSpace(earlyRemovals.String()),
		Additions:     strings.TrimSpace(additions.String()),
	}

	return sectionDiff, nil
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
	// PostgreSQL 17 keeps the NOT NULL flag of a domain in pg_constraint too. The query
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
	// A serial column and an identity column own their sequence. The table creates it,
	// so the diff of the sequences leaves it out.
	//
	// s.last_value is NULL until the first call of nextval. The diff needs that value to
	// decide if a RESTART clause is necessary.
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
	// The definition holds the name of the schema. The diff compares two schemas, so the
	// query removes that prefix from the header.
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

	return views, nil
}

// GetViewColumns returns the columns of the tables and of the views that one view reads.
// The rule of the view holds these dependencies in pg_depend. The type of each column
// comes with them, because a type change makes the view invalid.
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

	// Get columns. information_schema.columns gives the text ARRAY for an array column,
	// and the text USER-DEFINED for an enum column or a composite column. format_type
	// gives the exact type name instead, for example integer[] or the name of the enum
	// type. format_type adds a schema prefix to a type of another schema, so the query
	// removes the prefix of a type that the current schema holds.
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
		var colName, dataType string
		var notNull bool
		var colDefault sql.NullString

		err := columnRows.Scan(&colName, &dataType, &notNull, &colDefault)
		if err != nil {
			return nil, err
		}

		column := &PostgresColumn{
			Name:    colName,
			Type:    dataType,
			NotNull: notNull,
			Default: colDefault,
		}
		table.Columns = append(table.Columns, column)
	}

	err = columnRows.Err()
	if err != nil {
		return nil, err
	}

	// Get constraints
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

	// Get indexes
	indexRows, err := db.QueryContext(ctx, `
			SELECT indexname, indexdef
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

	// Get triggers
	triggerRows, err := db.QueryContext(ctx, `
			SELECT tgname, pg_get_triggerdef(oid)
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
