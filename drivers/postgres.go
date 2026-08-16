package drivers

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/samber/lo"
)

type PostgresDriverConfig struct {
	SourceConnectionString string
	TargetConnectionString string
}

type PostgresDriver struct {
	SourceDatabaseConnection *sql.DB
	TargetDatabaseConnection *sql.DB
}

func NewPostgresDriver(config *PostgresDriverConfig) (*PostgresDriver, error) {
	sourceDatabaseConnection, err := sql.Open("pgx", config.SourceConnectionString)
	if err != nil {
		return nil, err
	}

	targetDatabaseConnection, err := sql.Open("pgx", config.TargetConnectionString)
	if err != nil {
		return nil, err
	}

	driver := &PostgresDriver{
		SourceDatabaseConnection: sourceDatabaseConnection,
		TargetDatabaseConnection: targetDatabaseConnection,
	}

	return driver, nil
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
	var diff strings.Builder

	var subDiff string
	var err error

	// A table can use an extension, a type, a sequence, or a function, so these come
	// first.
	subDiff, err = d.DiffExtensions(ctx)
	if err != nil {
		return "", err
	}

	fmt.Fprintln(&diff, subDiff)

	subDiff, err = d.DiffTypes(ctx)
	if err != nil {
		return "", err
	}

	fmt.Fprintln(&diff, subDiff)

	subDiff, err = d.DiffSequences(ctx)
	if err != nil {
		return "", err
	}

	fmt.Fprintln(&diff, subDiff)

	subDiff, err = d.DiffFunctions(ctx)
	if err != nil {
		return "", err
	}

	fmt.Fprintln(&diff, subDiff)

	subDiff, err = d.DiffTables(ctx)
	if err != nil {
		return "", err
	}

	fmt.Fprintln(&diff, subDiff)

	subDiff, err = d.DiffViews(ctx)
	if err != nil {
		return "", err
	}

	fmt.Fprintln(&diff, subDiff)

	return strings.TrimSpace(diff.String()), nil
}

func (d *PostgresDriver) DiffExtensions(ctx context.Context) (string, error) {
	var diff strings.Builder

	sourceExtensions, err := d.GetExtensions(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return "", err
	}

	targetExtensions, err := d.GetExtensions(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return "", err
	}

	for _, sourceExtension := range sourceExtensions {
		targetExtension, found := lo.Find(targetExtensions, func(e *PostgresExtension) bool {
			return e.Name == sourceExtension.Name
		})

		if !found {
			fmt.Fprintf(&diff, "%s\n", sourceExtension.String())
			continue
		}

		if sourceExtension.Version != targetExtension.Version {
			fmt.Fprintf(&diff, "%s\n", sourceExtension.StringUpdate())
		}
	}

	for _, targetExtension := range targetExtensions {
		_, found := lo.Find(sourceExtensions, func(e *PostgresExtension) bool {
			return e.Name == targetExtension.Name
		})

		if !found {
			fmt.Fprintf(&diff, "%s\n", targetExtension.StringDrop())
		}
	}

	return strings.TrimSpace(diff.String()), nil
}

func (d *PostgresDriver) DiffTypes(ctx context.Context) (string, error) {
	var diff strings.Builder

	sourceTypes, err := d.GetTypes(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return "", err
	}

	targetTypes, err := d.GetTypes(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return "", err
	}

	for _, sourceType := range sourceTypes {
		targetType, found := lo.Find(targetTypes, func(t *PostgresType) bool {
			return t.Name == sourceType.Name
		})

		if !found {
			fmt.Fprintf(&diff, "%s\n", sourceType.String())
			continue
		}

		subDiff := sourceType.Diff(targetType)
		if subDiff != "" {
			fmt.Fprintf(&diff, "%s\n", subDiff)
		}
	}

	for _, targetType := range targetTypes {
		_, found := lo.Find(sourceTypes, func(t *PostgresType) bool {
			return t.Name == targetType.Name
		})

		if !found {
			fmt.Fprintf(&diff, "DROP TYPE %s;\n", quoteIdentifier(targetType.Name))
		}
	}

	return strings.TrimSpace(diff.String()), nil
}

func (d *PostgresDriver) DiffSequences(ctx context.Context) (string, error) {
	var diff strings.Builder

	sourceSequences, err := d.GetSequences(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return "", err
	}

	targetSequences, err := d.GetSequences(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return "", err
	}

	for _, sourceSequence := range sourceSequences {
		targetSequence, found := lo.Find(targetSequences, func(s *PostgresSequence) bool {
			return s.Name == sourceSequence.Name
		})

		if !found {
			fmt.Fprintf(&diff, "%s\n", sourceSequence.String())
			continue
		}

		subDiff := sourceSequence.Diff(targetSequence)
		if subDiff != "" {
			fmt.Fprintf(&diff, "%s\n", subDiff)
		}
	}

	for _, targetSequence := range targetSequences {
		_, found := lo.Find(sourceSequences, func(s *PostgresSequence) bool {
			return s.Name == targetSequence.Name
		})

		if !found {
			fmt.Fprintf(&diff, "DROP SEQUENCE %s;\n", quoteIdentifier(targetSequence.Name))
		}
	}

	return strings.TrimSpace(diff.String()), nil
}

func (d *PostgresDriver) DiffFunctions(ctx context.Context) (string, error) {
	var diff strings.Builder

	sourceFunctions, err := d.GetFunctions(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return "", err
	}

	targetFunctions, err := d.GetFunctions(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return "", err
	}

	// The definition of PostgreSQL starts with CREATE OR REPLACE FUNCTION, so a new
	// function and a modified function take the same statement.
	for _, sourceFunction := range sourceFunctions {
		targetFunction, found := lo.Find(targetFunctions, func(f *PostgresFunction) bool {
			return f.Signature() == sourceFunction.Signature()
		})

		if !found || sourceFunction.Def != targetFunction.Def {
			fmt.Fprintf(&diff, "%s\n", sourceFunction.String())
		}
	}

	for _, targetFunction := range targetFunctions {
		_, found := lo.Find(sourceFunctions, func(f *PostgresFunction) bool {
			return f.Signature() == targetFunction.Signature()
		})

		if !found {
			fmt.Fprintf(&diff, "%s\n", targetFunction.StringDrop())
		}
	}

	return strings.TrimSpace(diff.String()), nil
}

func (d *PostgresDriver) DiffTables(ctx context.Context) (string, error) {
	var diff strings.Builder

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return "", err
	}

	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return "", err
	}

	// Added or modified tables
	for _, sourceTable := range sourceTables {
		targetTable, found := lo.Find(targetTables, func(t *PostgresTable) bool {
			return t.Name == sourceTable.Name
		})

		// Table not found in target database
		if !found {
			fmt.Fprintf(&diff, "%s\n", sourceTable.String())
			continue
		}

		subDiff, err := sourceTable.DiffTable(targetTable)
		if err != nil {
			return "", err
		}

		fmt.Fprintln(&diff, subDiff)
	}

	// Removed tables
	for _, targetTable := range targetTables {
		_, found := lo.Find(sourceTables, func(t *PostgresTable) bool {
			return t.Name == targetTable.Name
		})

		// Table not found in source database
		if !found {
			fmt.Fprintf(&diff, "DROP TABLE %s;\n", quoteIdentifier(targetTable.Name))
		}
	}

	return strings.TrimSpace(diff.String()), nil
}

func (d *PostgresDriver) DiffViews(ctx context.Context) (string, error) {
	var diff strings.Builder

	sourceViews, err := d.GetViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return "", err
	}

	targetViews, err := d.GetViews(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return "", err
	}

	// Added or modified views
	for _, sourceView := range sourceViews {
		targetView, found := lo.Find(targetViews, func(v *PostgresView) bool {
			return v.Name == sourceView.Name
		})

		if !found {
			fmt.Fprintf(&diff, "%s\n", sourceView.String())
			continue
		}

		if sourceView.Def != targetView.Def {
			fmt.Fprintf(&diff, "DROP VIEW %s;\n", quoteIdentifier(targetView.Name))
			fmt.Fprintf(&diff, "%s\n", sourceView.String())
		}
	}

	// Removed views
	for _, targetView := range targetViews {
		_, found := lo.Find(sourceViews, func(v *PostgresView) bool {
			return v.Name == targetView.Name
		})

		if !found {
			fmt.Fprintf(&diff, "DROP VIEW %s;\n", quoteIdentifier(targetView.Name))
		}
	}

	return strings.TrimSpace(diff.String()), nil
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

func (d *PostgresDriver) GetSequences(ctx context.Context, db *sql.DB) ([]*PostgresSequence, error) {
	// A serial column and an identity column own their sequence. The table creates it,
	// so the diff of the sequences leaves it out.
	rows, err := db.QueryContext(ctx, `
		SELECT s.sequencename, s.data_type::text, s.start_value, s.min_value, s.max_value, s.increment_by, s.cycle
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

		err := rows.Scan(&function.Name, &function.Arguments, &function.Def)
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

	return views, nil
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

	// Get columns
	columnRows, err := db.QueryContext(ctx, `
			SELECT column_name, data_type, is_nullable, column_default
			FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1
			ORDER BY ordinal_position
		`, tableName)
	if err != nil {
		return nil, err
	}

	defer columnRows.Close()

	for columnRows.Next() {
		var colName, dataType, isNullable string
		var colDefault sql.NullString

		err := columnRows.Scan(&colName, &dataType, &isNullable, &colDefault)
		if err != nil {
			return nil, err
		}

		column := &PostgresColumn{
			Name:    colName,
			Type:    dataType,
			NotNull: isNullable == "NO",
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
