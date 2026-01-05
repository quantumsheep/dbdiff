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

	subDiff, err := d.DiffTables(ctx)
	if err != nil {
		return "", err
	}
	fmt.Fprintln(&diff, subDiff)

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
			fmt.Fprintf(&diff, "DROP TABLE \"%s\";\n", targetTable.Name)
		}
	}

	subDiff, err := d.DiffViews(ctx)
	if err != nil {
		return "", err
	}
	fmt.Fprintln(&diff, subDiff)

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
			fmt.Fprintf(&diff, "DROP VIEW \"%s\";\n", targetView.Name)
			fmt.Fprintf(&diff, "%s\n", sourceView.String())
		}
	}

	// Removed views
	for _, targetView := range targetViews {
		_, found := lo.Find(sourceViews, func(v *PostgresView) bool {
			return v.Name == targetView.Name
		})

		if !found {
			fmt.Fprintf(&diff, "DROP VIEW \"%s\";\n", targetView.Name)
		}
	}

	return strings.TrimSpace(diff.String()), nil
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
		if err := tableRows.Scan(&tableName); err != nil {
			return nil, err
		}

		table, err := d.GetTable(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
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
		if err := columnRows.Scan(&colName, &dataType, &isNullable, &colDefault); err != nil {
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

	// Get constraints
	constraintRows, err := db.QueryContext(ctx, `
			SELECT conname, contype, pg_get_constraintdef(oid)
			FROM pg_constraint
			WHERE conrelid = $1::regclass
		`, tableName)
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

	// Get indexes
	indexRows, err := db.QueryContext(ctx, `
			SELECT indexname, indexdef
			FROM pg_indexes
			WHERE schemaname = current_schema() AND tablename = $1
			AND indexname NOT IN (
				SELECT conname FROM pg_constraint WHERE conrelid = $1::regclass
			)
		`, tableName)
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

	// Get triggers
	triggerRows, err := db.QueryContext(ctx, `
			SELECT tgname, pg_get_triggerdef(oid)
			FROM pg_trigger
			WHERE tgrelid = $1::regclass AND tgisinternal = false
		`, tableName)
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

	return table, nil
}
