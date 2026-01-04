package drivers

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sort"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/samber/lo"
)

type SQLLiteDriverConfig struct {
	SourceDatabasePath string
	TargetDatabasePath string
}

type SQLiteDriver struct {
	SourceDatabaseConnection *sql.DB
	TargetDatabaseConnection *sql.DB
}

func NewSQLiteDriver(config *SQLLiteDriverConfig) (*SQLiteDriver, error) {
	sourceDatabaseConnection, err := sql.Open("sqlite3", config.SourceDatabasePath)
	if err != nil {
		return nil, err
	}

	targetDatabaseConnection, err := sql.Open("sqlite3", config.TargetDatabasePath)
	if err != nil {
		return nil, err
	}

	driver := &SQLiteDriver{
		SourceDatabaseConnection: sourceDatabaseConnection,
		TargetDatabaseConnection: targetDatabaseConnection,
	}

	return driver, nil
}

func (d *SQLiteDriver) Close() error {
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

func (d *SQLiteDriver) Diff(ctx context.Context) (string, error) {
	var diff strings.Builder

	var subDiff string
	var err error

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

func (d *SQLiteDriver) DiffTables(ctx context.Context) (string, error) {
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
		targetTable, found := lo.Find(targetTables, func(t *SQLiteTable) bool {
			return t.Name == sourceTable.Name
		})

		// Table not found in target database
		if !found {
			fmt.Fprintf(&diff, "%s\n", sourceTable.String())
			continue
		}

		var subDiff string

		subDiff, err = sourceTable.DiffTable(targetTable)
		if err != nil {
			return "", err
		}
		fmt.Fprintln(&diff, subDiff)

		subDiff, err = sourceTable.DiffIndexes(targetTable)
		if err != nil {
			return "", err
		}
		fmt.Fprintln(&diff, subDiff)

		subDiff, err = sourceTable.DiffTriggers(targetTable)
		if err != nil {
			return "", err
		}
		fmt.Fprintln(&diff, subDiff)

	}

	// Removed tables
	for _, targetTable := range targetTables {
		_, found := lo.Find(sourceTables, func(t *SQLiteTable) bool {
			return t.Name == targetTable.Name
		})

		// Table not found in source database
		if !found {
			fmt.Fprintf(&diff, "DROP TABLE \"%s\";\n", targetTable.Name)
		}
	}

	return strings.TrimSpace(diff.String()), nil
}

func (d *SQLiteDriver) DiffViews(ctx context.Context) (string, error) {
	var diff strings.Builder

	sourceViews, err := d.GetViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return "", err
	}

	targetViews, err := d.GetViews(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return "", err
	}

	for _, sourceView := range sourceViews {
		targetView, found := lo.Find(targetViews, func(v *SQLiteView) bool {
			return v.Name == sourceView.Name
		})
		if !found {
			// New view
			fmt.Fprintf(&diff, "%s;\n", sourceView.SQL)
			continue
		}

		subDiff, err := sourceView.Diff(targetView)
		if err != nil {
			return "", err
		}
		fmt.Fprintln(&diff, subDiff)
	}

	for _, targetView := range targetViews {
		_, found := lo.Find(sourceViews, func(v *SQLiteView) bool {
			return v.Name == targetView.Name
		})
		if !found {
			// Removed view
			fmt.Fprintf(&diff, "DROP VIEW \"%s\";\n", targetView.Name)
		}
	}

	return strings.TrimSpace(diff.String()), nil
}

func (d *SQLiteDriver) GetTables(ctx context.Context, db *sql.DB) ([]*SQLiteTable, error) {
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []*SQLiteTable
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}

		columns, err := d.GetTableColumns(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		indexes, err := d.GetTableIndexes(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		triggers, err := d.GetTableTriggers(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		foreignKeys, err := d.GetTableForeignKeys(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		tables = append(tables, &SQLiteTable{
			Name:        tableName,
			Columns:     columns,
			Indexes:     indexes,
			Triggers:    triggers,
			ForeignKeys: foreignKeys,
		})
	}

	return tables, nil
}

func (d *SQLiteDriver) GetTableColumns(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteColumn, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+tableName+");")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []*SQLiteColumn
	for rows.Next() {
		var cid int
		var name string
		var ctype string
		var isNotNull int
		var defaultValue sql.NullString
		var isPrimaryKey int

		if err := rows.Scan(&cid, &name, &ctype, &isNotNull, &defaultValue, &isPrimaryKey); err != nil {
			return nil, err
		}

		columns = append(columns, &SQLiteColumn{
			Name:       name,
			Type:       ctype,
			NotNull:    isNotNull == 1,
			PrimaryKey: isPrimaryKey == 1,
			Default:    defaultValue,
		})
	}

	return columns, nil
}

func (d *SQLiteDriver) GetTableIndexes(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteIndex, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA index_list("+tableName+");")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []*SQLiteIndex
	for rows.Next() {
		var seq int
		var name string
		var isUnique int
		var origin string
		var partial int

		err := rows.Scan(&seq, &name, &isUnique, &origin, &partial)
		if err != nil {
			return nil, err
		}

		columns, err := d.GetIndexColumns(ctx, db, name)
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, &SQLiteIndex{
			Table:   tableName,
			Name:    name,
			Unique:  isUnique == 1,
			Columns: columns,
		})
	}

	return indexes, nil
}

func (d *SQLiteDriver) GetIndexColumns(ctx context.Context, db *sql.DB, indexName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA index_info("+indexName+");")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var seqno int
		var cid int
		var name string

		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, err
		}

		columns = append(columns, name)
	}

	return columns, nil
}

func (d *SQLiteDriver) GetTableTriggers(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteTrigger, error) {
	rows, err := db.QueryContext(ctx, "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?", tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var triggers []*SQLiteTrigger
	for rows.Next() {
		var name, sqlContent string
		if err := rows.Scan(&name, &sqlContent); err != nil {
			return nil, err
		}
		triggers = append(triggers, &SQLiteTrigger{
			Name: name,
			SQL:  sqlContent,
		})
	}
	return triggers, nil
}

func (d *SQLiteDriver) GetViews(ctx context.Context, db *sql.DB) ([]*SQLiteView, error) {
	rows, err := db.QueryContext(ctx, "SELECT name, sql FROM sqlite_master WHERE type = 'view' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []*SQLiteView
	for rows.Next() {
		var name, sqlContent string
		if err := rows.Scan(&name, &sqlContent); err != nil {
			return nil, err
		}
		views = append(views, &SQLiteView{
			Name: name,
			SQL:  sqlContent,
		})
	}
	return views, nil
}

func (d *SQLiteDriver) GetTableForeignKeys(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteForeignKey, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_list("+tableName+");")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	foreignKeysMap := make(map[int]*SQLiteForeignKey)

	for rows.Next() {
		var id, seq int
		var table, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}

		foreignKey, exists := foreignKeysMap[id]
		if !exists {
			foreignKey = &SQLiteForeignKey{
				Table:    table,
				From:     []string{},
				To:       []string{},
				OnUpdate: onUpdate,
				OnDelete: onDelete,
			}
			foreignKeysMap[id] = foreignKey
		}

		foreignKey.From = append(foreignKey.From, from)
		foreignKey.To = append(foreignKey.To, to)
	}

	foreignKeysSet := lo.Values(foreignKeysMap)

	sort.SliceStable(foreignKeysSet, func(i, j int) bool {
		a := foreignKeysSet[i]
		b := foreignKeysSet[j]

		if a.Table != b.Table {
			return a.Table < b.Table
		}

		if !slices.Equal(a.From, b.From) {
			return strings.Join(a.From, ",") < strings.Join(b.From, ",")
		}

		if !slices.Equal(a.To, b.To) {
			return strings.Join(a.To, ",") < strings.Join(b.To, ",")
		}

		if a.OnUpdate != b.OnUpdate {
			return a.OnUpdate < b.OnUpdate
		}

		if a.OnDelete != b.OnDelete {
			return a.OnDelete < b.OnDelete
		}

		return false
	})

	return foreignKeysSet, nil
}
