package drivers

import (
	"context"
	"database/sql"
	"fmt"
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

	sourceTables, err := d.getTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return "", err
	}

	targetTables, err := d.getTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return "", err
	}

	// Added or modified tables
	for _, sourceTable := range sourceTables {
		targetTable, found := lo.Find(targetTables, func(t *SQLiteTable) bool {
			return t.Name == sourceTable.Name
		})

		// Table not found in database 2
		if !found {
			fmt.Fprintf(&diff, "%s\n", sourceTable.String())
			continue
		}

		// Indexes comparison
		for _, sourceIndex := range sourceTable.Indexes {
			targetIndex, found := targetTable.IndexByName(sourceIndex.Name)
			if !found {
				// New index
				fmt.Fprintf(&diff, "%s\n", sourceIndex.String())
				continue
			}

			if !sourceIndex.Equal(targetIndex) {
				// Modified index: drop and recreate
				fmt.Fprintf(&diff, "DROP INDEX \"%s\";\n", targetIndex.Name)
				fmt.Fprintf(&diff, "%s\n", sourceIndex.String())
			}
		}

		for _, targetIndex := range targetTable.Indexes {
			_, found := sourceTable.IndexByName(targetIndex.Name)
			if !found {
				// Removed index
				fmt.Fprintf(&diff, "DROP INDEX \"%s\";\n", targetIndex.Name)
			}
		}

		// Triggers comparison
		for _, sourceTrigger := range sourceTable.Triggers {
			targetTrigger, found := targetTable.TriggerByName(sourceTrigger.Name)
			if !found {
				// New trigger
				fmt.Fprintf(&diff, "%s;\n", sourceTrigger.SQL)
				continue
			}

			if sourceTrigger.SQL != targetTrigger.SQL {
				// Modified trigger: drop and recreate
				fmt.Fprintf(&diff, "DROP TRIGGER \"%s\";\n", targetTrigger.Name)
				fmt.Fprintf(&diff, "%s;\n", sourceTrigger.SQL)
			}
		}

		for _, targetTrigger := range targetTable.Triggers {
			_, found := sourceTable.TriggerByName(targetTrigger.Name)
			if !found {
				// Removed trigger
				fmt.Fprintf(&diff, "DROP TRIGGER \"%s\";\n", targetTrigger.Name)
			}
		}

		addedColumns := []string{}
		modifiedColumns := []string{}
		removedColumns := []string{}
		renamedColumns := make(map[string]string) // oldName -> newName

		for _, sourceColumn := range sourceTable.Columns {
			targetColumn, found := targetTable.ColumnByName(sourceColumn.Name)

			// New column
			if !found {
				// Maybe it's a renamed column?
				renamedColumn, found := lo.Find(targetTable.Columns, func(c *SQLiteColumn) bool {
					_, existsInSourceTable := sourceTable.ColumnByName(c.Name)
					return !existsInSourceTable && c.HasEqualAttributes(sourceColumn)
				})
				if found {
					renamedColumns[renamedColumn.Name] = sourceColumn.Name
					continue
				}

				addedColumns = append(addedColumns, sourceColumn.Name)
				continue
			}

			if *sourceColumn == *targetColumn {
				continue
			}

			// Some modifications should be handled via columns addition/removal
			if sourceColumn.Type != targetColumn.Type {
				addedColumns = append(addedColumns, sourceColumn.Name)
				removedColumns = append(removedColumns, targetColumn.Name)
				continue
			}

			modifiedColumns = append(modifiedColumns, sourceColumn.Name)
		}

		// Removed columns
		for _, column2 := range targetTable.Columns {
			_, found := sourceTable.ColumnByName(column2.Name)
			if !found && !lo.Contains(lo.Keys(renamedColumns), column2.Name) {
				removedColumns = append(removedColumns, column2.Name)
			}
		}

		// Check if foreign keys changed
		foreignKeysChanged := false
		if len(sourceTable.ForeignKeys) != len(targetTable.ForeignKeys) {
			foreignKeysChanged = true
		} else {
			for i, sourceForeignKey := range sourceTable.ForeignKeys {
				targetForeignKey := targetTable.ForeignKeys[i]

				if !sourceForeignKey.Equal(targetForeignKey) {
					foreignKeysChanged = true
					break
				}
			}
		}

		// Modified columns or Foreign Keys need to be handled via table recreation
		if len(modifiedColumns) > 0 || foreignKeysChanged {
			tempTable := sourceTable.Copy()
			tempTable.Name = "_" + sourceTable.Name + "_temp"

			// Create temp table (table only; indexes recreated after rename)
			fmt.Fprintf(&diff, "%s\n", tempTable.StringCreateTable())

			// Reverse rename map: newName -> oldName
			newToOld := make(map[string]string, len(renamedColumns))
			for oldName, newName := range renamedColumns {
				newToOld[newName] = oldName
			}

			// Build INSERT column list (new schema) and SELECT expressions (from old schema)
			var insertColumns []string
			var selectColumns []string

			for _, newCol := range sourceTable.Columns {
				insertColumns = append(insertColumns, fmt.Sprintf("\"%s\"", newCol.Name))

				// If the column existed before (same name), copy from old table
				if _, ok := targetTable.ColumnByName(newCol.Name); ok {
					selectColumns = append(selectColumns, fmt.Sprintf("\"%s\"", newCol.Name))
					continue
				}

				// If it was renamed, copy from old name
				if oldName, ok := newToOld[newCol.Name]; ok {
					selectColumns = append(selectColumns, fmt.Sprintf("\"%s\"", oldName))
					continue
				}

				// Otherwise it is a new column: use DEFAULT if present, else NULL
				if newCol.Default.Valid {
					selectColumns = append(selectColumns, newCol.Default.String)
				} else {
					selectColumns = append(selectColumns, "NULL")
				}
			}

			// Copy data from old table to new temp table with explicit mapping
			fmt.Fprintf(
				&diff,
				"INSERT INTO \"%s\" (%s) SELECT %s FROM \"%s\";\n",
				tempTable.Name,
				strings.Join(insertColumns, ", "),
				strings.Join(selectColumns, ", "),
				sourceTable.Name,
			)

			// Drop old table
			fmt.Fprintf(&diff, "DROP TABLE \"%s\";\n", sourceTable.Name)

			// Rename new table to old table's name
			fmt.Fprintf(&diff, "ALTER TABLE \"%s\" RENAME TO \"%s\";\n", tempTable.Name, sourceTable.Name)

			// Recreate indexes (on final table name)
			for _, idx := range sourceTable.Indexes {
				fmt.Fprintf(&diff, "%s\n", idx.String())
			}
		} else {
			for oldName, newName := range renamedColumns {
				fmt.Fprintf(&diff, "ALTER TABLE \"%s\" RENAME COLUMN \"%s\" TO \"%s\";\n", sourceTable.Name, oldName, newName)
			}

			for _, columnName := range addedColumns {
				column, ok := sourceTable.ColumnByName(columnName)
				if !ok {
					return "", fmt.Errorf("internal error: added column %s not found in table %s", columnName, sourceTable.Name)
				}

				fmt.Fprintf(&diff, "ALTER TABLE \"%s\" ADD COLUMN %s;\n", sourceTable.Name, column.String())
			}

			for _, columnName := range removedColumns {
				fmt.Fprintf(&diff, "ALTER TABLE \"%s\" DROP COLUMN \"%s\";\n", sourceTable.Name, columnName)
			}
		}
	}

	// Removed tables
	for _, table2 := range targetTables {
		_, found := lo.Find(sourceTables, func(t *SQLiteTable) bool {
			return t.Name == table2.Name
		})

		if !found {
			fmt.Fprintf(&diff, "DROP TABLE \"%s\";\n", table2.Name)
		}
	}

	// Views comparison
	sourceViews, err := d.getViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return "", err
	}

	targetViews, err := d.getViews(ctx, d.TargetDatabaseConnection)
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

		if sourceView.SQL != targetView.SQL {
			// Modified view
			fmt.Fprintf(&diff, "DROP VIEW \"%s\";\n", targetView.Name)
			fmt.Fprintf(&diff, "%s;\n", sourceView.SQL)
		}
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

func (d *SQLiteDriver) getTables(ctx context.Context, db *sql.DB) ([]*SQLiteTable, error) {
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

		columns, err := d.getTableColumns(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		indexes, err := d.getTableIndexes(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		triggers, err := d.getTableTriggers(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		foreignKeys, err := d.getTableForeignKeys(ctx, db, tableName)
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

func (d *SQLiteDriver) getTableColumns(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteColumn, error) {
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

func (d *SQLiteDriver) getTableIndexes(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteIndex, error) {
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

		columns, err := d.getIndexColumns(ctx, db, name)
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

func (d *SQLiteDriver) getIndexColumns(ctx context.Context, db *sql.DB, indexName string) ([]string, error) {
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

func (d *SQLiteDriver) getTableTriggers(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteTrigger, error) {
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

func (d *SQLiteDriver) getViews(ctx context.Context, db *sql.DB) ([]*SQLiteView, error) {
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

func (d *SQLiteDriver) getTableForeignKeys(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteForeignKey, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_list("+tableName+");")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	foreignKeys := make(map[int]*SQLiteForeignKey)

	for rows.Next() {
		var id, seq int
		var table, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}

		foreignKey, exists := foreignKeys[id]
		if !exists {
			foreignKey = &SQLiteForeignKey{
				Table:    table,
				From:     []string{},
				To:       []string{},
				OnUpdate: onUpdate,
				OnDelete: onDelete,
			}
			foreignKeys[id] = foreignKey
		}

		foreignKey.From = append(foreignKey.From, from)
		foreignKey.To = append(foreignKey.To, to)
	}

	return lo.Values(foreignKeys), nil
}
