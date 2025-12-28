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

var _ Driver = (*SQLiteDriver)(nil)

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
				fmt.Fprintf(&diff, "DROP INDEX %s;\n", targetIndex.Name)
				fmt.Fprintf(&diff, "%s\n", sourceIndex.String())
			}
		}

		for _, targetIndex := range targetTable.Indexes {
			_, found := sourceTable.IndexByName(targetIndex.Name)
			if !found {
				// Removed index
				fmt.Fprintf(&diff, "DROP INDEX %s;\n", targetIndex.Name)
			}
		}

		addedColumns := []string{}
		modifiedColumns := []string{}
		removedColumns := []string{}
		renamedColumns := make(map[string]string)

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

		// Modified columns need to be handled via table recreation
		if len(modifiedColumns) > 0 {
			tempTable := sourceTable.Copy()
			tempTable.Name = "_" + sourceTable.Name + "_temp"

			fmt.Fprintf(&diff, "%s\n", tempTable.String())

			// Copy data from old table to new table
			fmt.Fprintf(&diff, "INSERT INTO _%s_temp SELECT ", sourceTable.Name)
			var columnNames []string
			for _, column := range targetTable.Columns {
				newName, isRenamed := renamedColumns[column.Name]
				if isRenamed {
					columnNames = append(columnNames, newName)
					continue
				}

				columnNames = append(columnNames, column.Name)
			}
			fmt.Fprintf(&diff, "%s FROM %s;\n", strings.Join(columnNames, ", "), sourceTable.Name)

			// Drop old table
			fmt.Fprintf(&diff, "DROP TABLE %s;\n", sourceTable.Name)

			// Rename new table to old table's name
			fmt.Fprintf(&diff, "ALTER TABLE _%s_temp RENAME TO %s;\n", sourceTable.Name, sourceTable.Name)
		} else {
			for oldName, newName := range renamedColumns {
				fmt.Fprintf(&diff, "ALTER TABLE %s RENAME COLUMN %s TO %s;\n", sourceTable.Name, oldName, newName)
			}

			for _, columnName := range addedColumns {
				column, ok := sourceTable.ColumnByName(columnName)
				if !ok {
					return "", fmt.Errorf("internal error: added column %s not found in table %s", columnName, sourceTable.Name)
				}

				fmt.Fprintf(&diff, "ALTER TABLE %s ADD COLUMN %s;\n", sourceTable.Name, column.String())
			}

			for _, columnName := range removedColumns {
				fmt.Fprintf(&diff, "ALTER TABLE %s DROP COLUMN %s;\n", sourceTable.Name, columnName)
			}
		}
	}

	// Removed tables
	for _, table2 := range targetTables {
		_, found := lo.Find(sourceTables, func(t *SQLiteTable) bool {
			return t.Name == table2.Name
		})

		if !found {
			fmt.Fprintf(&diff, "DROP TABLE %s;\n", table2.Name)
		}
	}

	return strings.TrimSpace(diff.String()), nil
}

type SQLiteTable struct {
	Name    string
	Columns []*SQLiteColumn
	Indexes []*SQLiteIndex
}

func (t *SQLiteTable) Copy() *SQLiteTable {
	new := *t
	return &new
}

func (t *SQLiteTable) ColumnByName(name string) (*SQLiteColumn, bool) {
	for _, column := range t.Columns {
		if column.Name == name {
			return column, true
		}
	}
	return nil, false
}

func (t *SQLiteTable) IndexByName(name string) (*SQLiteIndex, bool) {
	for _, index := range t.Indexes {
		if index.Name == name {
			return index, true
		}
	}
	return nil, false
}

func (t *SQLiteTable) String() string {
	var columnLines []string
	for _, column := range t.Columns {
		line := "\t" + column.String()
		columnLines = append(columnLines, line)
	}

	createTableColumns := strings.Join(columnLines, ",\n")
	createTable := fmt.Sprintf("CREATE TABLE %s (\n%s\n);", t.Name, createTableColumns)

	var createIndexes []string
	for _, index := range t.Indexes {
		createIndexes = append(createIndexes, index.String())
	}

	if len(createIndexes) > 0 {
		createTable += "\n" + strings.Join(createIndexes, "\n")
	}

	return createTable
}

type SQLiteColumn struct {
	Name       string
	Type       string
	NotNull    bool
	PrimaryKey bool
	Default    sql.NullString
}

func (c *SQLiteColumn) Copy() *SQLiteColumn {
	new := *c
	return &new
}

func (c *SQLiteColumn) HasEqualAttributes(other *SQLiteColumn) bool {
	copy := c.Copy()
	copy.Name = other.Name

	return *copy == *other
}

func (c *SQLiteColumn) String() string {
	value := fmt.Sprintf("%s %s", c.Name, c.Type)
	if c.NotNull {
		value += " NOT NULL"
	}
	if c.PrimaryKey {
		value += " PRIMARY KEY"
	}
	if c.Default.Valid {
		value += fmt.Sprintf(" DEFAULT %s", c.Default.String)
	}

	return value
}

type SQLiteIndex struct {
	Table   string
	Name    string
	Columns []string
	Unique  bool
}

func (i *SQLiteIndex) Equal(other *SQLiteIndex) bool {
	if i.Name != other.Name || i.Table != other.Table || i.Unique != other.Unique {
		return false
	}

	if len(i.Columns) != len(other.Columns) {
		return false
	}

	for idx, col := range i.Columns {
		if col != other.Columns[idx] {
			return false
		}
	}

	return true
}

func (i *SQLiteIndex) String() string {
	createIndex := "CREATE "
	if i.Unique {
		createIndex += "UNIQUE "
	}

	columns := strings.Join(i.Columns, ", ")

	createIndex += fmt.Sprintf("INDEX %s ON %s (%s);", i.Name, i.Table, columns)

	return createIndex
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

		tables = append(tables, &SQLiteTable{
			Name:    tableName,
			Columns: columns,
			Indexes: indexes,
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
