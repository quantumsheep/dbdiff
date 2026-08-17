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
	CompareData        bool
}

type SQLiteDriver struct {
	SourceDatabaseConnection *sql.DB
	TargetDatabaseConnection *sql.DB
	CompareData              bool
}

func NewSQLiteDriver(config *SQLLiteDriverConfig) (*SQLiteDriver, error) {
	sourceDatabasePath := strings.TrimPrefix(config.SourceDatabasePath, "sqlite://")
	targetDatabasePath := strings.TrimPrefix(config.TargetDatabasePath, "sqlite://")

	sourceDatabaseConnection, err := sql.Open("sqlite3", sourceDatabasePath)
	if err != nil {
		return nil, err
	}

	targetDatabaseConnection, err := sql.Open("sqlite3", targetDatabasePath)
	if err != nil {
		return nil, err
	}

	driver := &SQLiteDriver{
		SourceDatabaseConnection: sourceDatabaseConnection,
		TargetDatabaseConnection: targetDatabaseConnection,
		CompareData:              config.CompareData,
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

func (d *SQLiteDriver) Diff(ctx context.Context) ([]Instruction, error) {
	var instructions []Instruction

	tableInstructions, err := d.DiffTables(ctx)
	if err != nil {
		return nil, err
	}

	instructions = append(instructions, tableInstructions...)

	viewInstructions, err := d.DiffViews(ctx)
	if err != nil {
		return nil, err
	}

	instructions = append(instructions, viewInstructions...)

	if d.CompareData {
		dataInstructions, err := d.DiffData(ctx)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, dataInstructions...)
	}

	return instructions, nil
}

func (d *SQLiteDriver) DiffTables(ctx context.Context) ([]Instruction, error) {
	var instructions []Instruction

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceTable := range sourceTables {
		targetTable, found := lo.Find(targetTables, func(table *SQLiteTable) bool {
			return table.Name == sourceTable.Name
		})
		if !found {
			instructions = append(instructions, sourceTable.Instructions()...)
			continue
		}

		tableInstructions, err := sourceTable.DiffTable(targetTable)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, tableInstructions...)

		indexInstructions, err := sourceTable.DiffIndexes(targetTable)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, indexInstructions...)

		triggerInstructions, err := sourceTable.DiffTriggers(targetTable)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, triggerInstructions...)
	}

	for _, targetTable := range targetTables {
		_, found := lo.Find(sourceTables, func(table *SQLiteTable) bool {
			return table.Name == targetTable.Name
		})
		if !found {
			instructions = append(instructions, &SQLDropTableInstruction{Name: targetTable.Name})
		}
	}

	return instructions, nil
}

func (d *SQLiteDriver) DiffViews(ctx context.Context) ([]Instruction, error) {
	var instructions []Instruction

	sourceViews, err := d.GetViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetViews, err := d.GetViews(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, sourceView := range sourceViews {
		targetView, found := lo.Find(targetViews, func(view *SQLiteView) bool {
			return view.Name == sourceView.Name
		})
		if !found {
			instructions = append(instructions, &SQLiteCreateViewInstruction{
				Definition: sourceView.SQL,
			})

			continue
		}

		subInstructions, err := sourceView.Diff(targetView)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, subInstructions...)
	}

	for _, targetView := range targetViews {
		_, found := lo.Find(sourceViews, func(view *SQLiteView) bool {
			return view.Name == targetView.Name
		})
		if !found {
			instructions = append(instructions, &SQLDropViewInstruction{Name: targetView.Name})
		}
	}

	return instructions, nil
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

		err := rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}

		table, err := d.GetTable(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return tables, nil
}

func (d *SQLiteDriver) GetTable(ctx context.Context, db *sql.DB, tableName string) (*SQLiteTable, error) {
	columns, err := d.GetTableColumns(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	primaryKey, err := d.GetTablePrimaryKey(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	uniqueKeys, err := d.GetTableUniqueKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	// A UNIQUE constraint of one column belongs to the definition of that column.
	// A constraint of two or more columns is a table constraint.
	var uniqueConstraints [][]string

	for _, key := range uniqueKeys {
		if len(key) != 1 {
			uniqueConstraints = append(uniqueConstraints, key)
			continue
		}

		matchingColumn, found := lo.Find(columns, func(column *SQLiteColumn) bool {
			return column.Name == key[0]
		})
		if found {
			matchingColumn.Unique = true
		}
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

	return &SQLiteTable{
		Name:              tableName,
		Columns:           columns,
		PrimaryKey:        primaryKey,
		UniqueConstraints: uniqueConstraints,
		Indexes:           indexes,
		Triggers:          triggers,
		ForeignKeys:       foreignKeys,
	}, nil
}

func (d *SQLiteDriver) GetTableColumns(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteColumn, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+quoteIdentifier(tableName)+");")
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var columns []*SQLiteColumn
	var primaryKeyColumns []*SQLiteColumn

	for rows.Next() {
		var columnID int
		var name string
		var columnType string
		var isNotNull int
		var defaultValue sql.NullString
		var primaryKeyPosition int

		err := rows.Scan(&columnID, &name, &columnType, &isNotNull, &defaultValue, &primaryKeyPosition)
		if err != nil {
			return nil, err
		}

		column := &SQLiteColumn{
			Name:    name,
			Type:    columnType,
			NotNull: isNotNull == 1,
			Default: defaultValue,
		}

		columns = append(columns, column)

		if primaryKeyPosition > 0 {
			primaryKeyColumns = append(primaryKeyColumns, column)
		}
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	// A key of one column stays a column constraint, which keeps INTEGER PRIMARY KEY as the
	// alias of the rowid. GetTablePrimaryKey reads a key of two or more columns.
	if len(primaryKeyColumns) == 1 {
		primaryKeyColumns[0].PrimaryKey = true
	}

	return columns, nil
}

// GetTablePrimaryKey returns the columns of a primary key of two or more columns, in the
// order of the key. A key of one column gives an empty list.
func (d *SQLiteDriver) GetTablePrimaryKey(ctx context.Context, db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+quoteIdentifier(tableName)+");")
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	type primaryKeyColumn struct {
		Name     string
		Position int
	}

	var keyColumns []primaryKeyColumn

	for rows.Next() {
		var columnID int
		var name string
		var columnType string
		var isNotNull int
		var defaultValue sql.NullString
		var primaryKeyPosition int

		err := rows.Scan(&columnID, &name, &columnType, &isNotNull, &defaultValue, &primaryKeyPosition)
		if err != nil {
			return nil, err
		}

		if primaryKeyPosition == 0 {
			continue
		}

		keyColumns = append(keyColumns, primaryKeyColumn{
			Name:     name,
			Position: primaryKeyPosition,
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	if len(keyColumns) < 2 {
		return nil, nil
	}

	sort.SliceStable(keyColumns, func(i, j int) bool {
		return keyColumns[i].Position < keyColumns[j].Position
	})

	names := lo.Map(keyColumns, func(keyColumn primaryKeyColumn, _ int) string {
		return keyColumn.Name
	})

	return names, nil
}

// GetTableUniqueKeys returns the columns of each UNIQUE constraint of a table. It sorts the
// keys, because SQLite gives no stable order.
func (d *SQLiteDriver) GetTableUniqueKeys(ctx context.Context, db *sql.DB, tableName string) ([][]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA index_list("+quoteIdentifier(tableName)+");")
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var indexNames []string

	for rows.Next() {
		var position int
		var name string
		var isUnique int
		var origin string
		var partial int

		err := rows.Scan(&position, &name, &isUnique, &origin, &partial)
		if err != nil {
			return nil, err
		}

		if origin != "u" {
			continue
		}

		indexNames = append(indexNames, name)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	var keys [][]string

	for _, indexName := range indexNames {
		columnNames, err := d.GetIndexColumnNames(ctx, db, indexName)
		if err != nil {
			return nil, err
		}

		keys = append(keys, columnNames)
	}

	sort.SliceStable(keys, func(i, j int) bool {
		return strings.Join(keys[i], ",") < strings.Join(keys[j], ",")
	})

	return keys, nil
}

func (d *SQLiteDriver) GetIndexColumnNames(ctx context.Context, db *sql.DB, indexName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA index_info("+quoteIdentifier(indexName)+");")
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var names []string

	for rows.Next() {
		var keyPosition int
		var columnID int
		var name sql.NullString

		err := rows.Scan(&keyPosition, &columnID, &name)
		if err != nil {
			return nil, err
		}

		if !name.Valid {
			return nil, fmt.Errorf("the index %s holds a key at the position %d that is no column", indexName, keyPosition)
		}

		names = append(names, name.String)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return names, nil
}

func (d *SQLiteDriver) GetTableIndexes(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteIndex, error) {
	definitions, err := d.GetIndexDefinitions(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx, "PRAGMA index_list("+quoteIdentifier(tableName)+");")
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var indexes []*SQLiteIndex

	for rows.Next() {
		var position int
		var name string
		var isUnique int
		var origin string
		var partial int

		err := rows.Scan(&position, &name, &isUnique, &origin, &partial)
		if err != nil {
			return nil, err
		}

		// The origin "c" marks an index that a CREATE INDEX statement built. The column
		// definition already prints the index of a UNIQUE constraint or of a PRIMARY KEY.
		if origin != "c" {
			continue
		}

		definitionKeys, condition := parseIndexDefinition(definitions[name])

		keys, err := d.GetIndexKeys(ctx, db, name, definitionKeys)
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, &SQLiteIndex{
			Table:  tableName,
			Name:   name,
			Unique: isUnique == 1,
			Keys:   keys,
			Where:  condition,
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return indexes, nil
}

// GetIndexDefinitions returns the text of each index of a table. An index that a UNIQUE
// constraint or a PRIMARY KEY builds holds no text, and the map holds no entry for it.
func (d *SQLiteDriver) GetIndexDefinitions(ctx context.Context, db *sql.DB, tableName string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL", tableName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	definitions := make(map[string]string)

	for rows.Next() {
		var name, definition string

		err := rows.Scan(&name, &definition)
		if err != nil {
			return nil, err
		}

		definitions[name] = definition
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return definitions, nil
}

// GetIndexKeys returns the SQL text of each key part of an index. PRAGMA index_info gives
// no name for a key that an expression builds, so that key comes from definitionKeys.
func (d *SQLiteDriver) GetIndexKeys(ctx context.Context, db *sql.DB, indexName string, definitionKeys []string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA index_info("+quoteIdentifier(indexName)+");")
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var keys []string

	for rows.Next() {
		var keyPosition int
		var columnID int
		var name sql.NullString

		err := rows.Scan(&keyPosition, &columnID, &name)
		if err != nil {
			return nil, err
		}

		if name.Valid {
			keys = append(keys, quoteIdentifier(name.String))
			continue
		}

		if keyPosition < 0 || keyPosition >= len(definitionKeys) {
			return nil, fmt.Errorf("the definition of the index %s holds no key at the position %d", indexName, keyPosition)
		}

		keys = append(keys, definitionKeys[keyPosition])
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return keys, nil
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

		err := rows.Scan(&name, &sqlContent)
		if err != nil {
			return nil, err
		}

		triggers = append(triggers, &SQLiteTrigger{
			Name: name,
			SQL:  sqlContent,
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
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

		err := rows.Scan(&name, &sqlContent)
		if err != nil {
			return nil, err
		}

		views = append(views, &SQLiteView{
			Name: name,
			SQL:  sqlContent,
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return views, nil
}

func (d *SQLiteDriver) GetTableForeignKeys(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteForeignKey, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_list("+quoteIdentifier(tableName)+");")
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	foreignKeysMap := make(map[int]*SQLiteForeignKey)

	for rows.Next() {
		var foreignKeyID, keyPosition int
		var table, from, to, onUpdate, onDelete, match string

		err := rows.Scan(&foreignKeyID, &keyPosition, &table, &from, &to, &onUpdate, &onDelete, &match)
		if err != nil {
			return nil, err
		}

		foreignKey, exists := foreignKeysMap[foreignKeyID]
		if !exists {
			foreignKey = &SQLiteForeignKey{
				Table:    table,
				From:     []string{},
				To:       []string{},
				OnUpdate: onUpdate,
				OnDelete: onDelete,
			}
			foreignKeysMap[foreignKeyID] = foreignKey
		}

		foreignKey.From = append(foreignKey.From, from)
		foreignKey.To = append(foreignKey.To, to)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	foreignKeysSet := lo.Values(foreignKeysMap)

	sort.SliceStable(foreignKeysSet, func(i, j int) bool {
		first := foreignKeysSet[i]
		second := foreignKeysSet[j]
		if first.Table != second.Table {
			return first.Table < second.Table
		}

		if !slices.Equal(first.From, second.From) {
			return strings.Join(first.From, ",") < strings.Join(second.From, ",")
		}

		if !slices.Equal(first.To, second.To) {
			return strings.Join(first.To, ",") < strings.Join(second.To, ",")
		}

		if first.OnUpdate != second.OnUpdate {
			return first.OnUpdate < second.OnUpdate
		}

		if first.OnDelete != second.OnDelete {
			return first.OnDelete < second.OnDelete
		}

		return false
	})

	return foreignKeysSet, nil
}
