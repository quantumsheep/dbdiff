package drivers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/samber/lo"
)

type SQLiteDriverConfig struct {
	TargetDatabasePath string
	SourceDatabasePath string
	CompareData        bool
}

type SQLiteDriver struct {
	TargetDatabaseConnection *sql.DB
	SourceDatabaseConnection *sql.DB
	CompareData              bool

	temporaryDirectory string
}

func NewSQLiteDriver(ctx context.Context, config *SQLiteDriverConfig) (*SQLiteDriver, error) {
	driver := &SQLiteDriver{
		CompareData: config.CompareData,
	}

	targetDatabaseConnection, err := driver.OpenSide(ctx, config.TargetDatabasePath, "target")
	if err != nil {
		_ = driver.RemoveTemporaryDirectory()
		return nil, err
	}

	driver.TargetDatabaseConnection = targetDatabaseConnection

	sourceDatabaseConnection, err := driver.OpenSide(ctx, config.SourceDatabasePath, "source")
	if err != nil {
		_ = driver.TargetDatabaseConnection.Close()
		_ = driver.RemoveTemporaryDirectory()

		return nil, err
	}

	driver.SourceDatabaseConnection = sourceDatabaseConnection

	return driver, nil
}

func TrimSQLitePrefix(path string) string {
	return strings.TrimPrefix(path, "sqlite://")
}

func (d *SQLiteDriver) Close() error {
	targetError := d.TargetDatabaseConnection.Close()
	sourceError := d.SourceDatabaseConnection.Close()
	removeError := d.RemoveTemporaryDirectory()

	return FirstError(targetError, sourceError, removeError)
}

func (d *SQLiteDriver) Diff(ctx context.Context) ([]Instruction, error) {
	var instructions []Instruction

	tableInstructions, err := d.DiffTables(ctx)
	if err != nil {
		return nil, err
	}

	instructions = append(instructions, tableInstructions...)

	virtualTableInstructions, err := d.DiffVirtualTables(ctx)
	if err != nil {
		return nil, err
	}

	instructions = append(instructions, virtualTableInstructions...)

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

	// SQLite refuses the DROP TABLE of a recreation while another table names the
	// dropped table and the enforcement of the foreign keys is on.
	if holdsTableRecreation(instructions) {
		instructions = append([]Instruction{&SQLitePragmaForeignKeysInstruction{}}, instructions...)
		instructions = append(instructions, &SQLitePragmaForeignKeysInstruction{Enabled: true})
	}

	return instructions, nil
}

func holdsTableRecreation(instructions []Instruction) bool {
	for index := range instructions {
		name, _ := tableRecreationAt(instructions, index)
		if name != "" {
			return true
		}
	}

	return false
}

func (d *SQLiteDriver) DiffTables(ctx context.Context) ([]Instruction, error) {
	var instructions []Instruction

	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, targetTable := range targetTables {
		sourceTable, found := lo.Find(sourceTables, func(table *SQLiteTable) bool {
			return table.Name == targetTable.Name
		})
		if !found {
			instructions = append(instructions, targetTable.Instructions()...)
			continue
		}

		tableInstructions, err := targetTable.DiffTable(sourceTable)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, tableInstructions...)
	}

	// The reverse order gives each DROP TABLE statement before the table that it names.
	for _, sourceTable := range slices.Backward(sourceTables) {
		_, found := lo.Find(targetTables, func(table *SQLiteTable) bool {
			return table.Name == sourceTable.Name
		})
		if !found {
			instructions = append(instructions, &SQLDropTableInstruction{Name: sourceTable.Name})
		}
	}

	return instructions, nil
}

func (d *SQLiteDriver) DiffViews(ctx context.Context) ([]Instruction, error) {
	var instructions []Instruction

	targetViews, err := d.GetViews(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceViews, err := d.GetViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, targetView := range targetViews {
		sourceView, found := lo.Find(sourceViews, func(view *SQLiteView) bool {
			return view.Name == targetView.Name
		})
		if !found {
			instructions = append(instructions, targetView.Instructions()...)
			continue
		}

		subInstructions, err := targetView.Diff(sourceView)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, subInstructions...)
	}

	for _, sourceView := range sourceViews {
		_, found := lo.Find(targetViews, func(view *SQLiteView) bool {
			return view.Name == sourceView.Name
		})
		if !found {
			instructions = append(instructions, &SQLDropViewInstruction{Name: sourceView.Name})
		}
	}

	return instructions, nil
}

// A virtual table takes its own statement, and a shadow table belongs to its module.
// The order of sqlite_master is the order of the creation, so a table that holds a foreign
// key comes after the table that it names. Keep that order.
func (d *SQLiteDriver) GetTables(ctx context.Context, db *sql.DB) ([]*SQLiteTable, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT master.name
		FROM sqlite_master AS master
		WHERE master.type = 'table' AND master.name NOT LIKE 'sqlite_%'
		AND master.name <> ?
		AND NOT EXISTS (
			SELECT 1 FROM pragma_table_list AS list
			WHERE list.schema = 'main' AND list.name = master.name
			AND list.type IN ('virtual', 'shadow')
		)
		ORDER BY master.rowid;
	`, MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

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

	definition, err := d.GetTableDefinition(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	parsed := parseTableDefinition(definition)

	var uniqueConstraints []*SQLiteUniqueConstraint

	for _, key := range uniqueKeys {
		constraintName := parsed.UniqueNameOf(key)

		// A column constraint holds no name, so a named constraint of one column stays a
		// table constraint. Without that rule the name goes away.
		if len(key) != 1 || constraintName != "" {
			uniqueConstraints = append(uniqueConstraints, &SQLiteUniqueConstraint{
				Name:     constraintName,
				Columns:  key,
				Conflict: parsed.UniqueConflictOf(key),
			})

			continue
		}

		matchingColumn, found := lo.Find(columns, func(column *SQLiteColumn) bool {
			return column.Name == key[0]
		})
		if found {
			matchingColumn.Unique = true

			// The table form of the constraint holds the ON CONFLICT clause, and the
			// column keeps it.
			if matchingColumn.UniqueConflict == "" {
				matchingColumn.UniqueConflict = parsed.UniqueConflictOf(key)
			}
		}
	}

	indexes, err := d.GetTableIndexes(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	triggers, err := d.GetTriggers(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	foreignKeys, err := d.GetTableForeignKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	// PRAGMA foreign_key_list reports no DEFERRABLE clause. SQLite writes a key of one column
	// as a column constraint or as a table constraint, and dbdiff writes the table form, so a
	// key of one column reads either place.
	for _, foreignKey := range foreignKeys {
		foreignKey.Name = parsed.ForeignKeyNameOf(foreignKey.From)
		foreignKey.Deferrable = parsed.DeferrableOf(foreignKey.From)

		if foreignKey.Deferrable != "" || len(foreignKey.From) != 1 {
			continue
		}

		attributes, found := parsed.ColumnByName(foreignKey.From[0])
		if found {
			foreignKey.Deferrable = attributes.ForeignKeyDeferrable
		}
	}

	return &SQLiteTable{
		Name:               tableName,
		Columns:            columns,
		PrimaryKey:         primaryKey,
		UniqueConstraints:  uniqueConstraints,
		Indexes:            indexes,
		Triggers:           triggers,
		ForeignKeys:        foreignKeys,
		CheckConstraints:   parsed.CheckConstraints,
		PrimaryKeyName:     parsed.PrimaryKeyName,
		PrimaryKeyConflict: parsed.PrimaryKeyConflict,
		WithoutRowID:       parsed.WithoutRowID,
		Strict:             parsed.Strict,
	}, nil
}

func (d *SQLiteDriver) GetTableDefinition(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	row := db.QueryRowContext(ctx,
		"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?;", tableName)

	var definition sql.NullString

	err := row.Scan(&definition)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}

	if err != nil {
		return "", err
	}

	return definition.String, nil
}

const (
	hiddenColumnOfAVirtualTable = 1
	virtualGeneratedColumn      = 2
	storedGeneratedColumn       = 3
)

// PRAGMA table_info gives no generated column, so this method reads PRAGMA table_xinfo.
// That PRAGMA gives no expression, so parseTableDefinition reads it.
func (d *SQLiteDriver) GetTableColumns(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteColumn, error) {
	definition, err := d.GetTableDefinition(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	parsed := parseTableDefinition(definition)

	rows, err := db.QueryContext(ctx, "PRAGMA table_xinfo("+QuoteIdentifier(tableName)+");")
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var columns []*SQLiteColumn
	var primaryKeyColumns []*SQLiteColumn

	for rows.Next() {
		var columnID int
		var name string
		var columnType string
		var isNotNull int
		var defaultValue sql.NullString
		var primaryKeyPosition int
		var hidden int

		err := rows.Scan(&columnID, &name, &columnType, &isNotNull, &defaultValue, &primaryKeyPosition, &hidden)
		if err != nil {
			return nil, err
		}

		if hidden == hiddenColumnOfAVirtualTable {
			continue
		}

		column := &SQLiteColumn{
			Name:    name,
			Type:    columnType,
			NotNull: isNotNull == 1,
			Default: defaultValue,
		}

		attributes, found := parsed.ColumnByName(name)
		if found {
			column.AutoIncrement = attributes.AutoIncrement
			column.Collation = attributes.Collation
			column.Check = attributes.Check
			column.PrimaryKeyConflict = attributes.PrimaryKeyConflict
			column.UniqueConflict = attributes.UniqueConflict
			column.NotNullConflict = attributes.NotNullConflict

			if hidden == virtualGeneratedColumn || hidden == storedGeneratedColumn {
				column.GeneratedExpression = attributes.GeneratedExpression
			}
		}

		if hidden == virtualGeneratedColumn || hidden == storedGeneratedColumn {
			column.GeneratedStored = hidden == storedGeneratedColumn
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

func (d *SQLiteDriver) GetTablePrimaryKey(ctx context.Context, db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+QuoteIdentifier(tableName)+");")
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

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

// This method sorts the keys, because SQLite gives no stable order.
func (d *SQLiteDriver) GetTableUniqueKeys(ctx context.Context, db *sql.DB, tableName string) ([][]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA index_list("+QuoteIdentifier(tableName)+");")
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

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
	rows, err := db.QueryContext(ctx, "PRAGMA index_info("+QuoteIdentifier(indexName)+");")
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

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

	rows, err := db.QueryContext(ctx, "PRAGMA index_list("+QuoteIdentifier(tableName)+");")
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

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

// An index that a UNIQUE constraint or a PRIMARY KEY builds holds no text.
func (d *SQLiteDriver) GetIndexDefinitions(ctx context.Context, db *sql.DB, tableName string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL", tableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

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

// PRAGMA index_info gives no name for an expression key, no direction, and no collation,
// so definitionKeys and indexKeyModifiers read those parts from the text.
func (d *SQLiteDriver) GetIndexKeys(ctx context.Context, db *sql.DB, indexName string, definitionKeys []string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA index_info("+QuoteIdentifier(indexName)+");")
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

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
			modifiers := ""

			if keyPosition >= 0 && keyPosition < len(definitionKeys) {
				modifiers = indexKeyModifiers(definitionKeys[keyPosition])
			}

			keys = append(keys, QuoteIdentifier(name.String)+modifiers)

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

func (d *SQLiteDriver) GetTriggers(ctx context.Context, db *sql.DB, objectName string) ([]*SQLiteTrigger, error) {
	rows, err := db.QueryContext(ctx, "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?", objectName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

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

	defer func() { _ = rows.Close() }()

	var views []*SQLiteView

	for rows.Next() {
		var name, sqlContent string

		err := rows.Scan(&name, &sqlContent)
		if err != nil {
			return nil, err
		}

		triggers, err := d.GetTriggers(ctx, db, name)
		if err != nil {
			return nil, err
		}

		views = append(views, &SQLiteView{
			Name:     name,
			SQL:      sqlContent,
			Triggers: triggers,
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return views, nil
}

func (d *SQLiteDriver) GetTableForeignKeys(ctx context.Context, db *sql.DB, tableName string) ([]*SQLiteForeignKey, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_list("+QuoteIdentifier(tableName)+");")
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	foreignKeysMap := make(map[int]*SQLiteForeignKey)

	for rows.Next() {
		var foreignKeyID, keyPosition int
		var table, from, onUpdate, onDelete, match string

		// PRAGMA foreign_key_list gives a NULL parent column when the key names no parent column.
		var to sql.NullString

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

		if to.Valid {
			foreignKey.To = append(foreignKey.To, to.String)
		}
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
