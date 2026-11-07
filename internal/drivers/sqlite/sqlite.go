package driverssqlite

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
	_ "modernc.org/sqlite"
)

type SQLiteDriverConfig struct {
	IgnoreTables []string
}

type SQLiteDriver struct {
	DesiredDatabaseConnection *sql.DB
	CurrentDatabaseConnection *sql.DB
	IgnoreTables              []string

	temporaryDirectory string
}

func NewSQLiteDriver(config *SQLiteDriverConfig) *SQLiteDriver {
	return &SQLiteDriver{
		IgnoreTables: config.IgnoreTables,
	}
}

func TrimSQLitePrefix(path string) string {
	return strings.TrimPrefix(path, "sqlite://")
}

func (d *SQLiteDriver) Diff(
	ctx context.Context,
	current driversshared.DataSource,
	desired driversshared.DataSource,
	options driversshared.DiffOptions,
) (driversshared.Instructions, error) {
	release, err := d.openSides(ctx, current, desired)
	if err != nil {
		return nil, err
	}

	defer release()

	steps := []func(context.Context) ([]driversshared.Instruction, error){
		d.DiffTables,
		d.DiffVirtualTables,
		d.DiffViews,
	}

	if options.CompareData {
		steps = append(steps, d.DiffData)
	}

	var instructions []driversshared.Instruction

	for _, step := range steps {
		stepInstructions, err := step(ctx)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, stepInstructions...)
	}

	// SQLite refuses the DROP TABLE of a recreation while another table names the
	// dropped table and the enforcement of the foreign keys is on.
	if holdsTableRecreation(instructions) {
		instructions = append([]driversshared.Instruction{&SQLitePragmaForeignKeysInstruction{}}, instructions...)
		instructions = append(instructions, &SQLitePragmaForeignKeysInstruction{Enabled: true})
	}

	return instructions, nil
}

func (d *SQLiteDriver) openSides(
	ctx context.Context,
	current driversshared.DataSource,
	desired driversshared.DataSource,
) (func(), error) {
	desiredDatabaseConnection, err := d.OpenSide(ctx, desired, "desired")
	if err != nil {
		_ = d.RemoveTemporaryDirectory()

		return nil, err
	}

	d.DesiredDatabaseConnection = desiredDatabaseConnection

	currentDatabaseConnection, err := d.OpenSide(ctx, current, "current")
	if err != nil {
		_ = d.DesiredDatabaseConnection.Close()
		_ = d.RemoveTemporaryDirectory()

		return nil, err
	}

	d.CurrentDatabaseConnection = currentDatabaseConnection

	return func() {
		_ = d.CurrentDatabaseConnection.Close()
		_ = d.DesiredDatabaseConnection.Close()
		d.CurrentDatabaseConnection = nil
		d.DesiredDatabaseConnection = nil
		_ = d.RemoveTemporaryDirectory()
	}, nil
}

// SQLite compares two identifiers without the case of the ASCII letters. It keeps the case
// of every other letter.
func identifierKey(name string) string {
	return strings.Map(func(character rune) rune {
		if character >= 'A' && character <= 'Z' {
			return character + ('a' - 'A')
		}

		return character
	}, name)
}

func holdsTableRecreation(instructions []driversshared.Instruction) bool {
	return lo.SomeBy(instructions, func(instruction driversshared.Instruction) bool {
		_, isRecreation := instruction.(*SQLiteTableRecreationInstruction)

		return isRecreation
	})
}

func (d *SQLiteDriver) DiffTables(ctx context.Context) ([]driversshared.Instruction, error) {
	desiredTables, err := d.GetTables(ctx, d.DesiredDatabaseConnection)
	if err != nil {
		return nil, err
	}

	currentTables, err := d.GetTables(ctx, d.CurrentDatabaseConnection)
	if err != nil {
		return nil, err
	}

	reservedNames, err := d.getReservedNames(ctx)
	if err != nil {
		return nil, err
	}

	additions, removals, err := driversshared.DiffByKey(desiredTables, currentTables, driversshared.DiffRules[*SQLiteTable]{
		Key: func(table *SQLiteTable) string {
			return identifierKey(table.Name)
		},
		Create: func(table *SQLiteTable) []driversshared.Instruction {
			return table.Instructions()
		},
		Change: func(desired *SQLiteTable, current *SQLiteTable) ([]driversshared.Instruction, error) {
			return desired.DiffTable(current, reservedNames)
		},
		Drop: func(table *SQLiteTable) []driversshared.Instruction {
			return []driversshared.Instruction{&driversshared.SQLDropTableInstruction{
				Name: table.Name,
			}}
		},
	})
	if err != nil {
		return nil, err
	}

	// The reverse order gives each DROP TABLE statement before the table that it names.
	slices.Reverse(removals)

	return append(additions, removals...), nil
}

// A table, an index, and a view share one namespace. The set holds the key of each object
// of the two sides, so the temporary table of a recreation can take a free name.
func (d *SQLiteDriver) getReservedNames(ctx context.Context) (map[string]bool, error) {
	reservedNames := make(map[string]bool)

	for _, db := range []*sql.DB{d.CurrentDatabaseConnection, d.DesiredDatabaseConnection} {
		names, err := driversshared.QueryAll(ctx, db, "SELECT name FROM sqlite_master ORDER BY name;", func(rows *sql.Rows) (string, error) {
			var name string

			err := rows.Scan(&name)

			return name, err
		})
		if err != nil {
			return nil, err
		}

		for _, name := range names {
			reservedNames[identifierKey(name)] = true
		}
	}

	return reservedNames, nil
}

func (d *SQLiteDriver) DiffViews(ctx context.Context) ([]driversshared.Instruction, error) {
	additions, removals, err := driversshared.DiffSides(ctx, d.DesiredDatabaseConnection, d.CurrentDatabaseConnection, d.GetViews, driversshared.DiffRules[*SQLiteView]{
		Key: func(view *SQLiteView) string {
			return identifierKey(view.Name)
		},
		Create: func(view *SQLiteView) []driversshared.Instruction {
			return view.Instructions()
		},
		Change: func(desired *SQLiteView, current *SQLiteView) ([]driversshared.Instruction, error) {
			return desired.Diff(current)
		},
		Drop: func(view *SQLiteView) []driversshared.Instruction {
			return []driversshared.Instruction{&driversshared.SQLDropViewInstruction{
				Name: view.Name,
			}}
		},
	})
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}

// A virtual table takes its own statement, and a shadow table belongs to its module.
// The order of sqlite_master is the order of the creation, so a table that holds a foreign
// key comes after the table that it names. Keep that order.
func (d *SQLiteDriver) GetTables(ctx context.Context, db *sql.DB) ([]*SQLiteTable, error) {
	tableNames, err := driversshared.QueryAll(ctx, db, `
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
	`, func(rows *sql.Rows) (string, error) {
		var tableName string

		err := rows.Scan(&tableName)

		return tableName, err
	}, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	var tables []*SQLiteTable

	for _, tableName := range tableNames {
		ignored := slices.ContainsFunc(d.IgnoreTables, func(ignoredTableName string) bool {
			return identifierKey(ignoredTableName) == identifierKey(tableName)
		})
		if ignored {
			continue
		}

		table, err := d.GetTable(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func (d *SQLiteDriver) GetTable(ctx context.Context, db *sql.DB, tableName string) (*SQLiteTable, error) {
	definition, err := d.GetTableDefinition(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	parsed := parseTableDefinition(definition)

	columns, primaryKey, err := d.GetTableColumns(ctx, db, tableName, parsed)
	if err != nil {
		return nil, err
	}

	indexList, err := d.getIndexList(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	uniqueKeys, err := d.GetTableUniqueKeys(ctx, db, indexList)
	if err != nil {
		return nil, err
	}

	var uniqueConstraints []*SQLiteUniqueConstraint

	for _, key := range uniqueKeys {
		parsedUnique := parsed.UniqueConstraintOf(key)

		// A column constraint holds no name, so a named constraint of one column stays a
		// table constraint. Without that rule the name goes away.
		if len(key) != 1 || parsedUnique.Name != "" {
			uniqueConstraints = append(uniqueConstraints, &SQLiteUniqueConstraint{
				Name:     parsedUnique.Name,
				Columns:  key,
				Keys:     parsedUnique.Keys,
				Conflict: parsedUnique.Conflict,
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
				matchingColumn.UniqueConflict = parsedUnique.Conflict
			}
		}
	}

	indexes, err := d.GetTableIndexes(ctx, db, tableName, indexList)
	if err != nil {
		return nil, err
	}

	triggers, err := d.GetTriggers(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	foreignKeys, err := d.GetTableForeignKeys(ctx, db, tableName, parsed.ForeignKeys)
	if err != nil {
		return nil, err
	}

	primaryKeyKeys := parsed.PrimaryKeyKeys
	if len(primaryKeyKeys) != len(primaryKey) {
		primaryKeyKeys = nil
	}

	return &SQLiteTable{
		Name:               tableName,
		Columns:            columns,
		PrimaryKey:         primaryKey,
		PrimaryKeyKeys:     primaryKeyKeys,
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
// That PRAGMA gives no expression, so the parsed definition gives it. The second value
// holds a primary key of two or more columns, in the order of the key.
func (d *SQLiteDriver) GetTableColumns(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	parsed *SQLiteTableDefinition,
) ([]*SQLiteColumn, []string, error) {
	type primaryKeyColumn struct {
		Column   *SQLiteColumn
		Position int
	}

	// A PRAGMA statement takes no placeholder, so QuoteIdentifier must quote every name.
	rows, err := db.QueryContext(ctx, "PRAGMA table_xinfo("+driversshared.QuoteIdentifier(tableName)+");")
	if err != nil {
		return nil, nil, err
	}

	defer func() { _ = rows.Close() }()

	var columns []*SQLiteColumn
	var primaryKeyColumns []primaryKeyColumn

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
			return nil, nil, err
		}

		if hidden == hiddenColumnOfAVirtualTable {
			continue
		}

		column := &SQLiteColumn{
			Name:            name,
			Type:            columnType,
			NotNull:         isNotNull == 1,
			Default:         normalizedDefault(defaultValue),
			GeneratedStored: hidden == storedGeneratedColumn,
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

		columns = append(columns, column)

		if primaryKeyPosition > 0 {
			primaryKeyColumns = append(primaryKeyColumns, primaryKeyColumn{
				Column:   column,
				Position: primaryKeyPosition,
			})
		}
	}

	err = rows.Err()
	if err != nil {
		return nil, nil, err
	}

	// A key of one column stays a column constraint, which keeps INTEGER PRIMARY KEY as the
	// alias of the rowid.
	if len(primaryKeyColumns) == 1 {
		primaryKeyColumns[0].Column.PrimaryKey = true
	}

	if len(primaryKeyColumns) < 2 {
		return columns, nil, nil
	}

	slices.SortStableFunc(primaryKeyColumns, func(first primaryKeyColumn, second primaryKeyColumn) int {
		return cmp.Compare(first.Position, second.Position)
	})

	primaryKey := lo.Map(primaryKeyColumns, func(keyColumn primaryKeyColumn, _ int) string {
		return keyColumn.Column.Name
	})

	return columns, primaryKey, nil
}

// This method sorts the keys, because SQLite gives no stable order.
func (d *SQLiteDriver) GetTableUniqueKeys(
	ctx context.Context,
	db *sql.DB,
	indexList []sqliteIndexListEntry,
) ([][]string, error) {
	var keys [][]string

	for _, index := range indexList {
		if index.Origin != "u" {
			continue
		}

		indexInfo, err := d.getIndexInfo(ctx, db, index.Name)
		if err != nil {
			return nil, err
		}

		var columnNames []string

		for _, entry := range indexInfo {
			if !entry.Name.Valid {
				return nil, fmt.Errorf("the index %s holds a key at the position %d that is no column", index.Name, entry.Position)
			}

			columnNames = append(columnNames, entry.Name.String)
		}

		keys = append(keys, columnNames)
	}

	slices.SortStableFunc(keys, slices.Compare)

	return keys, nil
}

func (d *SQLiteDriver) GetTableIndexes(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	indexList []sqliteIndexListEntry,
) ([]*SQLiteIndex, error) {
	definitions, err := d.GetIndexDefinitions(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	var indexes []*SQLiteIndex

	for _, index := range indexList {
		// The origin "c" marks an index that a CREATE INDEX statement built. The column
		// definition already prints the index of a UNIQUE constraint or of a PRIMARY KEY.
		if index.Origin != "c" {
			continue
		}

		definitionKeys, condition := parseIndexDefinition(definitions[index.Name])

		keys, err := d.GetIndexKeys(ctx, db, index.Name, definitionKeys)
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, &SQLiteIndex{
			Table:  tableName,
			Name:   index.Name,
			Unique: index.Unique,
			Keys:   keys,
			Where:  condition,
		})
	}

	// PRAGMA index_list gives no stable order.
	slices.SortStableFunc(indexes, func(first *SQLiteIndex, second *SQLiteIndex) int {
		return strings.Compare(first.Name, second.Name)
	})

	return indexes, nil
}

type sqliteIndexListEntry struct {
	Name   string
	Unique bool
	Origin string
}

func (d *SQLiteDriver) getIndexList(ctx context.Context, db *sql.DB, tableName string) ([]sqliteIndexListEntry, error) {
	return driversshared.QueryAll(ctx, db, "PRAGMA index_list("+driversshared.QuoteIdentifier(tableName)+");", func(rows *sql.Rows) (sqliteIndexListEntry, error) {
		var entry sqliteIndexListEntry
		var position int
		var isUnique int
		var partial int

		err := rows.Scan(&position, &entry.Name, &isUnique, &entry.Origin, &partial)
		entry.Unique = isUnique == 1

		return entry, err
	})
}

// PRAGMA index_info gives a NULL name for an expression key.
type sqliteIndexInfoEntry struct {
	Position int
	Name     sql.NullString
}

func (d *SQLiteDriver) getIndexInfo(ctx context.Context, db *sql.DB, indexName string) ([]sqliteIndexInfoEntry, error) {
	return driversshared.QueryAll(ctx, db, "PRAGMA index_info("+driversshared.QuoteIdentifier(indexName)+");", func(rows *sql.Rows) (sqliteIndexInfoEntry, error) {
		var entry sqliteIndexInfoEntry
		var columnID int

		err := rows.Scan(&entry.Position, &columnID, &entry.Name)

		return entry, err
	})
}

// An index that a UNIQUE constraint or a PRIMARY KEY builds holds no text.
func (d *SQLiteDriver) GetIndexDefinitions(
	ctx context.Context,
	db *sql.DB,
	tableName string,
) (map[string]string, error) {
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
func (d *SQLiteDriver) GetIndexKeys(
	ctx context.Context,
	db *sql.DB,
	indexName string,
	definitionKeys []string,
) ([]string, error) {
	indexInfo, err := d.getIndexInfo(ctx, db, indexName)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(indexInfo))

	for _, entry := range indexInfo {
		keyPosition := entry.Position
		definitionKeyFound := keyPosition >= 0 && keyPosition < len(definitionKeys)

		if entry.Name.Valid {
			modifiers := ""

			if definitionKeyFound {
				modifiers = indexKeyModifiers(definitionKeys[keyPosition])
			}

			keys = append(keys, driversshared.QuoteIdentifier(entry.Name.String)+modifiers)

			continue
		}

		if !definitionKeyFound {
			return nil, fmt.Errorf("the definition of the index %s holds no key at the position %d", indexName, keyPosition)
		}

		keys = append(keys, definitionKeys[keyPosition])
	}

	return keys, nil
}

func (d *SQLiteDriver) GetTriggers(ctx context.Context, db *sql.DB, objectName string) ([]*SQLiteTrigger, error) {
	return driversshared.QueryAll(ctx, db, "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ? COLLATE NOCASE ORDER BY name", func(rows *sql.Rows) (*SQLiteTrigger, error) {
		trigger := &SQLiteTrigger{}

		err := rows.Scan(&trigger.Name, &trigger.SQL)

		return trigger, err
	}, objectName)
}

func (d *SQLiteDriver) GetViews(ctx context.Context, db *sql.DB) ([]*SQLiteView, error) {
	views, err := driversshared.QueryAll(ctx, db, "SELECT name, sql FROM sqlite_master WHERE type = 'view' AND name NOT LIKE 'sqlite_%' ORDER BY name", func(rows *sql.Rows) (*SQLiteView, error) {
		view := &SQLiteView{}

		err := rows.Scan(&view.Name, &view.SQL)

		return view, err
	})
	if err != nil {
		return nil, err
	}

	for _, view := range views {
		view.Triggers, err = d.GetTriggers(ctx, db, view.Name)
		if err != nil {
			return nil, err
		}
	}

	return views, nil
}

// PRAGMA foreign_key_list reports no name and no DEFERRABLE clause, so each key takes them
// from its clause in parsedForeignKeys.
func (d *SQLiteDriver) GetTableForeignKeys(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	parsedForeignKeys []*parsedConstraint,
) ([]*SQLiteForeignKey, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_list("+driversshared.QuoteIdentifier(tableName)+");")
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

	// SQLite gives the id 0 to the foreign key that the CREATE TABLE text declares last, so the
	// descending ids give the order of the text. Two keys on the same columns get their own
	// clauses only in that order.
	foreignKeysSet := make([]*SQLiteForeignKey, 0, len(foreignKeysMap))
	unmatchedForeignKeys := slices.Clone(parsedForeignKeys)

	for _, foreignKeyID := range slices.Backward(slices.Sorted(maps.Keys(foreignKeysMap))) {
		foreignKey := foreignKeysMap[foreignKeyID]

		position := slices.IndexFunc(unmatchedForeignKeys, func(parsedForeignKey *parsedConstraint) bool {
			return slices.Equal(parsedForeignKey.Columns, foreignKey.From)
		})
		if position >= 0 {
			foreignKey.Name = unmatchedForeignKeys[position].Name
			foreignKey.Deferrable = unmatchedForeignKeys[position].Deferrable
			unmatchedForeignKeys = slices.Delete(unmatchedForeignKeys, position, position+1)
		}

		foreignKeysSet = append(foreignKeysSet, foreignKey)
	}

	// PRAGMA foreign_key_list gives no stable order. Without this sort the output changes between two runs.
	slices.SortStableFunc(foreignKeysSet, func(first *SQLiteForeignKey, second *SQLiteForeignKey) int {
		return cmp.Or(
			strings.Compare(first.Table, second.Table),
			slices.Compare(first.From, second.From),
			slices.Compare(first.To, second.To),
			strings.Compare(first.OnUpdate, second.OnUpdate),
			strings.Compare(first.OnDelete, second.OnDelete),
			strings.Compare(first.Name, second.Name),
			strings.Compare(first.Deferrable, second.Deferrable),
		)
	})

	return foreignKeysSet, nil
}
