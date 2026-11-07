package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	embeddedmysql "github.com/quantumsheep/embedded-mysql"
	"github.com/samber/lo"
)

type MySQLDriverConfig struct {
	ComparePrivileges    bool
	ScratchServerVersion string
	ScratchDatabase      string
	IgnoreTables         []string
}

type MySQLDriver struct {
	DesiredDatabaseConnection *sql.DB
	CurrentDatabaseConnection *sql.DB
	ComparePrivileges         bool
	ScratchServerVersion      string
	ScratchDatabase           string
	IgnoreTables              []string

	scratchServer       *embeddedmysql.EmbeddedMySQL
	scratchDatabases    []*mysqlScratchDatabase
	detailsByConnection map[*sql.DB]*mysqlConnectionDetails
}

type mysqlConnectionDetails struct {
	mariadb          bool
	databaseName     string
	defaultEngine    string
	defaultCollation string
}

func isMariaDB(version string) bool {
	return strings.Contains(strings.ToLower(version), "mariadb")
}

func NewMySQLDriver(config *MySQLDriverConfig) *MySQLDriver {
	return &MySQLDriver{
		ComparePrivileges:    config.ComparePrivileges,
		ScratchServerVersion: config.ScratchServerVersion,
		ScratchDatabase:      config.ScratchDatabase,
		IgnoreTables:         config.IgnoreTables,
	}
}

func (d *MySQLDriver) registerConnection(ctx context.Context, db *sql.DB) error {
	row := db.QueryRowContext(ctx, `
		SELECT VERSION(), DATABASE(), @@default_storage_engine,
			(SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA
				WHERE SCHEMA_NAME = DATABASE());
	`)

	var version, defaultEngine string
	var databaseName, defaultCollation sql.NullString

	err := row.Scan(&version, &databaseName, &defaultEngine, &defaultCollation)
	if err != nil {
		return err
	}

	if !databaseName.Valid {
		return fmt.Errorf("the connection names no database")
	}

	d.detailsByConnection[db] = &mysqlConnectionDetails{
		mariadb:          isMariaDB(version),
		databaseName:     databaseName.String,
		defaultEngine:    defaultEngine,
		defaultCollation: defaultCollation.String,
	}

	return nil
}

func (d *MySQLDriver) openSides(
	ctx context.Context,
	current driversshared.DataSource,
	desired driversshared.DataSource,
) (func(), error) {
	d.detailsByConnection = make(map[*sql.DB]*mysqlConnectionDetails)

	desiredDatabaseConnection, err := d.OpenSide(ctx, desired, current, "desired")
	if err != nil {
		_ = d.releaseScratchResources(ctx)
		return nil, err
	}

	d.DesiredDatabaseConnection = desiredDatabaseConnection

	err = d.registerConnection(ctx, desiredDatabaseConnection)
	if err != nil {
		_ = d.DesiredDatabaseConnection.Close()
		_ = d.releaseScratchResources(ctx)

		return nil, err
	}

	currentDatabaseConnection, err := d.OpenSide(ctx, current, desired, "current")
	if err != nil {
		_ = d.DesiredDatabaseConnection.Close()
		_ = d.releaseScratchResources(ctx)

		return nil, err
	}

	d.CurrentDatabaseConnection = currentDatabaseConnection

	err = d.registerConnection(ctx, currentDatabaseConnection)
	if err != nil {
		_ = d.DesiredDatabaseConnection.Close()
		_ = d.CurrentDatabaseConnection.Close()
		_ = d.releaseScratchResources(ctx)

		return nil, err
	}

	return func() {
		_ = d.DesiredDatabaseConnection.Close()
		_ = d.CurrentDatabaseConnection.Close()
		d.DesiredDatabaseConnection = nil
		d.CurrentDatabaseConnection = nil
		_ = d.releaseScratchResources(ctx)
	}, nil
}

// If the context of the diff stops, the cleanup must still run. The time limit stops a
// cleanup that waits for a server that does not answer.
func (d *MySQLDriver) releaseScratchResources(ctx context.Context) error {
	cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	var firstError error

	for _, scratch := range d.scratchDatabases {
		err := scratch.drop(cleanupContext)
		if err != nil && firstError == nil {
			firstError = err
		}
	}

	d.scratchDatabases = nil

	err := d.StopScratchServer()
	if err != nil && firstError == nil {
		firstError = err
	}

	return firstError
}

func (d *MySQLDriver) Diff(
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

	desiredTables, err := d.GetTables(ctx, d.DesiredDatabaseConnection)
	if err != nil {
		return nil, err
	}

	currentTables, err := d.GetTables(ctx, d.CurrentDatabaseConnection)
	if err != nil {
		return nil, err
	}

	steps := []func(context.Context) ([]driversshared.Instruction, error){
		// A column default can read a sequence, so the sequences come before the tables.
		d.DiffSequences,
		func(context.Context) ([]driversshared.Instruction, error) {
			return d.DiffTables(desiredTables, currentTables)
		},
		// A view can call a function, and MySQL checks the call at the creation of the view,
		// so the routines come before the views.
		d.DiffRoutines,
		d.DiffViews,
		d.DiffEvents,
	}

	if d.ComparePrivileges {
		steps = append(steps, d.DiffPrivileges)
	}

	if options.CompareData {
		steps = append(steps, func(ctx context.Context) ([]driversshared.Instruction, error) {
			return d.DiffData(ctx, desiredTables, currentTables)
		})
	}

	var instructions []driversshared.Instruction

	for _, step := range steps {
		stepInstructions, err := step(ctx)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, stepInstructions...)
	}

	if needsForeignKeyChecksOff(desiredTables, currentTables) || dataNeedsForeignKeyChecksOff(instructions, desiredTables) {
		instructions = append([]driversshared.Instruction{&MySQLSetForeignKeyChecksInstruction{}}, instructions...)
		instructions = append(instructions, &MySQLSetForeignKeyChecksInstruction{Enabled: true})
	}

	return instructions, nil
}

func (d *MySQLDriver) DiffTables(
	desiredTables []*MySQLTable,
	currentTables []*MySQLTable,
) ([]driversshared.Instruction, error) {
	columnsDiffs := map[string]*MySQLTableColumnsDiff{}

	for _, desiredTable := range desiredTables {
		currentTable, found := lo.Find(currentTables, func(table *MySQLTable) bool {
			return table.Name == desiredTable.Name
		})
		if found {
			columnsDiffs[desiredTable.Name] = desiredTable.DiffColumns(currentTable)
		}
	}

	var dropDependents []driversshared.Instruction
	var addDependents []driversshared.Instruction

	desiredForeignKeyNames := lo.FlatMap(desiredTables, func(table *MySQLTable, _ int) []string {
		return lo.Map(table.ForeignKeys, func(foreignKey *MySQLForeignKey, _ int) string {
			return foreignKey.Name
		})
	})
	desiredCheckNames := lo.FlatMap(desiredTables, func(table *MySQLTable, _ int) []string {
		return lo.Map(table.CheckConstraints, func(check *MySQLCheckConstraint, _ int) string {
			return check.Name
		})
	})

	additions, removals, err := driversshared.DiffByKey(desiredTables, currentTables, driversshared.DiffRules[*MySQLTable]{
		Key: func(table *MySQLTable) string {
			return table.Name
		},
		Create: func(table *MySQLTable) []driversshared.Instruction {
			return table.Instructions()
		},
		Change: func(desired *MySQLTable, current *MySQLTable) ([]driversshared.Instruction, error) {
			tableDiff, err := desired.DiffTable(current, columnsDiffs)
			if err != nil {
				return nil, err
			}

			dropDependents = append(dropDependents, tableDiff.DropDependents...)
			addDependents = append(addDependents, tableDiff.AddDependents...)

			return tableDiff.Alter, nil
		},
		// The constraint names are unique in the schema, and the table drops come last. So a
		// name that a desired table takes again must be free before the creations.
		Drop: func(table *MySQLTable) []driversshared.Instruction {
			for _, foreignKey := range table.ForeignKeys {
				if slices.Contains(desiredForeignKeyNames, foreignKey.Name) {
					dropDependents = append(dropDependents, &MySQLAlterTableInstruction{
						Name: table.Name,
						Action: &MySQLDropForeignKeyAction{
							Name: foreignKey.Name,
						},
					})
				}
			}

			for _, check := range table.CheckConstraints {
				if slices.Contains(desiredCheckNames, check.Name) {
					dropDependents = append(dropDependents, &MySQLAlterTableInstruction{
						Name: table.Name,
						Action: &MySQLDropCheckConstraintAction{
							Name: check.Name,
						},
					})
				}
			}

			return []driversshared.Instruction{&MySQLDropTableInstruction{
				Name: table.Name,
			}}
		},
	})
	if err != nil {
		return nil, err
	}

	instructions := append(dropDependents, additions...)
	instructions = append(instructions, addDependents...)

	return append(instructions, removals...), nil
}

// The creation order and the removal order of the tables read no dependency, so a foreign
// key of a created table or of a dropped table needs the enforcement off.
func needsForeignKeyChecksOff(desiredTables []*MySQLTable, currentTables []*MySQLTable) bool {
	tableByName := func(tables []*MySQLTable, name string) (*MySQLTable, bool) {
		return lo.Find(tables, func(table *MySQLTable) bool {
			return table.Name == name
		})
	}

	for _, desiredTable := range desiredTables {
		_, found := tableByName(currentTables, desiredTable.Name)
		if !found && len(desiredTable.ForeignKeys) > 0 {
			return true
		}

		// A kept table can gain a key to a new table. The new table holds no row before the
		// data diff, so a check of the current rows can fail.
		for _, foreignKey := range desiredTable.ForeignKeys {
			_, referencedExists := tableByName(currentTables, foreignKey.ReferencedTable)
			if !referencedExists {
				return true
			}
		}
	}

	for _, currentTable := range currentTables {
		_, found := tableByName(desiredTables, currentTable.Name)
		if found {
			continue
		}

		if len(currentTable.ForeignKeys) > 0 {
			return true
		}

		referenced := lo.SomeBy(currentTables, func(table *MySQLTable) bool {
			return lo.SomeBy(table.ForeignKeys, func(foreignKey *MySQLForeignKey) bool {
				return foreignKey.ReferencedTable == currentTable.Name
			})
		})
		if referenced {
			return true
		}
	}

	return false
}

// Every catalog query needs a stable ORDER BY, because MySQL gives no stable row order.
func (d *MySQLDriver) GetTables(ctx context.Context, db *sql.DB) ([]*MySQLTable, error) {
	type tableRow struct {
		Name          string
		Engine        sql.NullString
		Collation     sql.NullString
		CreateOptions sql.NullString
	}

	tableRows, err := driversshared.QueryAll(ctx, db, `
		SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, CREATE_OPTIONS
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME <> ?
		ORDER BY TABLE_NAME;
	`, func(rows *sql.Rows) (tableRow, error) {
		var row tableRow

		err := rows.Scan(&row.Name, &row.Engine, &row.Collation, &row.CreateOptions)

		return row, err
	}, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	var tables []*MySQLTable

	for _, row := range tableRows {
		if slices.Contains(d.IgnoreTables, row.Name) {
			continue
		}

		partitioned := strings.Contains(strings.ToLower(row.CreateOptions.String), "partitioned")

		table, err := d.GetTable(ctx, db, row.Name, row.Engine, row.Collation, partitioned)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func (d *MySQLDriver) GetTable(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	engine sql.NullString,
	tableCollation sql.NullString,
	partitioned bool,
) (*MySQLTable, error) {
	columns, err := d.GetTableColumns(ctx, db, tableName, tableCollation)
	if err != nil {
		return nil, err
	}

	foreignKeys, err := d.GetTableForeignKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	primaryKey, indexes, err := d.GetTableIndexes(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	checkConstraints, err := d.GetTableCheckConstraints(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	triggers, err := d.GetTableTriggers(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	partitionClause := ""

	if partitioned {
		partitionClause, err = d.GetTablePartitionClause(ctx, db, tableName)
		if err != nil {
			return nil, err
		}
	}

	details := d.detailsByConnection[db]

	return &MySQLTable{
		Name:               tableName,
		Columns:            columns,
		PrimaryKey:         primaryKey,
		Indexes:            indexes,
		ForeignKeys:        foreignKeys,
		CheckConstraints:   checkConstraints,
		Triggers:           triggers,
		Engine:             engine.String,
		EngineIsDefault:    strings.EqualFold(engine.String, details.defaultEngine),
		Collation:          tableCollation.String,
		CollationIsDefault: tableCollation.String == details.defaultCollation,
		PartitionClause:    partitionClause,
	}, nil
}

// The PARTITIONS catalog table gives the parts of the clause, and the rebuild of the text
// from those parts is fragile. The text of SHOW CREATE TABLE holds the whole clause.
func (d *MySQLDriver) GetTablePartitionClause(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	row := db.QueryRowContext(ctx, "SHOW CREATE TABLE "+QuoteIdentifier(tableName)+";")

	var name, definition string

	err := row.Scan(&name, &definition)
	if err != nil {
		return "", err
	}

	return parsePartitionClause(definition), nil
}

// The clause ends the text of SHOW CREATE TABLE, so the last occurrence skips a comment
// or a default value that holds the same words. An old server wraps the clause in the
// conditional comment /*!50100 ... */.
func parsePartitionClause(definition string) string {
	position := strings.LastIndex(definition, "PARTITION BY")
	if position < 0 {
		return ""
	}

	clause := strings.TrimSpace(definition[position:])

	return strings.TrimSpace(strings.TrimSuffix(clause, "*/"))
}

// The order of ACTION_ORDER keeps two triggers of one timing and one event in their
// activation order, so the creation builds that order again without a FOLLOWS clause.
func (d *MySQLDriver) GetTableTriggers(ctx context.Context, db *sql.DB, tableName string) ([]*MySQLTrigger, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
		FROM information_schema.TRIGGERS
		WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = ?
		ORDER BY ACTION_TIMING, EVENT_MANIPULATION, ACTION_ORDER;
	`, func(rows *sql.Rows) (*MySQLTrigger, error) {
		trigger := &MySQLTrigger{
			Table: tableName,
		}

		err := rows.Scan(&trigger.Name, &trigger.Timing, &trigger.Event, &trigger.Statement)

		return trigger, err
	}, tableName)
}

func (d *MySQLDriver) GetTableColumns(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	tableCollation sql.NullString,
) ([]*MySQLColumn, error) {
	mariadb := d.detailsByConnection[db].mariadb

	return driversshared.QueryAll(ctx, db, `
		SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA,
			COLLATION_NAME, COLUMN_COMMENT, GENERATION_EXPRESSION
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION;
	`, func(rows *sql.Rows) (*MySQLColumn, error) {
		var name, columnType, isNullable, extra string
		var columnDefault, collationName, generationExpression sql.NullString
		var columnComment string

		err := rows.Scan(&name, &columnType, &isNullable, &columnDefault, &extra,
			&collationName, &columnComment, &generationExpression)
		if err != nil {
			return nil, err
		}

		column := &MySQLColumn{
			Name:    name,
			Type:    columnType,
			NotNull: isNullable == "NO",
			Comment: columnComment,
		}

		extraLower := strings.ToLower(extra)
		column.AutoIncrement = strings.Contains(extraLower, "auto_increment")

		onUpdatePosition := strings.Index(extraLower, "on update ")
		if onUpdatePosition >= 0 {
			column.OnUpdate = extra[onUpdatePosition+len("on update "):]
		}

		if generationExpression.Valid && generationExpression.String != "" {
			column.GeneratedExpression = generationExpression.String

			if !mariadb {
				column.GeneratedExpression = unescapeCatalogText(column.GeneratedExpression)
			}

			column.GeneratedStored = strings.Contains(extraLower, "stored generated")
		}

		applyColumnDefault(column, columnDefault, extraLower, mariadb)

		if collationName.Valid && collationName.String != tableCollation.String {
			column.Collation = collationName.String
		}

		return column, nil
	}, tableName)
}

// MariaDB stores every default as an expression, and it stores the text NULL for the
// absent default. MySQL stores a literal as its bare text, and it marks an expression
// with DEFAULT_GENERATED.
func applyColumnDefault(column *MySQLColumn, columnDefault sql.NullString, extraLower string, mariadb bool) {
	if !columnDefault.Valid {
		return
	}

	if mariadb {
		if columnDefault.String == "NULL" {
			return
		}

		column.Default = columnDefault
		column.DefaultIsExpression = true

		return
	}

	column.DefaultIsExpression = strings.Contains(extraLower, "default_generated")

	value := columnDefault.String
	if column.DefaultIsExpression {
		value = unescapeCatalogText(value)
	}

	column.Default = sql.NullString{
		String: value,
		Valid:  true,
	}
}

// The information_schema tables of MySQL escape a quote and a backslash of an expression
// text, and the statement needs the bare characters. One pass keeps an escaped backslash
// before a quote correct.
func unescapeCatalogText(value string) string {
	var builder strings.Builder

	for position := 0; position < len(value); position++ {
		escapesNext := value[position] == '\\' && position+1 < len(value) &&
			(value[position+1] == '\'' || value[position+1] == '\\')
		if escapesNext {
			position++
		}

		builder.WriteByte(value[position])
	}

	return builder.String()
}

func (d *MySQLDriver) GetTableIndexes(
	ctx context.Context,
	db *sql.DB,
	tableName string,
) ([]string, []*MySQLIndex, error) {
	mariadb := d.detailsByConnection[db].mariadb

	expressionColumn := "NULL"
	if !mariadb {
		expressionColumn = "EXPRESSION"
	}

	// The STATISTICS table of MariaDB holds no EXPRESSION column, so the query of MariaDB
	// selects NULL in its place.
	query := fmt.Sprintf(`
		SELECT INDEX_NAME, NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, COLLATION, %s
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX;
	`, expressionColumn)

	rows, err := db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, nil, err
	}

	defer func() { _ = rows.Close() }()

	var primaryKey []string
	var indexes []*MySQLIndex

	for rows.Next() {
		var indexName, indexType string
		var nonUnique int
		var columnName, collation, expression sql.NullString
		var subPart sql.NullInt64

		err := rows.Scan(&indexName, &nonUnique, &indexType, &columnName, &subPart, &collation, &expression)
		if err != nil {
			return nil, nil, err
		}

		if indexName == "PRIMARY" {
			primaryKey = append(primaryKey, columnName.String)
			continue
		}

		index, found := lo.Find(indexes, func(index *MySQLIndex) bool {
			return index.Name == indexName
		})
		if !found {
			index = &MySQLIndex{
				Table: tableName,
				Name:  indexName,
				Kind:  indexKind(indexType, nonUnique),
			}

			indexes = append(indexes, index)
		}

		if !mariadb {
			expression.String = unescapeCatalogText(expression.String)
		}

		index.Keys = append(index.Keys, indexKey(index.Kind,
			columnName, subPart, collation, expression))
	}

	err = rows.Err()
	if err != nil {
		return nil, nil, err
	}

	return primaryKey, indexes, nil
}

func indexKind(indexType string, nonUnique int) string {
	if indexType == "FULLTEXT" || indexType == "SPATIAL" {
		return indexType
	}

	if nonUnique == 0 {
		return "UNIQUE"
	}

	return ""
}

// A FULLTEXT index and a SPATIAL index report an internal prefix length, and the CREATE
// INDEX statement refuses it.
func indexKey(
	kind string,
	columnName sql.NullString,
	subPart sql.NullInt64,
	collation sql.NullString,
	expression sql.NullString,
) string {
	if expression.Valid && expression.String != "" {
		return "(" + expression.String + ")"
	}

	key := QuoteIdentifier(columnName.String)

	if subPart.Valid && kind != "FULLTEXT" && kind != "SPATIAL" {
		key += fmt.Sprintf("(%d)", subPart.Int64)
	}

	if collation.String == "D" {
		key += " DESC"
	}

	return key
}

func (d *MySQLDriver) GetTableForeignKeys(
	ctx context.Context,
	db *sql.DB,
	tableName string,
) ([]*MySQLForeignKey, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT rc.CONSTRAINT_NAME, rc.UPDATE_RULE, rc.DELETE_RULE, rc.REFERENCED_TABLE_NAME,
			kcu.COLUMN_NAME, kcu.REFERENCED_COLUMN_NAME
		FROM information_schema.REFERENTIAL_CONSTRAINTS rc
		JOIN information_schema.KEY_COLUMN_USAGE kcu
			ON kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
			AND kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
			AND kcu.TABLE_NAME = rc.TABLE_NAME
		WHERE rc.CONSTRAINT_SCHEMA = DATABASE() AND rc.TABLE_NAME = ?
		ORDER BY rc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION;
	`, tableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var foreignKeys []*MySQLForeignKey

	for rows.Next() {
		var constraintName, updateRule, deleteRule, referencedTable, columnName, referencedColumn string

		err := rows.Scan(&constraintName, &updateRule, &deleteRule, &referencedTable,
			&columnName, &referencedColumn)
		if err != nil {
			return nil, err
		}

		foreignKey, found := lo.Find(foreignKeys, func(foreignKey *MySQLForeignKey) bool {
			return foreignKey.Name == constraintName
		})
		if !found {
			foreignKey = &MySQLForeignKey{
				Name:            constraintName,
				ReferencedTable: referencedTable,
				OnUpdate:        updateRule,
				OnDelete:        deleteRule,
			}

			foreignKeys = append(foreignKeys, foreignKey)
		}

		foreignKey.Columns = append(foreignKey.Columns, columnName)
		foreignKey.ReferencedColumns = append(foreignKey.ReferencedColumns, referencedColumn)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return foreignKeys, nil
}

// The CHECK_CONSTRAINTS table of MySQL holds no table name, so the query joins the
// TABLE_CONSTRAINTS table. The table of MariaDB holds the table name, and MariaDB needs
// it, because two tables can hold a column constraint with one name.
func (d *MySQLDriver) GetTableCheckConstraints(
	ctx context.Context,
	db *sql.DB,
	tableName string,
) ([]*MySQLCheckConstraint, error) {
	query := `
		SELECT tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
		FROM information_schema.TABLE_CONSTRAINTS tc
		JOIN information_schema.CHECK_CONSTRAINTS cc
			ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
			AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
		WHERE tc.CONSTRAINT_SCHEMA = DATABASE() AND tc.TABLE_NAME = ?
			AND tc.CONSTRAINT_TYPE = 'CHECK'
		ORDER BY tc.CONSTRAINT_NAME;
	`

	mariadb := d.detailsByConnection[db].mariadb
	if mariadb {
		query = `
			SELECT CONSTRAINT_NAME, CHECK_CLAUSE, LEVEL = 'Column'
			FROM information_schema.CHECK_CONSTRAINTS
			WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = ?
			ORDER BY CONSTRAINT_NAME;
		`
	}

	return driversshared.QueryAll(ctx, db, query, func(rows *sql.Rows) (*MySQLCheckConstraint, error) {
		check := &MySQLCheckConstraint{}

		var err error

		if mariadb {
			err = rows.Scan(&check.Name, &check.Expression, &check.ColumnLevel)
		} else {
			err = rows.Scan(&check.Name, &check.Expression)
			check.Expression = unescapeCatalogText(check.Expression)
		}

		return check, err
	}, tableName)
}

func (d *MySQLDriver) DiffViews(ctx context.Context) ([]driversshared.Instruction, error) {
	getSortedViews := func(ctx context.Context, db *sql.DB) ([]*MySQLView, error) {
		views, err := d.GetViews(ctx, db)
		if err != nil {
			return nil, err
		}

		return sortViewsByDependency(views), nil
	}

	additions, removals, err := driversshared.DiffSides(ctx, d.DesiredDatabaseConnection, d.CurrentDatabaseConnection, getSortedViews, driversshared.DiffRules[*MySQLView]{
		Key: func(view *MySQLView) string {
			return view.Name
		},
		Create: func(view *MySQLView) []driversshared.Instruction {
			return view.Instructions()
		},
		Change: func(desired *MySQLView, current *MySQLView) ([]driversshared.Instruction, error) {
			return desired.Diff(current)
		},
		Drop: func(view *MySQLView) []driversshared.Instruction {
			return []driversshared.Instruction{&MySQLDropViewInstruction{
				Name: view.Name,
			}}
		},
	})
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}

// The definition text qualifies every name with the database, and the two sides hold two
// database names. The removal of the qualifier keeps the two texts comparable.
func (d *MySQLDriver) GetViews(ctx context.Context, db *sql.DB) ([]*MySQLView, error) {
	databaseQualifier := QuoteIdentifier(d.detailsByConnection[db].databaseName) + "."

	return driversshared.QueryAll(ctx, db, `
		SELECT TABLE_NAME, VIEW_DEFINITION, SECURITY_TYPE, CHECK_OPTION
		FROM information_schema.VIEWS
		WHERE TABLE_SCHEMA = DATABASE()
		ORDER BY TABLE_NAME;
	`, func(rows *sql.Rows) (*MySQLView, error) {
		var name string
		var definition sql.NullString
		var securityType string
		var checkOption string

		err := rows.Scan(&name, &definition, &securityType, &checkOption)
		if err != nil {
			return nil, err
		}

		// The catalog gives NULL when the connection misses the SHOW VIEW privilege.
		if !definition.Valid {
			return nil, fmt.Errorf("the view %q gives no definition. Grant the SHOW VIEW privilege", name)
		}

		view := &MySQLView{
			Name:       name,
			Definition: removeDatabaseQualifier(definition.String, databaseQualifier),
		}

		if securityType != "DEFINER" {
			view.SecurityType = securityType
		}

		if checkOption != "NONE" {
			view.CheckOption = checkOption
		}

		return view, nil
	})
}
