package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type MySQLDriverConfig struct {
	ComparePrivileges bool
	IgnoreTables      []string
}

type MySQLDriver struct {
	TargetDatabaseConnection *sql.DB
	SourceDatabaseConnection *sql.DB
	ComparePrivileges        bool
	IgnoreTables             []string

	scratchDatabases    []*mysqlScratchDatabase
	detailsByConnection map[*sql.DB]*mysqlConnectionDetails

	disableForeignKeyChecks bool

	// DiffTables keeps the two table lists here, so the data comparison reads the
	// schema one time.
	targetTables []*MySQLTable
	sourceTables []*MySQLTable
}

type mysqlConnectionDetails struct {
	mariadb          bool
	databaseName     string
	defaultEngine    string
	defaultCollation string
}

func NewMySQLDriver(config *MySQLDriverConfig) *MySQLDriver {
	return &MySQLDriver{
		ComparePrivileges: config.ComparePrivileges,
		IgnoreTables:      config.IgnoreTables,
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
		mariadb:          strings.Contains(strings.ToLower(version), "mariadb"),
		databaseName:     databaseName.String,
		defaultEngine:    defaultEngine,
		defaultCollation: defaultCollation.String,
	}

	return nil
}

func (d *MySQLDriver) openSides(ctx context.Context, source driversshared.DataSource,
	target driversshared.DataSource) (func(), error) {
	d.detailsByConnection = make(map[*sql.DB]*mysqlConnectionDetails)

	targetDatabaseConnection, err := d.OpenSide(ctx, target, source, "target")
	if err != nil {
		_ = d.dropScratchDatabases()
		return nil, err
	}

	d.TargetDatabaseConnection = targetDatabaseConnection

	err = d.registerConnection(ctx, targetDatabaseConnection)
	if err != nil {
		_ = d.TargetDatabaseConnection.Close()
		_ = d.dropScratchDatabases()

		return nil, err
	}

	sourceDatabaseConnection, err := d.OpenSide(ctx, source, target, "source")
	if err != nil {
		_ = d.TargetDatabaseConnection.Close()
		_ = d.dropScratchDatabases()

		return nil, err
	}

	d.SourceDatabaseConnection = sourceDatabaseConnection

	err = d.registerConnection(ctx, sourceDatabaseConnection)
	if err != nil {
		_ = d.TargetDatabaseConnection.Close()
		_ = d.SourceDatabaseConnection.Close()
		_ = d.dropScratchDatabases()

		return nil, err
	}

	return func() {
		_ = d.TargetDatabaseConnection.Close()
		_ = d.SourceDatabaseConnection.Close()
		d.TargetDatabaseConnection = nil
		d.SourceDatabaseConnection = nil
		_ = d.dropScratchDatabases()
	}, nil
}

func (d *MySQLDriver) dropScratchDatabases() error {
	var firstError error

	for _, scratch := range d.scratchDatabases {
		err := scratch.drop()
		if err != nil && firstError == nil {
			firstError = err
		}
	}

	d.scratchDatabases = nil

	return firstError
}

func (d *MySQLDriver) Diff(ctx context.Context, source driversshared.DataSource,
	target driversshared.DataSource, options driversshared.DiffOptions) ([]driversshared.Instruction, error) {
	release, err := d.openSides(ctx, source, target)
	if err != nil {
		return nil, err
	}

	defer release()

	d.disableForeignKeyChecks = false
	d.targetTables = nil
	d.sourceTables = nil

	var instructions []driversshared.Instruction

	// A column default can read a sequence, so the sequences come before the tables.
	sequenceInstructions, err := d.DiffSequences(ctx)
	if err != nil {
		return nil, err
	}

	instructions = append(instructions, sequenceInstructions...)

	tableInstructions, err := d.DiffTables(ctx)
	if err != nil {
		return nil, err
	}

	instructions = append(instructions, tableInstructions...)

	// A view can call a function, and MySQL checks the call at the creation of the view,
	// so the routines come before the views.
	routineInstructions, err := d.DiffRoutines(ctx)
	if err != nil {
		return nil, err
	}

	instructions = append(instructions, routineInstructions...)

	viewInstructions, err := d.DiffViews(ctx)
	if err != nil {
		return nil, err
	}

	instructions = append(instructions, viewInstructions...)

	eventInstructions, err := d.DiffEvents(ctx)
	if err != nil {
		return nil, err
	}

	instructions = append(instructions, eventInstructions...)

	if d.ComparePrivileges {
		privilegeInstructions, err := d.DiffPrivileges(ctx)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, privilegeInstructions...)
	}

	if options.CompareData {
		dataInstructions, err := d.DiffData(ctx)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, dataInstructions...)
	}

	if d.disableForeignKeyChecks {
		instructions = append([]driversshared.Instruction{&MySQLSetForeignKeyChecksInstruction{}}, instructions...)
		instructions = append(instructions, &MySQLSetForeignKeyChecksInstruction{Enabled: true})
	}

	return instructions, nil
}

func (d *MySQLDriver) DiffTables(ctx context.Context) ([]driversshared.Instruction, error) {
	targetTables, err := d.GetTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceTables, err := d.GetTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	d.targetTables = targetTables
	d.sourceTables = sourceTables

	if needsForeignKeyChecksOff(targetTables, sourceTables) {
		d.disableForeignKeyChecks = true
	}

	additions, removals, err := driversshared.DiffByKey(targetTables, sourceTables, driversshared.DiffRules[*MySQLTable]{
		Key: func(table *MySQLTable) string {
			return table.Name
		},
		Create: func(table *MySQLTable) []driversshared.Instruction {
			return table.Instructions()
		},
		Change: func(target *MySQLTable, source *MySQLTable) ([]driversshared.Instruction, error) {
			return target.DiffTable(source)
		},
		Drop: func(table *MySQLTable) []driversshared.Instruction {
			return []driversshared.Instruction{&MySQLDropTableInstruction{
				Name: table.Name,
			}}
		},
	})
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}

// The creation order and the removal order of the tables read no dependency, so a foreign
// key of a created table or of a dropped table needs the enforcement off.
func needsForeignKeyChecksOff(targetTables []*MySQLTable, sourceTables []*MySQLTable) bool {
	tableByName := func(tables []*MySQLTable, name string) (*MySQLTable, bool) {
		return lo.Find(tables, func(table *MySQLTable) bool {
			return table.Name == name
		})
	}

	for _, targetTable := range targetTables {
		_, found := tableByName(sourceTables, targetTable.Name)
		if !found && len(targetTable.ForeignKeys) > 0 {
			return true
		}

		// A kept table can gain a key to a table that another instruction creates later.
		for _, foreignKey := range targetTable.ForeignKeys {
			_, referencedExists := tableByName(sourceTables, foreignKey.ReferencedTable)
			if !referencedExists {
				return true
			}
		}
	}

	for _, sourceTable := range sourceTables {
		_, found := tableByName(targetTables, sourceTable.Name)
		if found {
			continue
		}

		if len(sourceTable.ForeignKeys) > 0 {
			return true
		}

		referenced := lo.SomeBy(sourceTables, func(table *MySQLTable) bool {
			return lo.SomeBy(table.ForeignKeys, func(foreignKey *MySQLForeignKey) bool {
				return foreignKey.ReferencedTable == sourceTable.Name
			})
		})
		if referenced {
			return true
		}
	}

	return false
}

func (d *MySQLDriver) GetTables(ctx context.Context, db *sql.DB) ([]*MySQLTable, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, CREATE_OPTIONS
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME <> ?
		ORDER BY TABLE_NAME;
	`, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	type tableRow struct {
		Name          string
		Engine        sql.NullString
		Collation     sql.NullString
		CreateOptions sql.NullString
	}

	var tableRows []tableRow

	for rows.Next() {
		var row tableRow

		err := rows.Scan(&row.Name, &row.Engine, &row.Collation, &row.CreateOptions)
		if err != nil {
			return nil, err
		}

		if slices.Contains(d.IgnoreTables, row.Name) {
			continue
		}

		tableRows = append(tableRows, row)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	var tables []*MySQLTable

	for _, row := range tableRows {
		partitioned := strings.Contains(strings.ToLower(row.CreateOptions.String), "partitioned")

		table, err := d.GetTable(ctx, db, row.Name, row.Engine, row.Collation, partitioned)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func (d *MySQLDriver) GetTable(ctx context.Context, db *sql.DB, tableName string,
	engine sql.NullString, tableCollation sql.NullString, partitioned bool) (*MySQLTable, error) {
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
	rows, err := db.QueryContext(ctx, `
		SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
		FROM information_schema.TRIGGERS
		WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = ?
		ORDER BY ACTION_TIMING, EVENT_MANIPULATION, ACTION_ORDER;
	`, tableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var triggers []*MySQLTrigger

	for rows.Next() {
		var name, timing, event, statement string

		err := rows.Scan(&name, &timing, &event, &statement)
		if err != nil {
			return nil, err
		}

		triggers = append(triggers, &MySQLTrigger{
			Table:     tableName,
			Name:      name,
			Timing:    timing,
			Event:     event,
			Statement: statement,
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return triggers, nil
}

func (d *MySQLDriver) GetTableColumns(ctx context.Context, db *sql.DB, tableName string,
	tableCollation sql.NullString) ([]*MySQLColumn, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA,
			COLLATION_NAME, COLUMN_COMMENT, GENERATION_EXPRESSION
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION;
	`, tableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	mariadb := d.detailsByConnection[db].mariadb

	var columns []*MySQLColumn

	for rows.Next() {
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
			column.GeneratedExpression = unescapeCatalogText(mariadb, generationExpression.String)
			column.GeneratedStored = strings.Contains(extraLower, "stored generated")
		}

		applyColumnDefault(column, columnDefault, extraLower, mariadb)

		if collationName.Valid && collationName.String != tableCollation.String {
			column.Collation = collationName.String
		}

		columns = append(columns, column)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return columns, nil
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
		value = unescapeCatalogText(mariadb, value)
	}

	column.Default = sql.NullString{
		String: value,
		Valid:  true,
	}
}

// The information_schema tables of MySQL escape a quote and a backslash of an expression
// text, and the statement needs the bare characters. One pass keeps an escaped backslash
// before a quote correct.
func unescapeCatalogText(mariadb bool, value string) string {
	if mariadb {
		return value
	}

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

func (d *MySQLDriver) GetTableIndexes(ctx context.Context, db *sql.DB, tableName string) ([]string, []*MySQLIndex, error) {
	expressionColumn := "NULL"
	if !d.detailsByConnection[db].mariadb {
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

		index.Keys = append(index.Keys, indexKey(d.detailsByConnection[db].mariadb, index.Kind,
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
func indexKey(mariadb bool, kind string, columnName sql.NullString, subPart sql.NullInt64,
	collation sql.NullString, expression sql.NullString) string {
	if expression.Valid && expression.String != "" {
		return "(" + unescapeCatalogText(mariadb, expression.String) + ")"
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

func (d *MySQLDriver) GetTableForeignKeys(ctx context.Context, db *sql.DB, tableName string) ([]*MySQLForeignKey, error) {
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
func (d *MySQLDriver) GetTableCheckConstraints(ctx context.Context, db *sql.DB, tableName string) ([]*MySQLCheckConstraint, error) {
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
			SELECT CONSTRAINT_NAME, CHECK_CLAUSE
			FROM information_schema.CHECK_CONSTRAINTS
			WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = ?
			ORDER BY CONSTRAINT_NAME;
		`
	}

	rows, err := db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var checkConstraints []*MySQLCheckConstraint

	for rows.Next() {
		var name, clause string

		err := rows.Scan(&name, &clause)
		if err != nil {
			return nil, err
		}

		checkConstraints = append(checkConstraints, &MySQLCheckConstraint{
			Name:       name,
			Expression: unescapeCatalogText(mariadb, clause),
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return checkConstraints, nil
}

func (d *MySQLDriver) DiffViews(ctx context.Context) ([]driversshared.Instruction, error) {
	targetViews, err := d.GetViews(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceViews, err := d.GetViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	additions, removals, err := driversshared.DiffByKey(targetViews, sourceViews, driversshared.DiffRules[*MySQLView]{
		Key: func(view *MySQLView) string {
			return view.Name
		},
		Create: func(view *MySQLView) []driversshared.Instruction {
			return view.Instructions()
		},
		Change: func(target *MySQLView, source *MySQLView) ([]driversshared.Instruction, error) {
			return target.Diff(source)
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
	rows, err := db.QueryContext(ctx, `
		SELECT TABLE_NAME, VIEW_DEFINITION
		FROM information_schema.VIEWS
		WHERE TABLE_SCHEMA = DATABASE()
		ORDER BY TABLE_NAME;
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	databaseQualifier := QuoteIdentifier(d.detailsByConnection[db].databaseName) + "."

	var views []*MySQLView

	for rows.Next() {
		var name string
		var definition sql.NullString

		err := rows.Scan(&name, &definition)
		if err != nil {
			return nil, err
		}

		// The catalog gives NULL when the connection misses the SHOW VIEW privilege.
		if !definition.Valid {
			return nil, fmt.Errorf("the view %q gives no definition. Grant the SHOW VIEW privilege", name)
		}

		views = append(views, &MySQLView{
			Name:       name,
			Definition: strings.ReplaceAll(definition.String, databaseQualifier, ""),
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return views, nil
}
