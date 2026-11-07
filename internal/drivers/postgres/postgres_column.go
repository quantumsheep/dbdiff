package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type PostgresColumn struct {
	Name    string
	Type    string
	NotNull bool
	Default sql.NullString

	Identity string

	IdentityOptions string

	GeneratedExpression string

	Serial string

	SerialSequenceName string

	Collation string

	// Definition writes no comment, because a column definition accepts none.
	Comment string

	Storage string

	StatisticsTarget sql.NullInt64

	// The parent of a table of INHERITS holds this column, and each change of the parent
	// reaches the child.
	Inherited bool
}

// A column definition accepts no storage mode before PostgreSQL 16, so a separate statement
func (c *PostgresColumn) StorageInstructions(tableName string) []driversshared.Instruction {
	if c.Storage == "" {
		return nil
	}

	return []driversshared.Instruction{&PostgresAlterTableInstruction{
		Name: tableName,
		Actions: []driversshared.AlterTableAction{&PostgresSetStorageAction{
			ColumnName: c.Name,
			Storage:    c.Storage,
		}},
	}}
}

// A column definition accepts no statistics target, so a separate statement sets it.
func (c *PostgresColumn) StatisticsInstructions(tableName string) []driversshared.Instruction {
	if !c.StatisticsTarget.Valid {
		return nil
	}

	return []driversshared.Instruction{&PostgresAlterTableInstruction{
		Name: tableName,
		Actions: []driversshared.AlterTableAction{&PostgresSetStatisticsAction{
			ColumnName:       c.Name,
			StatisticsTarget: c.StatisticsTarget.Int64,
		}},
	}}
}

func (c *PostgresColumn) HasEqualAttributes(other *PostgresColumn) bool {
	columnCopy := *c
	columnCopy.Name = other.Name

	return columnCopy == *other
}

// PostgreSQL 13 adds the DROP EXPRESSION action, and PostgreSQL 17 adds the SET EXPRESSION
// action. No version holds an action that makes a plain column a generated column. The TYPE
// action of a generated column accepts no USING clause and keeps the old expression.
func (c *PostgresColumn) NeedsRebuild(current *PostgresColumn, serverVersion int) bool {
	bothGenerated := c.GeneratedExpression != "" && current.GeneratedExpression != ""
	typeChanged := c.Type != current.Type || c.Collation != current.Collation

	switch {
	case bothGenerated && typeChanged:
		return true
	case c.GeneratedExpression == current.GeneratedExpression:
		return false
	case c.GeneratedExpression == "":
		return serverVersion < 130000
	case current.GeneratedExpression == "":
		return true
	default:
		return serverVersion < 170000
	}
}

func (c *PostgresColumn) Definition() string {
	dataType := c.Type
	if c.Serial != "" {
		dataType = c.Serial
	}

	value := fmt.Sprintf("%s %s", driversshared.QuoteIdentifier(c.Name), dataType)

	if c.Collation != "" {
		value += " COLLATE " + driversshared.QuoteIdentifier(c.Collation)
	}

	if c.NotNull {
		value += " NOT NULL"
	}

	if c.Identity != "" {
		value += fmt.Sprintf(" GENERATED %s AS IDENTITY", c.Identity)

		if c.IdentityOptions != "" {
			value += fmt.Sprintf(" (%s)", c.IdentityOptions)
		}
	}

	if c.GeneratedExpression != "" {
		value += fmt.Sprintf(" GENERATED ALWAYS AS (%s) STORED", c.GeneratedExpression)
	}

	// The word serial gives the default.
	if c.Default.Valid && c.Serial == "" {
		value += fmt.Sprintf(" DEFAULT %s", c.Default.String)
	}

	return value
}

// The word serial gives a sequence with this name and these options. The condition reads
// the aliases sequence_class, sequence_options, owner_table, and owner_column. Another
// sequence that a column owns keeps its own statement, and the column keeps its default.
const serialSequenceCondition = `(
	sequence_class.relname = owner_table.relname || '_' || owner_column.attname || '_seq'
	AND sequence_options.seqtypid = owner_column.atttypid
	AND sequence_options.seqstart = 1
	AND sequence_options.seqincrement = 1
	AND sequence_options.seqmin = 1
	AND sequence_options.seqmax = CASE owner_column.atttypid
		WHEN 'smallint'::regtype THEN 32767
		WHEN 'integer'::regtype THEN 2147483647
		WHEN 'bigint'::regtype THEN 9223372036854775807
	END
	AND sequence_options.seqcache = 1
	AND NOT sequence_options.seqcycle
)`

func getTableColumns(ctx context.Context, db *sql.DB, table *PostgresTable) error {
	// format_type gives the exact type name, because information_schema.columns gives ARRAY
	// for an array column and USER-DEFINED for an enum column. The query removes the prefix
	// of the current schema from that name.
	//
	// pg_attrdef holds the expression of a stored generated column, and it holds the default
	// value of every other column. The CASE expressions separate the two, because PostgreSQL
	// refuses a DEFAULT expression that reads another column.
	//
	// An empty text of the identity options, of the storage mode, or of the statistics target
	// names a column that keeps the default of its type. PostgreSQL 16 writes -1 for the
	// default statistics target, and PostgreSQL 17 writes NULL. GetSequences marks the
	// sequence of a serial column, so the word of that column builds it again.
	columns, err := driversshared.QueryAll(ctx, db, `
			SELECT
				a.attname,
				regexp_replace(
					format_type(a.atttypid, a.atttypmod),
					'^' || quote_ident(current_schema()) || '\.',
					''
				),
				a.attnotnull,
				CASE WHEN a.attgenerated = '' THEN pg_get_expr(d.adbin, d.adrelid) END,
				CASE a.attidentity WHEN 'a' THEN 'ALWAYS' WHEN 'd' THEN 'BY DEFAULT' ELSE '' END,
				CASE WHEN a.attgenerated = 's' THEN pg_get_expr(d.adbin, d.adrelid) ELSE '' END,
				coalesce(column_collation.collname, ''),
				coalesce(col_description(a.attrelid, a.attnum), ''),
				CASE WHEN a.attstorage <> base_type.typstorage THEN
					CASE a.attstorage
						WHEN 'p' THEN 'PLAIN'
						WHEN 'e' THEN 'EXTERNAL'
						WHEN 'm' THEN 'MAIN'
						WHEN 'x' THEN 'EXTENDED'
					END
				ELSE '' END,
				CASE WHEN a.attstattarget >= 0 THEN a.attstattarget END,
				coalesce((
					SELECT nullif(concat_ws(' ',
						CASE WHEN s.seqstart <> s.seqmin THEN 'START WITH ' || s.seqstart END,
						CASE WHEN s.seqincrement <> 1 THEN 'INCREMENT BY ' || s.seqincrement END,
						CASE WHEN s.seqmin <> 1 THEN 'MINVALUE ' || s.seqmin END,
						CASE WHEN s.seqmax <> CASE
							WHEN s.seqtypid = 'smallint'::regtype THEN 32767
							WHEN s.seqtypid = 'integer'::regtype THEN 2147483647
							ELSE 9223372036854775807 END
							THEN 'MAXVALUE ' || s.seqmax END,
						CASE WHEN s.seqcache <> 1 THEN 'CACHE ' || s.seqcache END,
						CASE WHEN s.seqcycle THEN 'CYCLE' END
					), '')
					FROM pg_sequence s
					JOIN pg_depend dep ON dep.objid = s.seqrelid AND dep.deptype = 'i'
					WHERE dep.refobjid = a.attrelid AND dep.refobjsubid = a.attnum
				), ''),
				CASE WHEN serial_sequence.relname IS NOT NULL THEN
					CASE a.atttypid
						WHEN 'smallint'::regtype THEN 'smallserial'
						WHEN 'integer'::regtype THEN 'serial'
						WHEN 'bigint'::regtype THEN 'bigserial'
					END
				ELSE '' END,
				coalesce(serial_sequence.relname, ''),
				NOT a.attislocal
			FROM pg_attribute a
			LEFT JOIN LATERAL (
				SELECT sequence_class.relname
				FROM pg_depend dep
				JOIN pg_class sequence_class ON sequence_class.oid = dep.objid
					AND sequence_class.relkind = 'S'
				JOIN pg_sequence sequence_options ON sequence_options.seqrelid = sequence_class.oid
				JOIN pg_class owner_table ON owner_table.oid = dep.refobjid
				JOIN pg_attribute owner_column ON owner_column.attrelid = dep.refobjid
					AND owner_column.attnum = dep.refobjsubid
				WHERE dep.refobjid = a.attrelid
				AND dep.refobjsubid = a.attnum
				AND dep.deptype = 'a'
				AND `+serialSequenceCondition+`
			) serial_sequence ON true
			LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
			LEFT JOIN pg_type base_type ON base_type.oid = a.atttypid
			LEFT JOIN pg_collation column_collation ON column_collation.oid = a.attcollation
				AND a.attcollation <> base_type.typcollation
			WHERE a.attrelid = $1::regclass
			AND a.attnum > 0
			AND NOT a.attisdropped
			ORDER BY a.attnum
		`, func(rows *sql.Rows) (*PostgresColumn, error) {
		var columnName, columnType, identity, generatedExpression, collation, comment string
		var identityOptions, storage, serial, serialSequenceName string
		var notNull, inherited bool
		var columnDefault sql.NullString
		var statisticsTarget sql.NullInt64

		err := rows.Scan(&columnName, &columnType, &notNull, &columnDefault,
			&identity, &generatedExpression, &collation, &comment, &storage,
			&statisticsTarget, &identityOptions, &serial, &serialSequenceName, &inherited)
		if err != nil {
			return nil, err
		}

		return &PostgresColumn{
			Name:                columnName,
			Type:                columnType,
			NotNull:             notNull,
			Default:             columnDefault,
			Identity:            identity,
			GeneratedExpression: generatedExpression,
			Collation:           collation,
			Comment:             comment,
			IdentityOptions:     identityOptions,
			Storage:             storage,
			StatisticsTarget:    statisticsTarget,
			SerialSequenceName:  serialSequenceName,
			Serial:              serial,
			Inherited:           inherited,
		}, nil
	}, driversshared.QuoteIdentifier(table.Name))
	if err != nil {
		return err
	}

	table.Columns = append(table.Columns, columns...)

	return nil
}

type PostgresColumnDependent struct {
	ColumnName string
	Kind       string
	Name       string
}

// A DROP COLUMN statement removes the indexes and the constraints of the column. PostgreSQL
// refuses it while a view, a policy, a trigger, or a rule uses the column. The kind blocker
// names an object that the diff cannot drop and create again.
func getColumnDependents(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	columnNames []string,
) ([]*PostgresColumnDependent, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT DISTINCT attname, kind, name
		FROM (
			SELECT
				a.attname,
				CASE
					WHEN index_class.oid IS NOT NULL THEN 'index'
					WHEN table_constraint.oid IS NOT NULL THEN 'constraint'
					WHEN table_trigger.oid IS NOT NULL THEN 'trigger'
					WHEN table_policy.polrelid = d.refobjid THEN 'policy'
					WHEN rule_class.relkind IN ('v', 'm') AND r.rulename = '_RETURN'
						AND rule_class.relnamespace = current_schema()::regnamespace THEN 'relation'
					WHEN table_policy.oid IS NOT NULL OR r.oid IS NOT NULL THEN 'blocker'
				END AS kind,
				CASE
					WHEN index_class.oid IS NOT NULL THEN index_class.relname
					WHEN table_constraint.oid IS NOT NULL THEN table_constraint.conname
					WHEN table_trigger.oid IS NOT NULL THEN table_trigger.tgname
					WHEN table_policy.polrelid = d.refobjid THEN table_policy.polname
					WHEN rule_class.relkind IN ('v', 'm') AND r.rulename = '_RETURN'
						AND rule_class.relnamespace = current_schema()::regnamespace THEN rule_class.relname
					WHEN table_policy.oid IS NOT NULL THEN
						format('policy %I on %s', table_policy.polname, table_policy.polrelid::regclass)
					WHEN r.rulename = '_RETURN' THEN format('view %s', r.ev_class::regclass)
					ELSE format('rule %I on %s', r.rulename, r.ev_class::regclass)
				END AS name
			FROM pg_depend d
			JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
			LEFT JOIN pg_class index_class ON d.classid = 'pg_class'::regclass
				AND index_class.oid = d.objid
				AND index_class.relkind IN ('i', 'I')
			LEFT JOIN pg_constraint table_constraint ON d.classid = 'pg_constraint'::regclass
				AND table_constraint.oid = d.objid
				AND table_constraint.conrelid = d.refobjid
			LEFT JOIN pg_trigger table_trigger ON d.classid = 'pg_trigger'::regclass
				AND table_trigger.oid = d.objid
			LEFT JOIN pg_policy table_policy ON d.classid = 'pg_policy'::regclass
				AND table_policy.oid = d.objid
			LEFT JOIN pg_rewrite r ON d.classid = 'pg_rewrite'::regclass AND r.oid = d.objid
			LEFT JOIN pg_class rule_class ON rule_class.oid = r.ev_class
			WHERE d.refclassid = 'pg_class'::regclass
			AND d.refobjid = $1::regclass
			AND a.attname = ANY($2)
		) dependents
		WHERE kind IS NOT NULL
		ORDER BY attname, kind, name
	`, func(rows *sql.Rows) (*PostgresColumnDependent, error) {
		dependent := &PostgresColumnDependent{}

		err := rows.Scan(&dependent.ColumnName, &dependent.Kind, &dependent.Name)

		return dependent, err
	}, driversshared.QuoteIdentifier(tableName), columnNames)
}

type PostgresTypeDependent struct {
	Kind       string
	TableName  string
	ColumnName string
	IsArray    bool
	Name       string
}

// The kind blocker names an object that the diff cannot change. A column of a child table
// takes the type change of its parent, so the query skips it. The array type of the type
// and the constraints of a domain go with the type, so the query skips them too.
func getTypeDependents(ctx context.Context, db *sql.DB, typeName string) ([]*PostgresTypeDependent, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT DISTINCT kind, table_name, column_name, is_array, name
		FROM (
			SELECT
				CASE
					WHEN d.deptype IN ('i', 'a') THEN NULL
					WHEN column_class.relkind IN ('v', 'm')
						AND column_class.relnamespace = current_schema()::regnamespace THEN 'relation'
					WHEN column_class.relkind IN ('r', 'p')
						AND column_class.relnamespace = current_schema()::regnamespace THEN
						CASE WHEN column_attribute.attinhcount = 0 THEN 'column' END
					WHEN default_class.relnamespace = current_schema()::regnamespace THEN 'default'
					WHEN rule_class.relkind IN ('v', 'm') AND r.rulename = '_RETURN'
						AND rule_class.relnamespace = current_schema()::regnamespace THEN 'relation'
					ELSE 'blocker'
				END AS kind,
				coalesce(column_class.relname, default_class.relname, rule_class.relname, '') AS table_name,
				coalesce(column_attribute.attname, default_attribute.attname, '') AS column_name,
				d.refobjid = t.typarray AS is_array,
				pg_describe_object(d.classid, d.objid, d.objsubid) AS name
			FROM pg_type t
			JOIN pg_depend d ON d.refclassid = 'pg_type'::regclass AND d.refobjid IN (t.oid, t.typarray)
			LEFT JOIN pg_class column_class ON d.classid = 'pg_class'::regclass
				AND d.objsubid > 0
				AND column_class.oid = d.objid
			LEFT JOIN pg_attribute column_attribute ON column_attribute.attrelid = column_class.oid
				AND column_attribute.attnum = d.objsubid
			LEFT JOIN pg_attrdef column_default ON d.classid = 'pg_attrdef'::regclass
				AND column_default.oid = d.objid
			LEFT JOIN pg_class default_class ON default_class.oid = column_default.adrelid
			LEFT JOIN pg_attribute default_attribute ON default_attribute.attrelid = column_default.adrelid
				AND default_attribute.attnum = column_default.adnum
			LEFT JOIN pg_rewrite r ON d.classid = 'pg_rewrite'::regclass AND r.oid = d.objid
			LEFT JOIN pg_class rule_class ON rule_class.oid = r.ev_class
			WHERE t.typnamespace = current_schema()::regnamespace
			AND t.typname = $1
		) dependents
		WHERE kind IS NOT NULL
		ORDER BY kind, table_name, column_name, name
	`, func(rows *sql.Rows) (*PostgresTypeDependent, error) {
		dependent := &PostgresTypeDependent{}

		err := rows.Scan(&dependent.Kind, &dependent.TableName, &dependent.ColumnName,
			&dependent.IsArray, &dependent.Name)

		return dependent, err
	}, typeName)
}

func typeDependentError(objectName string, typeName string) error {
	return fmt.Errorf(
		"the %s uses the type %q. dbdiff must drop and create this type again, "+
			"and it cannot drop and create this object again",
		objectName, typeName)
}

// PostgreSQL refuses to drop a type while a column uses it. The diff renames the current
// type, creates the new type, and converts each column to the new type. The conversion keeps
// the rows. An intermediate type, for example text, carries the values when PostgreSQL holds
// no cast between the two types. If no object uses the type, the result is empty.
func (d *PostgresDriver) replaceUsedType(
	ctx context.Context,
	objectType string,
	typeName string,
	createInstructions []driversshared.Instruction,
	intermediateType string,
) ([]driversshared.Instruction, error) {
	dependents, err := getTypeDependents(ctx, d.CurrentDatabaseConnection, typeName)
	if err != nil || len(dependents) == 0 {
		return nil, err
	}

	oldName := "dbdiff_old_" + typeName
	instructions := []driversshared.Instruction{&PostgresRenameTypeInstruction{
		ObjectType: objectType,
		Name:       typeName,
		NewName:    oldName,
	}}
	instructions = append(instructions, createInstructions...)

	// A default of the old type cannot take the new type, so every default goes first.
	var conversions []driversshared.Instruction

	for _, dependent := range dependents {
		switch dependent.Kind {
		case "default":
			if d.droppedColumnDefaults == nil {
				d.droppedColumnDefaults = map[string][]string{}
			}

			d.droppedColumnDefaults[dependent.TableName] = append(
				d.droppedColumnDefaults[dependent.TableName], dependent.ColumnName)
			instructions = append(instructions, &PostgresAlterTableInstruction{
				Name: dependent.TableName,
				Actions: []driversshared.AlterTableAction{
					&PostgresDropDefaultAction{
						ColumnName: dependent.ColumnName,
					},
				},
			})
		case "column":
			conversion, err := d.columnTypeConversion(ctx, dependent, typeName, intermediateType)
			if err != nil {
				return nil, err
			}

			conversions = append(conversions, conversion)
		case "relation":
			d.markRecreated(dependent.TableName)
		default:
			return nil, typeDependentError(dependent.Name, typeName)
		}
	}

	instructions = append(instructions, conversions...)

	if objectType == "DOMAIN" {
		return append(instructions, &PostgresDropDomainInstruction{
			Name: oldName,
		}), nil
	}

	return append(instructions, &PostgresDropTypeInstruction{
		Name: oldName,
	}), nil
}

// PostgreSQL rebuilds the indexes and the constraints of the column, and it refuses the
// change while a policy, a trigger, or a rule uses the column.
func (d *PostgresDriver) columnTypeConversion(
	ctx context.Context,
	dependent *PostgresTypeDependent,
	typeName string,
	intermediateType string,
) (driversshared.Instruction, error) {
	columnDependents, err := getColumnDependents(ctx, d.CurrentDatabaseConnection,
		dependent.TableName, []string{dependent.ColumnName})
	if err != nil {
		return nil, err
	}

	for _, columnDependent := range columnDependents {
		switch columnDependent.Kind {
		case "index", "constraint":
		case "relation":
			d.markRecreated(columnDependent.Name)
		default:
			return nil, typeDependentError(columnDependent.Kind+" "+columnDependent.Name, typeName)
		}
	}

	dataType := driversshared.QuoteIdentifier(typeName)

	if dependent.IsArray {
		dataType += "[]"

		if intermediateType != "" {
			intermediateType += "[]"
		}
	}

	return &PostgresAlterTableInstruction{
		Name: dependent.TableName,
		Actions: []driversshared.AlterTableAction{
			&PostgresAlterColumnTypeAction{
				ColumnName:       dependent.ColumnName,
				DataType:         dataType,
				UsingCast:        true,
				IntermediateType: intermediateType,
			},
		},
	}, nil
}
