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

// A column definition accepts no statistics source, so a separate statement holds it. The
func (c *PostgresColumn) StatisticsInstructions(tableName string) []driversshared.Instruction {
	if !c.StatisticsTarget.Valid {
		return nil
	}

	return []driversshared.Instruction{&PostgresAlterTableInstruction{
		Name: tableName,
		Actions: []driversshared.AlterTableAction{&PostgresSetStatisticsAction{
			ColumnName: c.Name,
			Source:     c.StatisticsTarget.Int64,
		}},
	}}
}

func (c *PostgresColumn) Copy() *PostgresColumn {
	columnCopy := *c
	return &columnCopy
}

func (c *PostgresColumn) HasEqualAttributes(other *PostgresColumn) bool {
	copy := c.Copy()
	copy.Name = other.Name

	// A rename of the table keeps the old sequence name, and the name alone changes
	// no behavior.
	copy.SerialSequenceName = other.SerialSequenceName

	return *copy == *other
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
		value += fmt.Sprintf(" GENERATED ALWAYS AS %s STORED", c.GeneratedExpression)
	}

	if c.Default.Valid {
		value += fmt.Sprintf(" DEFAULT %s", c.Default.String)
	}

	return value
}

func getTableColumns(ctx context.Context, db *sql.DB, table *PostgresTable) error {
	// format_type gives the exact type name, because information_schema.columns gives ARRAY
	// for an array column and USER-DEFINED for an enum column. The query removes the prefix
	// of the current schema from that name.
	//
	// pg_attrdef holds the expression of a stored generated column, and it holds the default
	// value of every other column. The CASE expressions separate the two, because PostgreSQL
	// refuses a DEFAULT expression that reads another column.
	//
	// An empty text of the identity options, of the storage mode, or of the statistics source
	// names a column that keeps the default of its type. PostgreSQL 16 writes -1 for the
	// default statistics source, and PostgreSQL 17 writes NULL. GetSequences excludes the
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
				coalesce((
					SELECT CASE a.atttypid
						WHEN 'smallint'::regtype THEN 'smallserial'
						WHEN 'integer'::regtype THEN 'serial'
						WHEN 'bigint'::regtype THEN 'bigserial'
					END
					FROM pg_depend dep
					JOIN pg_class sequence_class ON sequence_class.oid = dep.objid
						AND sequence_class.relkind = 'S'
					WHERE dep.refobjid = a.attrelid
					AND dep.refobjsubid = a.attnum
					AND dep.deptype = 'a'
				), ''),
				coalesce((
					SELECT sequence_class.relname
					FROM pg_depend dep
					JOIN pg_class sequence_class ON sequence_class.oid = dep.objid
						AND sequence_class.relkind = 'S'
					WHERE dep.refobjid = a.attrelid
					AND dep.refobjsubid = a.attnum
					AND dep.deptype = 'a'
				), '')
			FROM pg_attribute a
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
		var notNull bool
		var columnDefault sql.NullString
		var statisticsTarget sql.NullInt64

		err := rows.Scan(&columnName, &columnType, &notNull, &columnDefault,
			&identity, &generatedExpression, &collation, &comment, &storage,
			&statisticsTarget, &identityOptions, &serial, &serialSequenceName)
		if err != nil {
			return nil, err
		}

		// The word serial holds the default, so the definition writes no DEFAULT clause.
		if serial != "" {
			columnDefault = sql.NullString{}
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
		}, nil
	}, driversshared.QuoteIdentifier(table.Name))
	if err != nil {
		return err
	}

	table.Columns = append(table.Columns, columns...)

	return nil
}
