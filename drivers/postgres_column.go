package drivers

import (
	"database/sql"
	"fmt"
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

	Collation string

	// Definition writes no comment, because a column definition accepts none.
	Comment string

	Storage string

	StatisticsTarget sql.NullInt64
}

// A column definition accepts no storage mode before PostgreSQL 16, so a separate statement
func (c *PostgresColumn) StorageInstructions(tableName string) []Instruction {
	if c.Storage == "" {
		return nil
	}

	return []Instruction{&PostgresAlterTableInstruction{
		Name: tableName,
		Actions: []AlterTableAction{&PostgresSetStorageAction{
			ColumnName: c.Name,
			Storage:    c.Storage,
		}},
	}}
}

// A column definition accepts no statistics source, so a separate statement holds it. The
func (c *PostgresColumn) StatisticsInstructions(tableName string) []Instruction {
	if !c.StatisticsTarget.Valid {
		return nil
	}

	return []Instruction{&PostgresAlterTableInstruction{
		Name: tableName,
		Actions: []AlterTableAction{&PostgresSetStatisticsAction{
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

	return *copy == *other
}

func (c *PostgresColumn) Definition() string {
	dataType := c.Type
	if c.Serial != "" {
		dataType = c.Serial
	}

	value := fmt.Sprintf("%s %s", QuoteIdentifier(c.Name), dataType)

	if c.Collation != "" {
		value += " COLLATE " + QuoteIdentifier(c.Collation)
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
