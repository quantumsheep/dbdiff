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

	// Identity holds ALWAYS or BY DEFAULT for an identity column. It is empty for every
	// other column.
	Identity string

	// IdentityOptions holds the options of the sequence of an identity column, for example
	// "START WITH 100 INCREMENT BY 5". It is empty when every option keeps its default.
	IdentityOptions string

	// GeneratedExpression holds the expression of a stored generated column. It is empty
	// for every other column.
	GeneratedExpression string

	// Collation names the collation of the column. It is empty when the column keeps the
	// collation of its type.
	Collation string

	// Comment holds the comment of the column. Definition writes no comment, because a
	// column definition accepts none.
	Comment string

	// Storage names the storage mode of the column: PLAIN, EXTERNAL, EXTENDED, or MAIN.
	// The mode tells PostgreSQL to compress the value of the column, or to move it into a
	// TOAST table. It is empty when the column keeps the mode of its type.
	Storage string

	// StatisticsTarget holds the statistics target of the column. It is absent when the
	// column keeps the default target of the server.
	StatisticsTarget sql.NullInt64
}

// StorageInstructions returns the statement that sets the storage mode of the column. A
// column definition accepts no storage mode before PostgreSQL 16, so a separate statement
// holds it. The statement is absent when the column keeps the mode of its type.
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

// StatisticsInstructions returns the statement that sets the statistics target of the
// column. A column definition accepts no statistics target. The statement is absent when
// the column keeps the default target of the server.
func (c *PostgresColumn) StatisticsInstructions(tableName string) []Instruction {
	if !c.StatisticsTarget.Valid {
		return nil
	}

	return []Instruction{&PostgresAlterTableInstruction{
		Name: tableName,
		Actions: []AlterTableAction{&PostgresSetStatisticsAction{
			ColumnName: c.Name,
			Target:     c.StatisticsTarget.Int64,
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
	value := fmt.Sprintf("%s %s", quoteIdentifier(c.Name), c.Type)

	if c.Collation != "" {
		value += " COLLATE " + quoteIdentifier(c.Collation)
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
