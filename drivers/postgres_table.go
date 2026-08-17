package drivers

import (
	"fmt"
	"strings"
)

type PostgresTable struct {
	Name        string
	Columns     []*PostgresColumn
	Indexes     []*PostgresIndex
	Constraints []*PostgresConstraint
	Triggers    []*PostgresTrigger
}

func (t *PostgresTable) ColumnByName(name string) (*PostgresColumn, bool) {
	for _, column := range t.Columns {
		if column.Name == name {
			return column, true
		}
	}

	return nil, false
}

func (t *PostgresTable) DiffTable(other *PostgresTable, hasAutomaticCast AutomaticCastLookup) (string, error) {
	var diff strings.Builder

	for _, sourceColumn := range t.Columns {
		targetColumn, found := other.ColumnByName(sourceColumn.Name)
		if !found {
			fmt.Fprintf(&diff, "ALTER TABLE %s ADD COLUMN %s;\n", quoteIdentifier(t.Name), sourceColumn.String())
			continue
		}

		if !sourceColumn.HasEqualAttributes(targetColumn) {
			if sourceColumn.Type != targetColumn.Type {
				usingClause, err := columnUsingClause(sourceColumn, targetColumn, hasAutomaticCast)
				if err != nil {
					return "", err
				}

				fmt.Fprintf(&diff, "ALTER TABLE %s ALTER COLUMN %s TYPE %s%s;\n", quoteIdentifier(t.Name), quoteIdentifier(sourceColumn.Name), sourceColumn.Type, usingClause)
			}

			if sourceColumn.NotNull != targetColumn.NotNull {
				if sourceColumn.NotNull {
					fmt.Fprintf(&diff, "ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;\n", quoteIdentifier(t.Name), quoteIdentifier(sourceColumn.Name))
				} else {
					fmt.Fprintf(&diff, "ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;\n", quoteIdentifier(t.Name), quoteIdentifier(sourceColumn.Name))
				}
			}

			if sourceColumn.Default != targetColumn.Default {
				if sourceColumn.Default.Valid {
					fmt.Fprintf(&diff, "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;\n", quoteIdentifier(t.Name), quoteIdentifier(sourceColumn.Name), sourceColumn.Default.String)
				} else {
					fmt.Fprintf(&diff, "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;\n", quoteIdentifier(t.Name), quoteIdentifier(sourceColumn.Name))
				}
			}
		}
	}

	// PostgreSQL drops every constraint and every index of a column with the column, so
	// these two blocks come before the column removals below.
	for _, sourceConstraint := range t.Constraints {
		targetConstraint, found := other.ConstraintByName(sourceConstraint.Name)
		if !found {
			fmt.Fprintf(&diff, "ALTER TABLE %s ADD %s;\n", quoteIdentifier(t.Name), sourceConstraint.String())
			continue
		}

		if sourceConstraint.Def != targetConstraint.Def {
			fmt.Fprintf(&diff, "ALTER TABLE %s DROP CONSTRAINT %s;\n", quoteIdentifier(t.Name), quoteIdentifier(targetConstraint.Name))
			fmt.Fprintf(&diff, "ALTER TABLE %s ADD %s;\n", quoteIdentifier(t.Name), sourceConstraint.String())
		}
	}

	for _, targetConstraint := range other.Constraints {
		_, found := t.ConstraintByName(targetConstraint.Name)
		if !found {
			fmt.Fprintf(&diff, "ALTER TABLE %s DROP CONSTRAINT %s;\n", quoteIdentifier(t.Name), quoteIdentifier(targetConstraint.Name))
		}
	}

	for _, sourceIndex := range t.Indexes {
		targetIndex, found := other.IndexByName(sourceIndex.Name)
		if !found {
			fmt.Fprintf(&diff, "%s\n", sourceIndex.String())
			continue
		}

		if sourceIndex.Def != targetIndex.Def {
			fmt.Fprintf(&diff, "DROP INDEX %s;\n", quoteIdentifier(targetIndex.Name))
			fmt.Fprintf(&diff, "%s\n", sourceIndex.String())
		}
	}

	for _, targetIndex := range other.Indexes {
		_, found := t.IndexByName(targetIndex.Name)
		if !found {
			fmt.Fprintf(&diff, "DROP INDEX %s;\n", quoteIdentifier(targetIndex.Name))
		}
	}

	for _, targetColumn := range other.Columns {
		_, found := t.ColumnByName(targetColumn.Name)
		if !found {
			fmt.Fprintf(&diff, "ALTER TABLE %s DROP COLUMN %s;\n", quoteIdentifier(t.Name), quoteIdentifier(targetColumn.Name))
		}
	}

	for _, sourceTrigger := range t.Triggers {
		targetTrigger, found := other.TriggerByName(sourceTrigger.Name)
		if !found {
			fmt.Fprintf(&diff, "%s\n", sourceTrigger.String())
			continue
		}

		if sourceTrigger.Def != targetTrigger.Def {
			fmt.Fprintf(&diff, "DROP TRIGGER %s ON %s;\n", quoteIdentifier(targetTrigger.Name), quoteIdentifier(t.Name))
			fmt.Fprintf(&diff, "%s\n", sourceTrigger.String())
		}
	}

	for _, targetTrigger := range other.Triggers {
		_, found := t.TriggerByName(targetTrigger.Name)
		if !found {
			fmt.Fprintf(&diff, "DROP TRIGGER %s ON %s;\n", quoteIdentifier(targetTrigger.Name), quoteIdentifier(t.Name))
		}
	}

	return strings.TrimSpace(diff.String()), nil
}

func (t *PostgresTable) ConstraintByName(name string) (*PostgresConstraint, bool) {
	for _, c := range t.Constraints {
		if c.Name == name {
			return c, true
		}
	}

	return nil, false
}

func (t *PostgresTable) IndexByName(name string) (*PostgresIndex, bool) {
	for _, i := range t.Indexes {
		if i.Name == name {
			return i, true
		}
	}

	return nil, false
}

func (t *PostgresTable) TriggerByName(name string) (*PostgresTrigger, bool) {
	for _, tr := range t.Triggers {
		if tr.Name == name {
			return tr, true
		}
	}

	return nil, false
}

func (t *PostgresTable) StringCreateTable() string {
	var columnLines []string

	for _, column := range t.Columns {
		line := "\t" + column.String()
		columnLines = append(columnLines, line)
	}

	for _, constraint := range t.Constraints {
		line := "\t" + constraint.String()
		columnLines = append(columnLines, line)
	}

	createTableColumns := strings.Join(columnLines, ",\n")
	return fmt.Sprintf("CREATE TABLE %s (\n%s\n);", quoteIdentifier(t.Name), createTableColumns)
}

func (t *PostgresTable) String() string {
	statement := t.StringCreateTable()

	for _, index := range t.Indexes {
		statement += "\n" + index.String()
	}

	for _, trigger := range t.Triggers {
		statement += "\n" + trigger.String()
	}

	return statement
}
