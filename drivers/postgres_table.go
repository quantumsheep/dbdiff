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

func (t *PostgresTable) DiffTable(other *PostgresTable) (string, error) {
	var diff strings.Builder

	// Added or modified columns
	for _, sourceColumn := range t.Columns {
		targetColumn, found := other.ColumnByName(sourceColumn.Name)
		if !found {
			fmt.Fprintf(&diff, "ALTER TABLE \"%s\" ADD COLUMN %s;\n", t.Name, sourceColumn.String())
			continue
		}

		if !sourceColumn.HasEqualAttributes(targetColumn) {
			// Type change
			if sourceColumn.Type != targetColumn.Type {
				// Using USING clause might be needed for some conversions, but keeping it simple as requested.
				fmt.Fprintf(&diff, "ALTER TABLE \"%s\" ALTER COLUMN \"%s\" TYPE %s;\n", t.Name, sourceColumn.Name, sourceColumn.Type)
			}

			// Not Null change
			if sourceColumn.NotNull != targetColumn.NotNull {
				if sourceColumn.NotNull {
					fmt.Fprintf(&diff, "ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET NOT NULL;\n", t.Name, sourceColumn.Name)
				} else {
					fmt.Fprintf(&diff, "ALTER TABLE \"%s\" ALTER COLUMN \"%s\" DROP NOT NULL;\n", t.Name, sourceColumn.Name)
				}
			}

			// Default change
			if sourceColumn.Default != targetColumn.Default {
				if sourceColumn.Default.Valid {
					fmt.Fprintf(&diff, "ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET DEFAULT %s;\n", t.Name, sourceColumn.Name, sourceColumn.Default.String)
				} else {
					fmt.Fprintf(&diff, "ALTER TABLE \"%s\" ALTER COLUMN \"%s\" DROP DEFAULT;\n", t.Name, sourceColumn.Name)
				}
			}
		}
	}

	// Removed columns
	for _, targetColumn := range other.Columns {
		_, found := t.ColumnByName(targetColumn.Name)
		if !found {
			fmt.Fprintf(&diff, "ALTER TABLE \"%s\" DROP COLUMN \"%s\";\n", t.Name, targetColumn.Name)
		}
	}

	// Constraints
	for _, sourceConstraint := range t.Constraints {
		targetConstraint, found := other.ConstraintByName(sourceConstraint.Name)
		if !found {
			fmt.Fprintf(&diff, "ALTER TABLE \"%s\" ADD %s;\n", t.Name, sourceConstraint.String())
			continue
		}
		if sourceConstraint.Def != targetConstraint.Def {
			fmt.Fprintf(&diff, "ALTER TABLE \"%s\" DROP CONSTRAINT \"%s\";\n", t.Name, targetConstraint.Name)
			fmt.Fprintf(&diff, "ALTER TABLE \"%s\" ADD %s;\n", t.Name, sourceConstraint.String())
		}
	}
	for _, targetConstraint := range other.Constraints {
		_, found := t.ConstraintByName(targetConstraint.Name)
		if !found {
			fmt.Fprintf(&diff, "ALTER TABLE \"%s\" DROP CONSTRAINT \"%s\";\n", t.Name, targetConstraint.Name)
		}
	}

	// Indexes
	for _, sourceIndex := range t.Indexes {
		targetIndex, found := other.IndexByName(sourceIndex.Name)
		if !found {
			fmt.Fprintf(&diff, "%s\n", sourceIndex.String())
			continue
		}
		if sourceIndex.Def != targetIndex.Def {
			fmt.Fprintf(&diff, "DROP INDEX \"%s\";\n", targetIndex.Name)
			fmt.Fprintf(&diff, "%s\n", sourceIndex.String())
		}
	}
	for _, targetIndex := range other.Indexes {
		_, found := t.IndexByName(targetIndex.Name)
		if !found {
			fmt.Fprintf(&diff, "DROP INDEX \"%s\";\n", targetIndex.Name)
		}
	}

	// Triggers
	for _, sourceTrigger := range t.Triggers {
		targetTrigger, found := other.TriggerByName(sourceTrigger.Name)
		if !found {
			fmt.Fprintf(&diff, "%s\n", sourceTrigger.String())
			continue
		}
		if sourceTrigger.Def != targetTrigger.Def {
			fmt.Fprintf(&diff, "DROP TRIGGER \"%s\" ON \"%s\";\n", targetTrigger.Name, t.Name)
			fmt.Fprintf(&diff, "%s\n", sourceTrigger.String())
		}
	}
	for _, targetTrigger := range other.Triggers {
		_, found := t.TriggerByName(targetTrigger.Name)
		if !found {
			fmt.Fprintf(&diff, "DROP TRIGGER \"%s\" ON \"%s\";\n", targetTrigger.Name, t.Name)
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
	return fmt.Sprintf("CREATE TABLE \"%s\" (\n%s\n);", t.Name, createTableColumns)
}

func (t *PostgresTable) String() string {
	str := t.StringCreateTable()

	for _, index := range t.Indexes {
		str += "\n" + index.String()
	}

	for _, trigger := range t.Triggers {
		str += "\n" + trigger.String()
	}

	return str
}
