package drivers

import (
	"fmt"
	"slices"
	"strings"

	"github.com/samber/lo"
)

// equalColumnGroups compares two lists of column groups. The two lists hold their groups in
// the same order, because each read method sorts them.
func equalColumnGroups(first [][]string, second [][]string) bool {
	return slices.EqualFunc(first, second, func(a []string, b []string) bool {
		return slices.Equal(a, b)
	})
}

type SQLiteTable struct {
	Name    string
	Columns []*SQLiteColumn

	// PrimaryKey holds the columns of a primary key of two or more columns, in the order of
	// the key. A primary key of one column stays a column constraint, so this field is
	// empty for such a key.
	PrimaryKey []string

	// UniqueConstraints holds the columns of each UNIQUE constraint of two or more columns.
	// A UNIQUE constraint of one column stays a column constraint, so this field holds no
	// entry for such a constraint.
	UniqueConstraints [][]string

	Indexes     []*SQLiteIndex
	Triggers    []*SQLiteTrigger
	ForeignKeys []*SQLiteForeignKey
}

func (t *SQLiteTable) Copy() *SQLiteTable {
	new := *t
	return &new
}

func (t *SQLiteTable) ColumnByName(name string) (*SQLiteColumn, bool) {
	for _, column := range t.Columns {
		if column.Name == name {
			return column, true
		}
	}

	return nil, false
}

func (t *SQLiteTable) IndexByName(name string) (*SQLiteIndex, bool) {
	for _, index := range t.Indexes {
		if index.Name == name {
			return index, true
		}
	}

	return nil, false
}

func (t *SQLiteTable) TriggerByName(name string) (*SQLiteTrigger, bool) {
	for _, trigger := range t.Triggers {
		if trigger.Name == name {
			return trigger, true
		}
	}

	return nil, false
}

func (t *SQLiteTable) StringCreateTable() string {
	var columnLines []string

	for _, column := range t.Columns {
		line := "\t" + column.String()
		columnLines = append(columnLines, line)
	}

	if len(t.PrimaryKey) > 0 {
		line := fmt.Sprintf("\tPRIMARY KEY (%s)", strings.Join(quoteIdentifiers(t.PrimaryKey), ", "))
		columnLines = append(columnLines, line)
	}

	for _, constraint := range t.UniqueConstraints {
		line := fmt.Sprintf("\tUNIQUE (%s)", strings.Join(quoteIdentifiers(constraint), ", "))
		columnLines = append(columnLines, line)
	}

	for _, fk := range t.ForeignKeys {
		line := "\t" + fk.String()
		columnLines = append(columnLines, line)
	}

	createTableColumns := strings.Join(columnLines, ",\n")
	return fmt.Sprintf("CREATE TABLE %s (\n%s\n);", quoteIdentifier(t.Name), createTableColumns)
}

func (t *SQLiteTable) StringCreateIndexes() string {
	var createIndexes []string

	for _, index := range t.Indexes {
		createIndexes = append(createIndexes, index.String())
	}

	return strings.Join(createIndexes, "\n")
}

func (t *SQLiteTable) StringCreateTriggers() string {
	var createTriggers []string

	for _, trigger := range t.Triggers {
		createTriggers = append(createTriggers, trigger.SQL+";")
	}

	return strings.Join(createTriggers, "\n")
}

func (t *SQLiteTable) String() string {
	str := t.StringCreateTable()

	indexes := t.StringCreateIndexes()
	if indexes != "" {
		str += "\n" + indexes
	}

	triggers := t.StringCreateTriggers()
	if triggers != "" {
		str += "\n" + triggers
	}

	return str
}

type SQLiteTableColumnsDiff struct {
	Added    []string
	Modified []string
	Removed  []string
	Renamed  map[string]string // oldName -> newName

	ForeignKeysChanged bool

	// ConstraintsChanged is true when the primary key of two or more columns changes, or
	// when a UNIQUE constraint of two or more columns changes. SQLite adds no such
	// constraint to a table, so the change needs a recreation of the table.
	ConstraintsChanged bool
}

func (t *SQLiteTable) DiffColumns(other *SQLiteTable) *SQLiteTableColumnsDiff {
	diff := &SQLiteTableColumnsDiff{
		Added:              []string{},
		Modified:           []string{},
		Removed:            []string{},
		Renamed:            make(map[string]string),
		ForeignKeysChanged: false,
		ConstraintsChanged: !slices.Equal(t.PrimaryKey, other.PrimaryKey) || !equalColumnGroups(t.UniqueConstraints, other.UniqueConstraints),
	}

	for _, sourceColumn := range t.Columns {
		targetColumn, found := other.ColumnByName(sourceColumn.Name)

		// New column
		if !found {
			// Maybe it's a renamed column?
			candidates := lo.Filter(other.Columns, func(c *SQLiteColumn, _ int) bool {
				_, existsInSourceTable := t.ColumnByName(c.Name)
				_, alreadyRenamed := diff.Renamed[c.Name]
				return !existsInSourceTable && !alreadyRenamed && c.HasEqualAttributes(sourceColumn)
			})

			// A rename is a guess. Several candidates make the guess wrong, so the
			// column becomes an addition and the old columns become removals.
			if len(candidates) == 1 {
				diff.Renamed[candidates[0].Name] = sourceColumn.Name
				continue
			}

			diff.Added = append(diff.Added, sourceColumn.Name)
			continue
		}

		if *sourceColumn == *targetColumn {
			continue
		}

		if sourceColumn.Type != targetColumn.Type {
			// Type change to compatible type should be done in table recreation
			if sourceColumn.IsTypeChangeCompatible(targetColumn) {
				diff.Modified = append(diff.Modified, sourceColumn.Name)
				continue
			}

			diff.Removed = append(diff.Removed, targetColumn.Name)
			diff.Added = append(diff.Added, sourceColumn.Name)
			continue
		}

		diff.Modified = append(diff.Modified, sourceColumn.Name)
	}

	// Removed columns
	for _, targetColumn := range other.Columns {
		_, found := t.ColumnByName(targetColumn.Name)
		if !found && !lo.Contains(lo.Keys(diff.Renamed), targetColumn.Name) {
			diff.Removed = append(diff.Removed, targetColumn.Name)
		}
	}

	// Check if foreign keys changed
	if len(t.ForeignKeys) != len(other.ForeignKeys) {
		diff.ForeignKeysChanged = true
	} else {
		for _, sourceForeignKey := range t.ForeignKeys {
			found := lo.SomeBy(other.ForeignKeys, func(fk *SQLiteForeignKey) bool {
				return fk.Equal(sourceForeignKey)
			})
			if !found {
				diff.ForeignKeysChanged = true
				break
			}
		}
	}

	return diff
}

func (t *SQLiteTable) DiffTable(other *SQLiteTable) (string, error) {
	columnsDiff := t.DiffColumns(other)

	var diff strings.Builder

	// Modified columns or Foreign Keys need to be handled via table recreation
	if len(columnsDiff.Modified) > 0 || columnsDiff.ForeignKeysChanged || columnsDiff.ConstraintsChanged {
		tempTable := t.Copy()
		tempTable.Name = "_" + t.Name + "_temp"

		// Create temp table (table only; indexes recreated after rename)
		fmt.Fprintf(&diff, "%s\n", tempTable.StringCreateTable())

		// Reverse rename map: newName -> oldName
		newToOld := lo.Invert(columnsDiff.Renamed)

		// Build INSERT column list (new schema) and SELECT expressions (from old schema)
		var insertColumns []string
		var selectColumns []string

		for _, newCol := range t.Columns {
			insertColumns = append(insertColumns, quoteIdentifier(newCol.Name))

			// If the column existed before (same name), copy from old table
			_, ok := other.ColumnByName(newCol.Name)
			if ok {
				selectColumns = append(selectColumns, quoteIdentifier(newCol.Name))
				continue
			}

			// If it was renamed, copy from old name
			oldName, ok := newToOld[newCol.Name]
			if ok {
				selectColumns = append(selectColumns, quoteIdentifier(oldName))
				continue
			}

			// Otherwise it is a new column: use DEFAULT if present, else NULL
			if newCol.Default.Valid {
				selectColumns = append(selectColumns, newCol.Default.String)
			} else {
				selectColumns = append(selectColumns, "NULL")
			}
		}

		// Copy data from old table to new temp table with explicit mapping
		fmt.Fprintf(
			&diff,
			"INSERT INTO %s (%s) SELECT %s FROM %s;\n",
			quoteIdentifier(tempTable.Name),
			strings.Join(insertColumns, ", "),
			strings.Join(selectColumns, ", "),
			quoteIdentifier(t.Name),
		)

		// Drop old table
		fmt.Fprintf(&diff, "DROP TABLE %s;\n", quoteIdentifier(t.Name))

		// Rename new table to old table's name
		fmt.Fprintf(&diff, "ALTER TABLE %s RENAME TO %s;\n", quoteIdentifier(tempTable.Name), quoteIdentifier(t.Name))

		// Recreate indexes (on final table name)
		for _, idx := range t.Indexes {
			fmt.Fprintf(&diff, "%s\n", idx.String())
		}
	} else {
		for oldName, newName := range columnsDiff.Renamed {
			fmt.Fprintf(&diff, "ALTER TABLE %s RENAME COLUMN %s TO %s;\n", quoteIdentifier(t.Name), quoteIdentifier(oldName), quoteIdentifier(newName))
		}

		for _, columnName := range columnsDiff.Removed {
			fmt.Fprintf(&diff, "ALTER TABLE %s DROP COLUMN %s;\n", quoteIdentifier(t.Name), quoteIdentifier(columnName))
		}

		for _, columnName := range columnsDiff.Added {
			column, ok := t.ColumnByName(columnName)
			if !ok {
				return "", fmt.Errorf("internal error: added column %s not found in table %s", columnName, t.Name)
			}

			fmt.Fprintf(&diff, "ALTER TABLE %s ADD COLUMN %s;\n", quoteIdentifier(t.Name), column.String())
		}

	}

	return strings.TrimSpace(diff.String()), nil
}

func (t *SQLiteTable) DiffTriggers(other *SQLiteTable) (string, error) {
	var diff strings.Builder

	for _, sourceTrigger := range t.Triggers {
		targetTrigger, found := other.TriggerByName(sourceTrigger.Name)
		if !found {
			// New trigger
			fmt.Fprintf(&diff, "%s;\n", sourceTrigger.SQL)
			continue
		}

		if sourceTrigger.SQL != targetTrigger.SQL {
			// Modified trigger: drop and recreate
			fmt.Fprintf(&diff, "DROP TRIGGER %s;\n", quoteIdentifier(targetTrigger.Name))
			fmt.Fprintf(&diff, "%s;\n", sourceTrigger.SQL)
		}
	}

	for _, targetTrigger := range other.Triggers {
		_, found := t.TriggerByName(targetTrigger.Name)
		if !found {
			// Removed trigger
			fmt.Fprintf(&diff, "DROP TRIGGER %s;\n", quoteIdentifier(targetTrigger.Name))
		}
	}

	return diff.String(), nil
}

func (t *SQLiteTable) DiffIndexes(other *SQLiteTable) (string, error) {
	var diff strings.Builder

	for _, sourceIndex := range t.Indexes {
		targetIndex, found := other.IndexByName(sourceIndex.Name)
		if !found {
			// New index
			fmt.Fprintf(&diff, "%s\n", sourceIndex.String())
			continue
		}

		if !sourceIndex.Equal(targetIndex) {
			// Modified index: drop and recreate
			fmt.Fprintf(&diff, "DROP INDEX %s;\n", quoteIdentifier(targetIndex.Name))
			fmt.Fprintf(&diff, "%s\n", sourceIndex.String())
		}
	}

	for _, targetIndex := range other.Indexes {
		_, found := t.IndexByName(targetIndex.Name)
		if !found {
			// Removed index
			fmt.Fprintf(&diff, "DROP INDEX %s;\n", quoteIdentifier(targetIndex.Name))
		}
	}

	return diff.String(), nil
}
