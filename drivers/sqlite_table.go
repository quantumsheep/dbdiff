package drivers

import (
	"fmt"
	"slices"
	"strings"

	"github.com/samber/lo"
)

func equalColumnGroups(first [][]string, second [][]string) bool {
	return slices.EqualFunc(first, second, func(firstGroup []string, secondGroup []string) bool {
		return slices.Equal(firstGroup, secondGroup)
	})
}

type SQLiteTable struct {
	Name    string
	Columns []*SQLiteColumn

	// A key or a constraint of one column stays a column constraint. These two fields hold
	// the columns of a key or of a constraint of two or more columns only.
	PrimaryKey        []string
	UniqueConstraints [][]string

	Indexes     []*SQLiteIndex
	Triggers    []*SQLiteTrigger
	ForeignKeys []*SQLiteForeignKey
}

func (t *SQLiteTable) Copy() *SQLiteTable {
	tableCopy := *t
	return &tableCopy
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
		line := "\t" + column.Definition()
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

	for _, foreignKey := range t.ForeignKeys {
		line := "\t" + foreignKey.Clause()
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
	statement := t.StringCreateTable()

	indexes := t.StringCreateIndexes()
	if indexes != "" {
		statement += "\n" + indexes
	}

	triggers := t.StringCreateTriggers()
	if triggers != "" {
		statement += "\n" + triggers
	}

	return statement
}

type SQLiteTableColumnsDiff struct {
	Added    []string
	Modified []string
	Removed  []string
	Renamed  map[string]string

	ForeignKeysChanged bool
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
		if !found {
			candidates := lo.Filter(other.Columns, func(column *SQLiteColumn, _ int) bool {
				_, existsInSourceTable := t.ColumnByName(column.Name)
				_, alreadyRenamed := diff.Renamed[column.Name]
				return !existsInSourceTable && !alreadyRenamed && column.HasEqualAttributes(sourceColumn)
			})

			// A rename is a guess. Two candidates make the guess unsafe, so the column
			// becomes an addition and the old columns become removals.
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

	for _, targetColumn := range other.Columns {
		_, found := t.ColumnByName(targetColumn.Name)
		if !found && !lo.Contains(lo.Keys(diff.Renamed), targetColumn.Name) {
			diff.Removed = append(diff.Removed, targetColumn.Name)
		}
	}

	if len(t.ForeignKeys) != len(other.ForeignKeys) {
		diff.ForeignKeysChanged = true
	} else {
		for _, sourceForeignKey := range t.ForeignKeys {
			found := lo.SomeBy(other.ForeignKeys, func(foreignKey *SQLiteForeignKey) bool {
				return foreignKey.Equal(sourceForeignKey)
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

	// SQLite supports no ALTER COLUMN, so a modified column, a new foreign key, or a new
	// table constraint needs a recreation of the table.
	if len(columnsDiff.Modified) > 0 || columnsDiff.ForeignKeysChanged || columnsDiff.ConstraintsChanged {
		tempTable := t.Copy()
		tempTable.Name = "_" + t.Name + "_temp"

		fmt.Fprintf(&diff, "%s\n", tempTable.StringCreateTable())

		newToOld := lo.Invert(columnsDiff.Renamed)

		var insertColumns []string
		var selectColumns []string

		for _, newColumn := range t.Columns {
			insertColumns = append(insertColumns, quoteIdentifier(newColumn.Name))

			_, ok := other.ColumnByName(newColumn.Name)
			if ok {
				selectColumns = append(selectColumns, quoteIdentifier(newColumn.Name))
				continue
			}

			oldName, ok := newToOld[newColumn.Name]
			if ok {
				selectColumns = append(selectColumns, quoteIdentifier(oldName))
				continue
			}

			if newColumn.Default.Valid {
				selectColumns = append(selectColumns, newColumn.Default.String)
			} else {
				selectColumns = append(selectColumns, "NULL")
			}
		}

		fmt.Fprintf(
			&diff,
			"INSERT INTO %s (%s) SELECT %s FROM %s;\n",
			quoteIdentifier(tempTable.Name),
			strings.Join(insertColumns, ", "),
			strings.Join(selectColumns, ", "),
			quoteIdentifier(t.Name),
		)

		fmt.Fprintf(&diff, "DROP TABLE %s;\n", quoteIdentifier(t.Name))

		fmt.Fprintf(&diff, "ALTER TABLE %s RENAME TO %s;\n", quoteIdentifier(tempTable.Name), quoteIdentifier(t.Name))

		for _, index := range t.Indexes {
			fmt.Fprintf(&diff, "%s\n", index.String())
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

			fmt.Fprintf(&diff, "ALTER TABLE %s ADD COLUMN %s;\n", quoteIdentifier(t.Name), column.Definition())
		}

	}

	return strings.TrimSpace(diff.String()), nil
}

func (t *SQLiteTable) DiffTriggers(other *SQLiteTable) (string, error) {
	var diff strings.Builder

	for _, sourceTrigger := range t.Triggers {
		targetTrigger, found := other.TriggerByName(sourceTrigger.Name)
		if !found {
			fmt.Fprintf(&diff, "%s;\n", sourceTrigger.SQL)
			continue
		}

		if sourceTrigger.SQL != targetTrigger.SQL {
			fmt.Fprintf(&diff, "DROP TRIGGER %s;\n", quoteIdentifier(targetTrigger.Name))
			fmt.Fprintf(&diff, "%s;\n", sourceTrigger.SQL)
		}
	}

	for _, targetTrigger := range other.Triggers {
		_, found := t.TriggerByName(targetTrigger.Name)
		if !found {
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
			fmt.Fprintf(&diff, "%s\n", sourceIndex.String())
			continue
		}

		if !sourceIndex.Equal(targetIndex) {
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

	return diff.String(), nil
}
