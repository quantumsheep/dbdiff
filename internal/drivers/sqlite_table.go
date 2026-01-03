package drivers

import (
	"fmt"
	"strings"
)

type SQLiteTable struct {
	Name        string
	Columns     []*SQLiteColumn
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

	for _, fk := range t.ForeignKeys {
		line := "\t" + fk.String()
		columnLines = append(columnLines, line)
	}

	createTableColumns := strings.Join(columnLines, ",\n")
	return fmt.Sprintf("CREATE TABLE \"%s\" (\n%s\n);", t.Name, createTableColumns)
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

	if indexes := t.StringCreateIndexes(); indexes != "" {
		str += "\n" + indexes
	}

	if triggers := t.StringCreateTriggers(); triggers != "" {
		str += "\n" + triggers
	}

	return str
}
