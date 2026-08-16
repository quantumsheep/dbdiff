package drivers

import (
	"fmt"
	"strings"
)

type SQLiteIndex struct {
	Table   string
	Name    string
	Columns []string
	Unique  bool
}

func (i *SQLiteIndex) Equal(other *SQLiteIndex) bool {
	if i.Name != other.Name || i.Table != other.Table || i.Unique != other.Unique {
		return false
	}

	if len(i.Columns) != len(other.Columns) {
		return false
	}

	for idx, col := range i.Columns {
		if col != other.Columns[idx] {
			return false
		}
	}

	return true
}

func (i *SQLiteIndex) String() string {
	createIndex := "CREATE "

	if i.Unique {
		createIndex += "UNIQUE "
	}

	columns := strings.Join(quoteIdentifiers(i.Columns), ", ")

	createIndex += fmt.Sprintf("INDEX %s ON %s (%s);", quoteIdentifier(i.Name), quoteIdentifier(i.Table), columns)

	return createIndex
}
