package drivers

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
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

	quotedColumns := lo.Map(i.Columns, func(c string, _ int) string {
		return fmt.Sprintf("\"%s\"", c)
	})
	columns := strings.Join(quotedColumns, ", ")

	createIndex += fmt.Sprintf("INDEX \"%s\" ON \"%s\" (%s);", i.Name, i.Table, columns)

	return createIndex
}
