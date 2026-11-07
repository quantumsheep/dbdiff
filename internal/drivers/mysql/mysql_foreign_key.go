package driversmysql

import (
	"fmt"
	"slices"
	"strings"
)

type MySQLForeignKey struct {
	Name              string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	OnUpdate          string
	OnDelete          string
}

func (k *MySQLForeignKey) Equal(other *MySQLForeignKey) bool {
	return k.Name == other.Name &&
		slices.Equal(k.Columns, other.Columns) &&
		k.ReferencedTable == other.ReferencedTable &&
		slices.Equal(k.ReferencedColumns, other.ReferencedColumns) &&
		sameReferentialAction(k.OnUpdate, other.OnUpdate) &&
		sameReferentialAction(k.OnDelete, other.OnDelete)
}

// InnoDB checks NO ACTION at once, the same as RESTRICT. MySQL stores the default rule as
// NO ACTION and MariaDB stores it as RESTRICT.
func sameReferentialAction(rule string, otherRule string) bool {
	return rule == otherRule || !withReferentialAction(rule) && !withReferentialAction(otherRule)
}

func (k *MySQLForeignKey) UsesColumn(columnName string) bool {
	return slices.Contains(k.Columns, columnName)
}

func (k *MySQLForeignKey) Clause() string {
	clause := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		QuoteIdentifier(k.Name),
		strings.Join(QuoteIdentifiers(k.Columns), ", "),
		QuoteIdentifier(k.ReferencedTable),
		strings.Join(QuoteIdentifiers(k.ReferencedColumns), ", "))

	if withReferentialAction(k.OnDelete) {
		clause += " ON DELETE " + k.OnDelete
	}

	if withReferentialAction(k.OnUpdate) {
		clause += " ON UPDATE " + k.OnUpdate
	}

	return clause
}

func withReferentialAction(rule string) bool {
	return rule != "" && rule != "NO ACTION" && rule != "RESTRICT"
}
