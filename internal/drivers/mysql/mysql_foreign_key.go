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
		k.OnUpdate == other.OnUpdate &&
		k.OnDelete == other.OnDelete
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
