package drivers

import (
	"fmt"
	"strings"
)

type SQLiteForeignKey struct {
	Table    string
	From     []string
	To       []string
	OnUpdate string
	OnDelete string
}

func (fk *SQLiteForeignKey) Clause() string {
	fromColumns := strings.Join(quoteIdentifiers(fk.From), ", ")
	toColumns := strings.Join(quoteIdentifiers(fk.To), ", ")

	statement := fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s (%s)", fromColumns, quoteIdentifier(fk.Table), toColumns)

	if fk.OnUpdate != "NO ACTION" && fk.OnUpdate != "" {
		statement += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
	}

	if fk.OnDelete != "NO ACTION" && fk.OnDelete != "" {
		statement += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
	}

	return statement
}

func (fk *SQLiteForeignKey) Equal(other *SQLiteForeignKey) bool {
	if fk.Table != other.Table || fk.OnUpdate != other.OnUpdate || fk.OnDelete != other.OnDelete {
		return false
	}

	if len(fk.From) != len(other.From) || len(fk.To) != len(other.To) {
		return false
	}

	for i := range fk.From {
		if fk.From[i] != other.From[i] {
			return false
		}
	}

	for i := range fk.To {
		if fk.To[i] != other.To[i] {
			return false
		}
	}

	return true
}
