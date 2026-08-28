package drivers

import (
	"fmt"
	"strings"
)

type SQLiteForeignKey struct {
	Name string

	Table    string
	From     []string
	To       []string
	OnUpdate string
	OnDelete string

	Deferrable string
}

func (fk *SQLiteForeignKey) Clause() string {
	fromColumns := strings.Join(QuoteIdentifiers(fk.From), ", ")
	toColumns := strings.Join(QuoteIdentifiers(fk.To), ", ")

	statement := fmt.Sprintf("%sFOREIGN KEY (%s) REFERENCES %s (%s)",
		constraintNameClause(fk.Name), fromColumns, QuoteIdentifier(fk.Table), toColumns)

	if fk.OnUpdate != "NO ACTION" && fk.OnUpdate != "" {
		statement += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
	}

	if fk.OnDelete != "NO ACTION" && fk.OnDelete != "" {
		statement += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
	}

	if fk.Deferrable != "" {
		statement += " " + fk.Deferrable
	}

	return statement
}

func (fk *SQLiteForeignKey) Equal(other *SQLiteForeignKey) bool {
	if fk.Name != other.Name || fk.Table != other.Table {
		return false
	}

	if fk.OnUpdate != other.OnUpdate || fk.OnDelete != other.OnDelete {
		return false
	}

	if fk.Deferrable != other.Deferrable {
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
