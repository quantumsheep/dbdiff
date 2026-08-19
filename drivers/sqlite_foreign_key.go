package drivers

import (
	"fmt"
	"strings"
)

type SQLiteForeignKey struct {
	// Name holds the name of the constraint. It is empty for a key that the schema declares
	// with no name.
	Name string

	Table    string
	From     []string
	To       []string
	OnUpdate string
	OnDelete string

	// Deferrable holds the text of the DEFERRABLE clause, for example
	// "DEFERRABLE INITIALLY DEFERRED". It is empty for a key with no such clause.
	Deferrable string
}

func (fk *SQLiteForeignKey) Clause() string {
	fromColumns := strings.Join(quoteIdentifiers(fk.From), ", ")
	toColumns := strings.Join(quoteIdentifiers(fk.To), ", ")

	statement := fmt.Sprintf("%sFOREIGN KEY (%s) REFERENCES %s (%s)",
		constraintNameClause(fk.Name), fromColumns, quoteIdentifier(fk.Table), toColumns)

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
