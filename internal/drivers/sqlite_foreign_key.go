package drivers

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
)

type SQLiteForeignKey struct {
	Table    string
	From     []string
	To       []string
	OnUpdate string
	OnDelete string
}

func (fk *SQLiteForeignKey) String() string {
	fromColumnsQuoted := lo.Map(fk.From, func(c string, _ int) string {
		return fmt.Sprintf("\"%s\"", c)
	})
	toColumnsQuoted := lo.Map(fk.To, func(c string, _ int) string {
		return fmt.Sprintf("\"%s\"", c)
	})

	fromColumns := strings.Join(fromColumnsQuoted, ", ")
	toColumns := strings.Join(toColumnsQuoted, ", ")

	s := fmt.Sprintf("FOREIGN KEY (%s) REFERENCES \"%s\" (%s)", fromColumns, fk.Table, toColumns)
	if fk.OnUpdate != "NO ACTION" && fk.OnUpdate != "" {
		s += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
	}
	if fk.OnDelete != "NO ACTION" && fk.OnDelete != "" {
		s += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
	}
	return s
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
