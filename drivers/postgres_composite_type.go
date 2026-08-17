package drivers

import (
	"fmt"
	"slices"
	"strings"
)

type PostgresCompositeTypeAttribute struct {
	Name string
	Type string
}

type PostgresCompositeType struct {
	Name       string
	Attributes []*PostgresCompositeTypeAttribute
}

func (t *PostgresCompositeType) Equal(other *PostgresCompositeType) bool {
	if t.Name != other.Name {
		return false
	}

	return slices.EqualFunc(t.Attributes, other.Attributes, func(first, second *PostgresCompositeTypeAttribute) bool {
		return *first == *second
	})
}

func (t *PostgresCompositeType) String() string {
	attributeLines := make([]string, len(t.Attributes))

	for i, attribute := range t.Attributes {
		attributeLines[i] = fmt.Sprintf("\t%s %s", quoteIdentifier(attribute.Name), attribute.Type)
	}

	return fmt.Sprintf("CREATE TYPE %s AS (\n%s\n);", quoteIdentifier(t.Name), strings.Join(attributeLines, ",\n"))
}

func (t *PostgresCompositeType) StringDrop() string {
	return fmt.Sprintf("DROP TYPE %s;", quoteIdentifier(t.Name))
}

// One ALTER TYPE statement changes one attribute only, and the order of the attributes
// stays fixed. A recreation gives the wanted attribute list in every case.
func (t *PostgresCompositeType) Diff(other *PostgresCompositeType) string {
	if t.Equal(other) {
		return ""
	}

	var diff strings.Builder

	fmt.Fprintf(&diff, "%s\n", other.StringDrop())
	fmt.Fprintf(&diff, "%s\n", t.String())

	return strings.TrimSpace(diff.String())
}
