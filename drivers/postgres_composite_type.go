package drivers

import (
	"slices"
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

func (t *PostgresCompositeType) CreateInstruction() *PostgresCreateCompositeTypeInstruction {
	return &PostgresCreateCompositeTypeInstruction{
		Name:       t.Name,
		Attributes: t.Attributes,
	}
}

func (t *PostgresCompositeType) DropInstruction() *PostgresDropTypeInstruction {
	return &PostgresDropTypeInstruction{Name: t.Name}
}

// One ALTER TYPE statement changes one attribute only, and the order of the attributes
// stays fixed. A recreation gives the wanted attribute list in every case.
func (t *PostgresCompositeType) Diff(other *PostgresCompositeType) []Instruction {
	if t.Equal(other) {
		return nil
	}

	return []Instruction{other.DropInstruction(), t.CreateInstruction()}
}
