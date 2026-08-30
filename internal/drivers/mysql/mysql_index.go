package driversmysql

import (
	"slices"
)

type MySQLIndex struct {
	Table string
	Name  string

	Kind string

	// A key holds the SQL text of one key part, so a prefix length, a DESC keyword, and
	// an expression key keep their text.
	Keys []string
}

func (i *MySQLIndex) Equal(other *MySQLIndex) bool {
	if i.Name != other.Name || i.Table != other.Table || i.Kind != other.Kind {
		return false
	}

	return slices.Equal(i.Keys, other.Keys)
}

func (i *MySQLIndex) CreateInstruction() *MySQLCreateIndexInstruction {
	return &MySQLCreateIndexInstruction{
		Kind:      i.Kind,
		Name:      i.Name,
		TableName: i.Table,
		Keys:      i.Keys,
	}
}

func (i *MySQLIndex) DropInstruction() *MySQLDropIndexInstruction {
	return &MySQLDropIndexInstruction{
		Name:      i.Name,
		TableName: i.Table,
	}
}
