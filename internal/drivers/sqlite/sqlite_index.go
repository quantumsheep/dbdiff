package driverssqlite

import (
	"slices"
	"strings"
)

type SQLiteIndex struct {
	Table string
	Name  string

	// A key holds the SQL text of one key part, so an expression key keeps its text.
	Keys  []string
	Where string

	Unique bool
}

func (i *SQLiteIndex) Equal(other *SQLiteIndex) bool {
	if i.Name != other.Name || i.Table != other.Table || i.Unique != other.Unique {
		return false
	}

	if i.Where != other.Where {
		return false
	}

	return slices.Equal(i.Keys, other.Keys)
}

func (i *SQLiteIndex) CreateInstruction() *SQLiteCreateIndexInstruction {
	return &SQLiteCreateIndexInstruction{
		Unique:    i.Unique,
		Name:      i.Name,
		TableName: i.Table,
		Keys:      i.Keys,
		Where:     i.Where,
	}
}

const noQuote = rune(0)

func closingQuote(character rune) rune {
	switch character {
	case '\'', '"', '`':
		return character
	case '[':
		return ']'
	}

	return noQuote
}

// PRAGMA index_info gives no name for an expression key, and it gives no condition.
func parseIndexDefinition(definition string) ([]string, string) {
	definition = withoutComments(definition)

	opening, closing := keyListBounds(definition)
	if opening < 0 || closing < 0 {
		return nil, ""
	}

	keys := splitTopLevel(definition[opening+1 : closing])

	for position, key := range keys {
		keys[position] = strings.TrimSpace(key)
	}

	return keys, parseIndexCondition(definition[closing+1:])
}

func parseIndexCondition(text string) string {
	const whereKeyword = "WHERE"

	condition := strings.TrimSpace(text)
	if len(condition) <= len(whereKeyword) || !strings.EqualFold(condition[:len(whereKeyword)], whereKeyword) {
		return ""
	}

	return strings.TrimSpace(condition[len(whereKeyword):])
}
