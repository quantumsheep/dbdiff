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
	instruction := &SQLiteCreateIndexInstruction{
		Unique:    i.Unique,
		Name:      i.Name,
		TableName: i.Table,
		Keys:      i.Keys,
	}

	if i.Where != "" {
		instruction.Condition = &SQLiteIndexPredicateCondition{Expression: i.Where}
	}

	return instruction
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
	start := indexOfKeyList(definition)
	if start < 0 {
		return nil, ""
	}

	var keys []string
	var key strings.Builder

	depth := 0
	quote := noQuote

	for position, character := range definition[start:] {
		if quote != noQuote {
			if character == quote {
				quote = noQuote
			}

			key.WriteRune(character)
			continue
		}

		opened := closingQuote(character)
		if opened != noQuote {
			quote = opened

			key.WriteRune(character)
			continue
		}

		if character == '(' {
			depth++

			if depth > 1 {
				key.WriteRune(character)
			}

			continue
		}

		if character == ')' {
			depth--

			if depth > 0 {
				key.WriteRune(character)
				continue
			}

			keys = append(keys, strings.TrimSpace(key.String()))

			return keys, parseIndexCondition(definition[start+position+1:])
		}

		if character == ',' && depth == 1 {
			keys = append(keys, strings.TrimSpace(key.String()))
			key.Reset()

			continue
		}

		key.WriteRune(character)
	}

	return nil, ""
}

func indexOfKeyList(definition string) int {
	quote := noQuote

	for position, character := range definition {
		if quote != noQuote {
			if character == quote {
				quote = noQuote
			}

			continue
		}

		opened := closingQuote(character)
		if opened != noQuote {
			quote = opened
			continue
		}

		if character == '(' {
			return position
		}
	}

	return -1
}

func parseIndexCondition(text string) string {
	const whereKeyword = "WHERE"

	condition := strings.TrimSpace(text)
	if len(condition) <= len(whereKeyword) || !strings.EqualFold(condition[:len(whereKeyword)], whereKeyword) {
		return ""
	}

	return strings.TrimSpace(condition[len(whereKeyword):])
}
