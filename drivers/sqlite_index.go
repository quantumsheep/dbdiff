package drivers

import (
	"fmt"
	"slices"
	"strings"
)

type SQLiteIndex struct {
	Table string
	Name  string

	// Keys holds the SQL text of each key part of the index. A key that a column builds
	// holds the quoted name of the column. A key that an expression builds holds the text
	// of the expression.
	Keys []string

	// Where holds the condition of a partial index. It is empty for a full index.
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

func (i *SQLiteIndex) String() string {
	var statement strings.Builder

	statement.WriteString("CREATE ")

	if i.Unique {
		statement.WriteString("UNIQUE ")
	}

	fmt.Fprintf(
		&statement,
		"INDEX %s ON %s (%s)",
		quoteIdentifier(i.Name),
		quoteIdentifier(i.Table),
		strings.Join(i.Keys, ", "),
	)

	if i.Where != "" {
		fmt.Fprintf(&statement, " WHERE %s", i.Where)
	}

	statement.WriteString(";")

	return statement.String()
}

const noQuote = rune(0)

// closingQuote returns the character that closes a quoted part of a statement. It returns
// noQuote for a character that opens no quoted part.
func closingQuote(character rune) rune {
	switch character {
	case '\'', '"', '`':
		return character
	case '[':
		return ']'
	}

	return noQuote
}

// parseIndexDefinition reads the key parts and the condition of a CREATE INDEX statement.
// PRAGMA index_info gives no name for a key that an expression builds, and it gives no
// condition. The text of the definition gives both.
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

// indexOfKeyList returns the position of the parenthesis that opens the key list. It
// returns -1 when the statement holds no key list.
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

// parseIndexCondition returns the condition of a partial index. The text starts after the
// parenthesis that closes the key list.
func parseIndexCondition(text string) string {
	const whereKeyword = "WHERE"

	condition := strings.TrimSpace(text)

	if len(condition) <= len(whereKeyword) || !strings.EqualFold(condition[:len(whereKeyword)], whereKeyword) {
		return ""
	}

	return strings.TrimSpace(condition[len(whereKeyword):])
}
