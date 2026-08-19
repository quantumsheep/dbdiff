package drivers

import (
	"strings"
)

// indexKeyModifiers returns the collation and the direction of one key of an index, with a
// leading space. PRAGMA index_info reports neither, so the text of the CREATE INDEX
// statement gives them.
//
// The keyword ASC is the default of SQLite. This function drops it, so an index that names
// it equals an index that does not.
func indexKeyModifiers(definitionKey string) string {
	tokens := splitTopLevelTokens(definitionKey)
	if len(tokens) < 2 {
		return ""
	}

	var modifiers []string

	for _, token := range tokens[1:] {
		if strings.EqualFold(token, "ASC") {
			continue
		}

		modifiers = append(modifiers, token)
	}

	if len(modifiers) == 0 {
		return ""
	}

	return " " + strings.Join(modifiers, " ")
}

// parseTableOptions reads the options that follow the column list of a CREATE TABLE
// statement. SQLite accepts WITHOUT ROWID and STRICT, in any order and in any case.
func parseTableOptions(definition string) (withoutRowID bool, strict bool) {
	tail := definition[indexAfterColumnList(definition):]

	for _, option := range strings.Split(tail, ",") {
		option = strings.Join(strings.Fields(option), " ")

		if strings.EqualFold(option, "WITHOUT ROWID") {
			withoutRowID = true
		}

		if strings.EqualFold(option, "STRICT") {
			strict = true
		}
	}

	return withoutRowID, strict
}

// indexAfterColumnList returns the position that follows the parenthesis which closes the
// column list. A definition with no column list gives the length of the text.
func indexAfterColumnList(definition string) int {
	start := indexOfKeyList(definition)
	if start < 0 {
		return len(definition)
	}

	depth := 0
	quote := noQuote

	for position, character := range definition[start:] {
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
			depth++
			continue
		}

		if character == ')' {
			depth--

			if depth == 0 {
				return start + position + 1
			}
		}
	}

	return len(definition)
}

// SQLiteTableDefinition holds the parts of a CREATE TABLE statement that no PRAGMA gives.
// PRAGMA table_xinfo names a generated column, and it gives no expression for it. No PRAGMA
// reports a collation, the keyword AUTOINCREMENT, or a check.
type SQLiteTableDefinition struct {
	Columns          map[string]*SQLiteColumn
	CheckConstraints []string
	WithoutRowID     bool
	Strict           bool
}

// ColumnByName returns the parsed attributes of one column.
func (d *SQLiteTableDefinition) ColumnByName(name string) (*SQLiteColumn, bool) {
	column, found := d.Columns[name]
	return column, found
}

// parseTableDefinition reads the CREATE TABLE statement of sqlite_master.
func parseTableDefinition(definition string) *SQLiteTableDefinition {
	parsed := &SQLiteTableDefinition{
		Columns: make(map[string]*SQLiteColumn),
	}

	parsed.WithoutRowID, parsed.Strict = parseTableOptions(definition)

	for _, part := range splitColumnDefinitions(definition) {
		tokens := splitTopLevelTokens(part)
		if len(tokens) == 0 {
			continue
		}

		// A table constraint starts with its keyword, so it names no column. The keyword
		// CONSTRAINT and a name can come before that keyword.
		if isTableConstraintKeyword(tokens[0]) {
			constraint := tokens

			if strings.EqualFold(constraint[0], "CONSTRAINT") && len(constraint) > 2 {
				constraint = constraint[2:]
			}

			if strings.EqualFold(constraint[0], "CHECK") && len(constraint) > 1 {
				parsed.CheckConstraints = append(parsed.CheckConstraints, constraint[1])
			}

			continue
		}

		name := unquoteIdentifier(tokens[0])
		if name == "" {
			continue
		}

		parsed.Columns[name] = parseColumnAttributes(name, tokens[1:])
	}

	return parsed
}

func isTableConstraintKeyword(token string) bool {
	switch strings.ToUpper(token) {
	case "CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN":
		return true
	}

	return false
}

// parseColumnAttributes reads the constraints that follow the name of a column. SQLite
// accepts them in any order, so the loop tests each token.
func parseColumnAttributes(name string, tokens []string) *SQLiteColumn {
	column := &SQLiteColumn{Name: name}

	for position, token := range tokens {
		next := ""
		if position+1 < len(tokens) {
			next = tokens[position+1]
		}

		switch {
		case strings.EqualFold(token, "AUTOINCREMENT"):
			column.AutoIncrement = true

		case strings.EqualFold(token, "COLLATE") && next != "":
			column.Collation = unquoteIdentifier(next)

		case strings.EqualFold(token, "CHECK") && strings.HasPrefix(next, "("):
			column.Check = next

		case strings.EqualFold(token, "AS") && strings.HasPrefix(next, "("):
			column.GeneratedExpression = next

			if position+2 < len(tokens) {
				column.GeneratedStored = strings.EqualFold(tokens[position+2], "STORED")
			}
		}
	}

	return column
}

// splitColumnDefinitions returns the parts of the list that follows the table name. A table
// constraint gives a part too, and parseGeneratedColumn drops it.
func splitColumnDefinitions(definition string) []string {
	start := indexOfKeyList(definition)
	if start < 0 {
		return nil
	}

	var parts []string
	var part strings.Builder

	depth := 0
	quote := noQuote

	for _, character := range definition[start:] {
		if quote != noQuote {
			if character == quote {
				quote = noQuote
			}

			part.WriteRune(character)
			continue
		}

		opened := closingQuote(character)
		if opened != noQuote {
			quote = opened

			part.WriteRune(character)
			continue
		}

		if character == '(' {
			depth++

			if depth > 1 {
				part.WriteRune(character)
			}

			continue
		}

		if character == ')' {
			depth--

			if depth > 0 {
				part.WriteRune(character)
				continue
			}

			return append(parts, part.String())
		}

		if character == ',' && depth == 1 {
			parts = append(parts, part.String())
			part.Reset()

			continue
		}

		part.WriteRune(character)
	}

	return parts
}

// splitTopLevelTokens cuts a column definition at each run of spaces outside a quote. A
// group in parentheses stays one token, so an expression keeps its text.
func splitTopLevelTokens(part string) []string {
	var tokens []string
	var token strings.Builder

	depth := 0
	quote := noQuote

	flush := func() {
		if token.Len() > 0 {
			tokens = append(tokens, token.String())
			token.Reset()
		}
	}

	for _, character := range part {
		if quote != noQuote {
			token.WriteRune(character)

			if character == quote {
				quote = noQuote
			}

			continue
		}

		opened := closingQuote(character)
		if opened != noQuote {
			quote = opened

			token.WriteRune(character)
			continue
		}

		if character == '(' {
			depth++

			token.WriteRune(character)
			continue
		}

		if character == ')' {
			depth--

			token.WriteRune(character)

			if depth == 0 {
				flush()
			}

			continue
		}

		if depth == 0 && (character == ' ' || character == '\t' || character == '\n' || character == '\r') {
			flush()
			continue
		}

		token.WriteRune(character)
	}

	flush()

	return tokens
}

// unquoteIdentifier removes the quotes that SQLite accepts around a name.
func unquoteIdentifier(token string) string {
	if len(token) < 2 {
		return token
	}

	first := rune(token[0])

	closing := closingQuote(first)
	if closing == noQuote || rune(token[len(token)-1]) != closing {
		return token
	}

	name := token[1 : len(token)-1]

	if first == '"' || first == '\'' || first == '`' {
		name = strings.ReplaceAll(name, string(first)+string(first), string(first))
	}

	return name
}
