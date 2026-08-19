package drivers

import (
	"strings"
)

// deferrableClause returns the DEFERRABLE clause in these tokens, with no leading space.
// SQLite accepts the keyword NOT before it, and it accepts the keywords INITIALLY DEFERRED
// or INITIALLY IMMEDIATE after it.
func deferrableClause(tokens []string) string {
	for position, token := range tokens {
		if !strings.EqualFold(token, "DEFERRABLE") {
			continue
		}

		clause := []string{"DEFERRABLE"}

		if position > 0 && strings.EqualFold(tokens[position-1], "NOT") {
			clause = []string{"NOT", "DEFERRABLE"}
		}

		if position+2 < len(tokens) && strings.EqualFold(tokens[position+1], "INITIALLY") {
			clause = append(clause, "INITIALLY", strings.ToUpper(tokens[position+2]))
		}

		return strings.Join(clause, " ")
	}

	return ""
}

func conflictResolution(tokens []string) string {
	for position, token := range tokens {
		if !strings.EqualFold(token, "ON") || position+2 >= len(tokens) {
			continue
		}

		if strings.EqualFold(tokens[position+1], "CONFLICT") {
			return strings.ToUpper(tokens[position+2])
		}
	}

	return ""
}

func constraintColumnNames(list string) []string {
	inner := strings.TrimSuffix(strings.TrimPrefix(list, "("), ")")

	var names []string

	for _, part := range strings.Split(inner, ",") {
		tokens := splitTopLevelTokens(part)
		if len(tokens) == 0 {
			continue
		}

		names = append(names, unquoteIdentifier(tokens[0]))
	}

	return names
}

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

// A definition with no column list gives the length of the text.
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
	Columns            map[string]*SQLiteColumn
	CheckConstraints   []*SQLiteCheckConstraint
	UniqueNames        map[string]string
	ForeignKeyNames    map[string]string
	UniqueConflicts    map[string]string
	ForeignKeyDefers   map[string]string
	PrimaryKeyConflict string
	WithoutRowID       bool
	Strict             bool
}

func (d *SQLiteTableDefinition) UniqueConflictOf(columns []string) string {
	return d.UniqueConflicts[strings.Join(columns, ",")]
}

func (d *SQLiteTableDefinition) DeferrableOf(columns []string) string {
	return d.ForeignKeyDefers[strings.Join(columns, ",")]
}

func (d *SQLiteTableDefinition) UniqueNameOf(columns []string) string {
	return d.UniqueNames[strings.Join(columns, ",")]
}

func (d *SQLiteTableDefinition) ForeignKeyNameOf(columns []string) string {
	return d.ForeignKeyNames[strings.Join(columns, ",")]
}

func (d *SQLiteTableDefinition) ColumnByName(name string) (*SQLiteColumn, bool) {
	column, found := d.Columns[name]
	return column, found
}

func parseTableDefinition(definition string) *SQLiteTableDefinition {
	parsed := &SQLiteTableDefinition{
		Columns:          make(map[string]*SQLiteColumn),
		UniqueConflicts:  make(map[string]string),
		ForeignKeyDefers: make(map[string]string),
		UniqueNames:      make(map[string]string),
		ForeignKeyNames:  make(map[string]string),
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
			constraintName := ""

			if strings.EqualFold(constraint[0], "CONSTRAINT") && len(constraint) > 2 {
				constraintName = unquoteIdentifier(constraint[1])
				constraint = constraint[2:]
			}

			if strings.EqualFold(constraint[0], "CHECK") && len(constraint) > 1 {
				parsed.CheckConstraints = append(parsed.CheckConstraints, &SQLiteCheckConstraint{
					Name:       constraintName,
					Expression: constraint[1],
				})
			}

			if strings.EqualFold(constraint[0], "UNIQUE") && len(constraint) > 1 {
				key := strings.Join(constraintColumnNames(constraint[1]), ",")
				parsed.UniqueConflicts[key] = conflictResolution(constraint[1:])
				parsed.UniqueNames[key] = constraintName
			}

			if strings.EqualFold(constraint[0], "PRIMARY") && len(constraint) > 2 {
				parsed.PrimaryKeyConflict = conflictResolution(constraint[2:])
			}

			if strings.EqualFold(constraint[0], "FOREIGN") && len(constraint) > 2 {
				key := strings.Join(constraintColumnNames(constraint[2]), ",")
				parsed.ForeignKeyDefers[key] = deferrableClause(constraint[2:])
				parsed.ForeignKeyNames[key] = constraintName
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

	// An ON CONFLICT clause belongs to the constraint before it, so the loop keeps the
	// name of the constraint that it read last.
	lastConstraint := ""

	for position, token := range tokens {
		next := ""
		if position+1 < len(tokens) {
			next = tokens[position+1]
		}

		if strings.EqualFold(token, "ON") && strings.EqualFold(next, "CONFLICT") &&
			position+2 < len(tokens) {
			switch lastConstraint {
			case "PRIMARY KEY":
				column.PrimaryKeyConflict = strings.ToUpper(tokens[position+2])
			case "UNIQUE":
				column.UniqueConflict = strings.ToUpper(tokens[position+2])
			case "NOT NULL":
				column.NotNullConflict = strings.ToUpper(tokens[position+2])
			}

			continue
		}

		switch {
		case strings.EqualFold(token, "PRIMARY") && strings.EqualFold(next, "KEY"):
			lastConstraint = "PRIMARY KEY"
		case strings.EqualFold(token, "UNIQUE"):
			lastConstraint = "UNIQUE"
		case strings.EqualFold(token, "NOT") && strings.EqualFold(next, "NULL"):
			lastConstraint = "NOT NULL"
		}

		switch {
		case strings.EqualFold(token, "AUTOINCREMENT"):
			column.AutoIncrement = true

		case strings.EqualFold(token, "COLLATE") && next != "":
			column.Collation = unquoteIdentifier(next)

		case strings.EqualFold(token, "CHECK") && strings.HasPrefix(next, "("):
			column.Check = next

		case strings.EqualFold(token, "REFERENCES"):
			column.ForeignKeyDeferrable = deferrableClause(tokens[position:])

		case strings.EqualFold(token, "AS") && strings.HasPrefix(next, "("):
			column.GeneratedExpression = next

			if position+2 < len(tokens) {
				column.GeneratedStored = strings.EqualFold(tokens[position+2], "STORED")
			}
		}
	}

	return column
}

// A table constraint gives a part too, and parseTableDefinition drops it.
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
