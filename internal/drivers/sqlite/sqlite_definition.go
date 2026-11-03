package driverssqlite

import (
	"slices"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

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

	for _, part := range splitTopLevel(inner) {
		tokens := splitTopLevelTokens(part)
		if len(tokens) == 0 {
			continue
		}

		names = append(names, unquoteIdentifier(tokens[0]))
	}

	return names
}

// No PRAGMA reports the COLLATE clause and the DESC keyword of a key of a table
// constraint, so the text of the CREATE TABLE statement gives them. A list of plain
// names gives nil, so a plain constraint keeps its plain form.
func constraintKeyClauses(list string) []string {
	inner := strings.TrimSuffix(strings.TrimPrefix(list, "("), ")")

	var clauses []string

	holdsModifier := false

	for _, part := range splitTopLevel(inner) {
		tokens := splitTopLevelTokens(part)
		if len(tokens) == 0 {
			continue
		}

		modifiers := indexKeyModifiers(part)
		if modifiers != "" {
			holdsModifier = true
		}

		clauses = append(clauses, driversshared.QuoteIdentifier(unquoteIdentifier(tokens[0]))+modifiers)
	}

	if !holdsModifier {
		return nil
	}

	return clauses
}

// PRAGMA index_info reports no collation and no direction, so the text of the CREATE INDEX
// statement gives them. ASC is the default of SQLite, and this function drops it, so an
// index that names it equals an index that does not.
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

// SQLite accepts WITHOUT ROWID and STRICT, in any order and in any case.
func parseTableOptions(definition string) (withoutRowID bool, strict bool) {
	_, closing := keyListBounds(definition)
	if closing < 0 {
		return false, false
	}

	for _, option := range splitTopLevel(definition[closing+1:]) {
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

// A parsed constraint holds the parts of a UNIQUE or a FOREIGN KEY clause that no PRAGMA
// reports. Two constraints can have the same columns, so no map keys them by column.
type parsedConstraint struct {
	Name       string
	Columns    []string
	Keys       []string
	Conflict   string
	Deferrable string
}

// PRAGMA table_xinfo gives no expression of a generated column, and no PRAGMA reports a
// collation, the keyword AUTOINCREMENT, or a check.
type SQLiteTableDefinition struct {
	Columns           map[string]*SQLiteColumn
	CheckConstraints  []*SQLiteCheckConstraint
	UniqueConstraints []*parsedConstraint

	// The foreign keys of the columns and of the table, in the order of the CREATE TABLE
	// text. GetTableForeignKeys matches the PRAGMA entries to them by this order.
	ForeignKeys []*parsedConstraint

	PrimaryKeyName     string
	PrimaryKeyConflict string
	PrimaryKeyKeys     []string
	WithoutRowID       bool
	Strict             bool
}

// If no constraint has these columns, this method returns an empty constraint.
func (d *SQLiteTableDefinition) UniqueConstraintOf(columns []string) *parsedConstraint {
	for _, constraint := range d.UniqueConstraints {
		if slices.Equal(constraint.Columns, columns) {
			return constraint
		}
	}

	return &parsedConstraint{}
}

func (d *SQLiteTableDefinition) ColumnByName(name string) (*SQLiteColumn, bool) {
	column, found := d.Columns[name]
	return column, found
}

func parseTableDefinition(definition string) *SQLiteTableDefinition {
	parsed := &SQLiteTableDefinition{
		Columns: make(map[string]*SQLiteColumn),
	}

	definition = withoutComments(definition)

	parsed.WithoutRowID, parsed.Strict = parseTableOptions(definition)

	for _, part := range splitColumnDefinitions(definition) {
		tokens := splitTopLevelTokens(part)
		if len(tokens) == 0 {
			continue
		}

		// A table constraint starts with its keyword, so it names no column.
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
				parsed.UniqueConstraints = append(parsed.UniqueConstraints, &parsedConstraint{
					Name:     constraintName,
					Columns:  constraintColumnNames(constraint[1]),
					Keys:     constraintKeyClauses(constraint[1]),
					Conflict: conflictResolution(constraint[1:]),
				})
			}

			if strings.EqualFold(constraint[0], "PRIMARY") && len(constraint) > 2 {
				parsed.PrimaryKeyName = constraintName
				parsed.PrimaryKeyConflict = conflictResolution(constraint[2:])
				parsed.PrimaryKeyKeys = constraintKeyClauses(constraint[2])
			}

			if strings.EqualFold(constraint[0], "FOREIGN") && len(constraint) > 2 {
				parsed.ForeignKeys = append(parsed.ForeignKeys, &parsedConstraint{
					Name:       constraintName,
					Columns:    constraintColumnNames(constraint[2]),
					Deferrable: deferrableClause(constraint[2:]),
				})
			}

			continue
		}

		name := unquoteIdentifier(tokens[0])
		if name == "" {
			continue
		}

		column, foreignKeys := parseColumnAttributes(name, tokens[1:])

		parsed.Columns[name] = column
		parsed.ForeignKeys = append(parsed.ForeignKeys, foreignKeys...)
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

// SQLite accepts the constraints of a column in any order, so the loop tests each token.
func parseColumnAttributes(name string, tokens []string) (*SQLiteColumn, []*parsedConstraint) {
	column := &SQLiteColumn{Name: name}

	var foreignKeys []*parsedConstraint

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
			foreignKey := &parsedConstraint{
				Columns:    []string{name},
				Deferrable: deferrableClause(tokens[position:]),
			}

			if position >= 2 && strings.EqualFold(tokens[position-2], "CONSTRAINT") {
				foreignKey.Name = unquoteIdentifier(tokens[position-1])
			}

			foreignKeys = append(foreignKeys, foreignKey)

		case strings.EqualFold(token, "AS") && strings.HasPrefix(next, "("):
			column.GeneratedExpression = next

			if position+2 < len(tokens) {
				column.GeneratedStored = strings.EqualFold(tokens[position+2], "STORED")
			}
		}
	}

	return column, foreignKeys
}

// A table constraint gives a part too, and parseTableDefinition drops it.
func splitColumnDefinitions(definition string) []string {
	opening, closing := keyListBounds(definition)
	if opening < 0 || closing < 0 {
		return nil
	}

	return splitTopLevel(definition[opening+1 : closing])
}

// A group in parentheses stays one token, so an expression keeps its text.
func splitTopLevelTokens(part string) []string {
	var tokens []string

	tokenStart := -1

	endToken := func(position int) {
		if tokenStart >= 0 {
			tokens = append(tokens, part[tokenStart:position])
			tokenStart = -1
		}
	}

	scanTopLevel(part, func(position int, depth int) {
		character := part[position]

		switch {
		case depth != 0:
			return
		case character == '(':
			// Without this split, CHECK(x) is one token and the constraint disappears.
			endToken(position)
			tokenStart = position
		case character == ')':
			endToken(position + 1)
		case character == ' ' || character == '\t' || character == '\n' || character == '\r':
			endToken(position)
		case tokenStart < 0:
			tokenStart = position
		}
	})

	endToken(len(part))

	return tokens
}

func splitTopLevel(text string) []string {
	var parts []string

	partStart := 0

	scanTopLevel(text, func(position int, depth int) {
		if depth == 0 && text[position] == ',' {
			parts = append(parts, text[partStart:position])
			partStart = position + 1
		}
	})

	return append(parts, text[partStart:])
}

// The first group in parentheses holds the columns of a table or the keys of an index.
func keyListBounds(definition string) (opening int, closing int) {
	opening = -1
	closing = -1

	scanTopLevel(definition, func(position int, depth int) {
		if depth != 0 || closing >= 0 {
			return
		}

		switch definition[position] {
		case '(':
			opening = position
		case ')':
			closing = position
		}
	})

	return opening, closing
}

// The visit function receives the position of each character outside quoted text, with the
// parenthesis depth around that character. The two parentheses of a group get the same depth.
// The function receives a quoted text one time, at its opening quote, so a test of that
// character never matches a parenthesis or a separator in the text.
func scanTopLevel(text string, visit func(position int, depth int)) {
	depth := 0
	index := 0

	for index < len(text) {
		end := max(endOfQuotedTextOrComment(text, index), index+1)

		if text[index] == ')' {
			depth--
		}

		visit(index, depth)

		if text[index] == '(' {
			depth++
		}

		index = end
	}
}

// SQLite keeps the comments of a CREATE statement in sqlite_master. Each comment becomes a
// space, so no parser reads a comment as a part of a column or a key.
func withoutComments(text string) string {
	var result strings.Builder

	index := 0

	for index < len(text) {
		end := endOfQuotedTextOrComment(text, index)

		switch {
		case end == index:
			result.WriteByte(text[index])

			end++
		case text[index] == '-' || text[index] == '/':
			result.WriteByte(' ')
		default:
			result.WriteString(text[index:end])
		}

		index = end
	}

	return result.String()
}

// If no quoted text and no comment starts at index, this function returns index.
// SQLite nests no block comment, so the first "*/" ends it. SQLite has no E'...' string, so
// the quoted text starts the slice that EndOfQuotedText reads. Then no letter before the
// quote turns on the backslash escape.
func endOfQuotedTextOrComment(text string, index int) int {
	rest := text[index:]

	switch {
	case strings.HasPrefix(rest, "--"):
		return driversshared.EndOfLineComment(text, index)
	case strings.HasPrefix(rest, "/*"):
		return endOfTerminator(text, index+len("/*"), "*/")
	case rest[0] == '[':
		return endOfTerminator(text, index+1, "]")
	case closingQuote(rune(rest[0])) != noQuote:
		return index + driversshared.EndOfQuotedText(rest, 0)
	}

	return index
}

func endOfTerminator(text string, index int, terminator string) int {
	end := strings.Index(text[index:], terminator)
	if end < 0 {
		return len(text)
	}

	return index + end + len(terminator)
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
