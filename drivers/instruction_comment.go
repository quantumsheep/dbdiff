package drivers

import "strings"

func AnnotateInstructions(instructions []Instruction) []Instruction {
	comments := instructionComments(instructions)

	var annotated []Instruction

	previous := ""

	for index, instruction := range instructions {
		comment := comments[index]

		if comment != "" && comment != previous {
			annotated = append(annotated, &SQLCommentInstruction{
				Text: comment,
			})
		}

		annotated = append(annotated, instruction)
		previous = comment
	}

	return annotated
}

func instructionComments(instructions []Instruction) []string {
	comments := make([]string, len(instructions))

	for index, instruction := range instructions {
		comments[index] = instruction.Comment()
	}

	markTableRecreations(instructions, comments)

	return comments
}

func markTableRecreations(instructions []Instruction, comments []string) {
	for index := range instructions {
		tableName, length := tableRecreationAt(instructions, index)
		if length == 0 {
			continue
		}

		comment := objectComment("Recreate", "table", tableName)

		for position := range comments[index : index+length] {
			comments[index+position] = comment
		}
	}
}

// The recreation of a table of SQLite prints six parts: a temporary table, the copy of the
// rows, the removal of the old table, the rename of the temporary table, the indexes, and
// the triggers. The rename names the table, and the search of that rename stops at the
// next CREATE TABLE statement, which starts another table.
func tableRecreationAt(instructions []Instruction, index int) (string, int) {
	createTable, isCreateTable := instructions[index].(*SQLiteCreateTableInstruction)
	if !isCreateTable {
		return "", 0
	}

	tableName := ""
	renameIndex := -1

	for position, instruction := range instructions[index+1:] {
		_, startsAnotherTable := instruction.(*SQLiteCreateTableInstruction)
		if startsAnotherTable {
			break
		}

		newName, renames := renamedTableName(instruction, createTable.Name)
		if !renames {
			continue
		}

		if createTable.Name == "_"+newName+"_temp" {
			tableName = newName
			renameIndex = index + 1 + position
		}

		break
	}

	if renameIndex < 0 {
		return "", 0
	}

	end := renameIndex

	for position, instruction := range instructions[renameIndex+1:] {
		if !belongsToTable(instruction, tableName) {
			break
		}

		end = renameIndex + 1 + position
	}

	return tableName, end - index + 1
}

func renamedTableName(instruction Instruction, name string) (string, bool) {
	alterTable, isAlterTable := instruction.(*SQLiteAlterTableInstruction)
	if !isAlterTable || alterTable.Name != name {
		return "", false
	}

	rename, isRename := alterTable.Action.(*SQLRenameTableAction)
	if !isRename {
		return "", false
	}

	return rename.NewName, true
}

func belongsToTable(instruction Instruction, tableName string) bool {
	index, isIndex := instruction.(*SQLiteCreateIndexInstruction)
	if isIndex {
		return index.TableName == tableName
	}

	trigger, isTrigger := instruction.(*SQLiteCreateTriggerInstruction)
	if isTrigger {
		_, triggerTableName := parseDefinition(trigger.Definition, "TRIGGER", "ON")

		return triggerTableName == tableName
	}

	return false
}

func objectComment(verb string, kind string, name string) string {
	if name == "" {
		return verb + " the " + kind
	}

	return verb + " the " + kind + " " + quoteIdentifier(name)
}

func tableObjectComment(verb string, kind string, name string, tableName string) string {
	comment := objectComment(verb, kind, name)

	if tableName == "" {
		return comment
	}

	return comment + " of the table " + quoteIdentifier(tableName)
}

func ownedObjectComment(verb string, kind string, ownerKind string, ownerName string) string {
	return verb + " the " + kind + " of the " + strings.ToLower(ownerKind) + " " +
		quoteIdentifier(ownerName)
}

func definitionComment(verb string, kind string, definition string, keyword string) string {
	name, _ := parseDefinition(definition, keyword, "")

	return objectComment(verb, kind, name)
}

func tableDefinitionComment(verb string, kind string, definition string, keyword string, ownerKeyword string) string {
	name, tableName := parseDefinition(definition, keyword, ownerKeyword)

	return tableObjectComment(verb, kind, name, tableName)
}

// PostgreSQL and SQLite give the definition text of an index, of a trigger, of a view, of
// a rule, of a function, and of a statistics object. The name of the object follows the
// keyword of its kind, and the name of the table follows the keyword of the owner.
func parseDefinition(definition string, keyword string, ownerKeyword string) (string, string) {
	tokens := definitionTokens(definition)

	position := keywordPosition(tokens, keyword, 0)
	if position < 0 {
		return "", ""
	}

	position++

	for position < len(tokens) && isExistenceKeyword(tokens[position]) {
		position++
	}

	if position >= len(tokens) {
		return "", ""
	}

	name := tokens[position]

	if ownerKeyword == "" {
		return name, ""
	}

	ownerPosition := keywordPosition(tokens, ownerKeyword, position+1)
	if ownerPosition < 0 || ownerPosition+1 >= len(tokens) {
		return name, ""
	}

	return name, tokens[ownerPosition+1]
}

func keywordPosition(tokens []string, keyword string, start int) int {
	for position, token := range tokens[start:] {
		if strings.EqualFold(token, keyword) {
			return start + position
		}
	}

	return -1
}

func isExistenceKeyword(token string) bool {
	return strings.EqualFold(token, "IF") ||
		strings.EqualFold(token, "NOT") ||
		strings.EqualFold(token, "EXISTS")
}

const definitionSeparators = " \t\n\r(),;"

func definitionTokens(definition string) []string {
	var tokens []string
	var token strings.Builder

	characters := []rune(definition)
	inQuotes := false
	skip := false

	for index, character := range characters {
		if skip {
			skip = false
			continue
		}

		if character == '"' {
			// A quoted identifier holds a doubled quote for one quote of its name.
			if inQuotes && index+1 < len(characters) && characters[index+1] == '"' {
				token.WriteRune('"')
				skip = true
				continue
			}

			inQuotes = !inQuotes
			continue
		}

		if !inQuotes && strings.ContainsRune(definitionSeparators, character) {
			if token.Len() > 0 {
				tokens = append(tokens, token.String())
				token.Reset()
			}

			continue
		}

		token.WriteRune(character)
	}

	if token.Len() > 0 {
		tokens = append(tokens, token.String())
	}

	return tokens
}
