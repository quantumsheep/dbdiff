package driversshared

import "strings"

func AnnotateInstructions(instructions []Instruction) []Instruction {
	var annotated []Instruction

	previous := ""

	for _, instruction := range instructions {
		comment := instruction.Comment()

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

func ObjectComment(verb string, kind string, name string) string {
	if name == "" {
		return verb + " the " + kind
	}

	return verb + " the " + kind + " " + QuoteIdentifier(name)
}

func TableObjectComment(verb string, kind string, name string, tableName string) string {
	comment := ObjectComment(verb, kind, name)

	if tableName == "" {
		return comment
	}

	return comment + " of the table " + QuoteIdentifier(tableName)
}

func OwnedObjectComment(verb string, kind string, ownerKind string, ownerName string) string {
	return verb + " the " + kind + " of the " + strings.ToLower(ownerKind) + " " +
		QuoteIdentifier(ownerName)
}

func DefinitionComment(verb string, kind string, definition string, keyword string) string {
	name, _ := parseDefinition(definition, keyword, "")

	return ObjectComment(verb, kind, name)
}

func TableDefinitionComment(verb string, kind string, definition string, keyword string, ownerKeyword string) string {
	name, tableName := parseDefinition(definition, keyword, ownerKeyword)

	return TableObjectComment(verb, kind, name, tableName)
}

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
