package drivers

import "strings"

const (
	NoTransactionDirective      = "-- dbdiff:no-transaction"
	AtlasNoTransactionDirective = "-- atlas:txmode none"
)

func FileUsesTransaction(content string) bool {
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)

		if trimmed == NoTransactionDirective || trimmed == AtlasNoTransactionDirective {
			return false
		}
	}

	return true
}

// PostgreSQL runs the statements of one call in an implicit transaction block, and it
// refuses CREATE INDEX CONCURRENTLY there. A file that holds the directive takes one call
// for each statement for that reason, and every other file runs in one call.
//
// A semicolon splits nothing inside parentheses, because the actions of CREATE RULE sit
// there. It splits nothing inside BEGIN ... END, because the body of a trigger of SQLite
// and the body of BEGIN ATOMIC hold whole statements. A CASE expression ends with END
// too, so the block counter reads CASE as an opener.
func SplitSQLStatements(content string) []string {
	var statements []string

	start := 0
	index := 0

	parenthesisDepth := 0
	blockDepth := 0
	triggerStatement := false

	for index < len(content) {
		rest := content[index:]

		switch {
		case strings.HasPrefix(rest, "--"):
			index = endOfLineComment(content, index)
		case strings.HasPrefix(rest, "/*"):
			index = endOfBlockComment(content, index)
		case content[index] == '\'' || content[index] == '"':
			index = endOfQuotedText(content, index)
		case content[index] == '$':
			index = endOfDollarQuotedText(content, index)
		case content[index] == '(':
			parenthesisDepth++
			index++
		case content[index] == ')':
			parenthesisDepth--
			index++
		case isSQLWordStart(content[index]):
			word, end := readSQLWord(content, index)

			switch {
			case word == "trigger" && blockDepth == 0:
				triggerStatement = true
			case word == "begin" && blockDepth == 0 &&
				(triggerStatement || nextSQLWord(content, end) == "atomic"):
				blockDepth++
			case word == "case" && blockDepth > 0:
				blockDepth++
			case word == "end" && blockDepth > 0:
				blockDepth--
			}

			index = end
		case content[index] == ';' && parenthesisDepth == 0 && blockDepth == 0:
			statements = appendStatement(statements, content[start:index])
			index++
			start = index
			triggerStatement = false
		default:
			index++
		}
	}

	return appendStatement(statements, content[start:])
}

func isSQLWordStart(letter byte) bool {
	return letter == '_' ||
		(letter >= 'a' && letter <= 'z') ||
		(letter >= 'A' && letter <= 'Z')
}

func readSQLWord(content string, index int) (string, int) {
	end := index

	for end < len(content) {
		letter := content[end]

		isWordLetter := isSQLWordStart(letter) || (letter >= '0' && letter <= '9')
		if !isWordLetter {
			break
		}

		end++
	}

	return strings.ToLower(content[index:end]), end
}

func nextSQLWord(content string, index int) string {
	for index < len(content) {
		letter := content[index]

		if letter != ' ' && letter != '\t' && letter != '\n' && letter != '\r' {
			break
		}

		index++
	}

	if index >= len(content) || !isSQLWordStart(content[index]) {
		return ""
	}

	word, _ := readSQLWord(content, index)

	return word
}

func appendStatement(statements []string, statement string) []string {
	statement = strings.TrimSpace(statement)
	if statement == "" {
		return statements
	}

	return append(statements, statement)
}

func endOfLineComment(content string, index int) int {
	end := strings.IndexByte(content[index:], '\n')
	if end < 0 {
		return len(content)
	}

	return index + end + 1
}

// PostgreSQL nests a block comment, so this function counts the pairs.
func endOfBlockComment(content string, index int) int {
	depth := 0

	for index < len(content) {
		if strings.HasPrefix(content[index:], "/*") {
			depth++
			index += 2

			continue
		}

		if strings.HasPrefix(content[index:], "*/") {
			depth--
			index += 2

			if depth == 0 {
				return index
			}

			continue
		}

		index++
	}

	return len(content)
}

func endOfQuotedText(content string, index int) int {
	quote := content[index]
	escapes := quote == '\'' && index > 0 && (content[index-1] == 'E' || content[index-1] == 'e')

	index++

	for index < len(content) {
		// PostgreSQL reads a backslash as an escape in a string of the form E'...' only.
		// A string of another form holds the backslash as a character of the text.
		if escapes && content[index] == '\\' {
			index += 2

			continue
		}

		if content[index] != quote {
			index++

			continue
		}

		// Two quote characters are one character of the text.
		if index+1 < len(content) && content[index+1] == quote {
			index += 2

			continue
		}

		return index + 1
	}

	return len(content)
}

func endOfDollarQuotedText(content string, index int) int {
	tag := dollarQuoteTag(content, index)
	if tag == "" {
		return index + 1
	}

	end := strings.Index(content[index+len(tag):], tag)
	if end < 0 {
		return len(content)
	}

	return index + len(tag) + end + len(tag)
}

// A tag is $$ or $name$. A dollar that no tag follows names a parameter, for example $1.
func dollarQuoteTag(content string, index int) string {
	for offset := index + 1; offset < len(content); offset++ {
		letter := content[offset]

		if letter == '$' {
			return content[index : offset+1]
		}

		isTagLetter := letter == '_' ||
			(letter >= 'a' && letter <= 'z') ||
			(letter >= 'A' && letter <= 'Z') ||
			(offset > index+1 && letter >= '0' && letter <= '9')

		if !isTagLetter {
			return ""
		}
	}

	return ""
}
