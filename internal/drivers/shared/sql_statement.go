package driversshared

import (
	"slices"
	"strings"
)

const (
	NoTransactionDirective      = "-- dbdiff:no-transaction"
	AtlasNoTransactionDirective = "-- atlas:txmode none"
)

// FileUsesTransaction reports whether a migration file runs in one transaction.
func FileUsesTransaction(content string) bool {
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)

		if trimmed == NoTransactionDirective || trimmed == AtlasNoTransactionDirective {
			return false
		}
	}

	return true
}

// SplitSQLStatements splits a SQL file into its statements.
func SplitSQLStatements(content string, backslashEscapes bool) []string {
	var statements []string
	var state statementState

	start := 0
	index := 0

	for index < len(content) {
		end := endOfIgnoredText(content, index, backslashEscapes)

		switch {
		case end > index:
			index = end
		case content[index] == ';' && state.endsAtSemicolon():
			statements = appendStatement(statements, content[start:index])
			index++
			start = index
			state = statementState{}
		default:
			index = state.read(content, index)
		}
	}

	return appendStatement(statements, content[start:])
}

// endOfIgnoredText returns the end of the comment or quoted text at index, or index.
func endOfIgnoredText(content string, index int, backslashEscapes bool) int {
	rest := content[index:]

	switch {
	case strings.HasPrefix(rest, "--"):
		return EndOfLineComment(content, index)
	case strings.HasPrefix(rest, "/*"):
		return endOfBlockComment(content, index)
	case backslashEscapes && (rest[0] == '\'' || rest[0] == '"'):
		return endOfQuotedText(content, index, true)
	case rest[0] == '\'' || rest[0] == '"' || rest[0] == '`':
		return EndOfQuotedText(content, index)
	case rest[0] == '$':
		return endOfDollarQuotedText(content, index)
	}

	return index
}

// statementState tracks where the splitter is in the current statement.
type statementState struct {
	parenthesisDepth int
	blockDepth       int
	ownsBody         bool
}

// endsAtSemicolon reports whether a semicolon ends the statement here.
func (s *statementState) endsAtSemicolon() bool {
	return s.parenthesisDepth == 0 && s.blockDepth == 0
}

// read updates the state with the character or word at index and returns the next index.
func (s *statementState) read(content string, index int) int {
	switch {
	case content[index] == '(':
		s.parenthesisDepth++
	case content[index] == ')':
		s.parenthesisDepth--
	case isSQLWordStart(content[index]):
		return s.readWord(content, index)
	}

	return index + 1
}

// readWord updates the block state with the word at index and returns the next index.
func (s *statementState) readWord(content string, index int) int {
	word, end := readSQLWord(content, index)
	nextWord, nextEnd := nextSQLWord(content, end)

	switch {
	case isBodyOwnerKeyword(word) && s.blockDepth == 0:
		s.ownsBody = true
	case word == "begin" && (s.blockDepth > 0 || s.ownsBody || nextWord == "atomic"):
		s.blockDepth++
	case word == "case" && s.blockDepth > 0:
		s.blockDepth++
	case word == "end" && s.blockDepth > 0 && slices.Contains([]string{"if", "loop", "while", "repeat"}, nextWord):
		return nextEnd
	case word == "end" && s.blockDepth > 0:
		s.blockDepth--

		if nextWord == "case" {
			return nextEnd
		}
	}

	return end
}

// isBodyOwnerKeyword reports whether the word starts an object that holds a body.
func isBodyOwnerKeyword(word string) bool {
	return word == "trigger" || word == "procedure" || word == "function" || word == "event"
}

// isSQLWordStart reports whether the letter can start a word.
func isSQLWordStart(letter byte) bool {
	return letter == '_' ||
		(letter >= 'a' && letter <= 'z') ||
		(letter >= 'A' && letter <= 'Z')
}

// isSQLWordLetter reports whether the letter can continue a word.
func isSQLWordLetter(letter byte) bool {
	return isSQLWordStart(letter) || (letter >= '0' && letter <= '9')
}

// readSQLWord returns the lowercase word at index and its end.
func readSQLWord(content string, index int) (string, int) {
	end := index

	for end < len(content) && isSQLWordLetter(content[end]) {
		end++
	}

	return strings.ToLower(content[index:end]), end
}

// nextSQLWord returns the lowercase word after the spaces at index and its end.
func nextSQLWord(content string, index int) (string, int) {
	start := len(content) - len(strings.TrimLeft(content[index:], " \t\n\r"))
	if start == len(content) || !isSQLWordStart(content[start]) {
		return "", index
	}

	return readSQLWord(content, start)
}

// appendStatement adds the trimmed statement if it is not empty.
func appendStatement(statements []string, statement string) []string {
	statement = strings.TrimSpace(statement)
	if statement == "" {
		return statements
	}

	return append(statements, statement)
}

// EndOfLineComment returns the end of the line comment at index.
func EndOfLineComment(content string, index int) int {
	end := strings.IndexByte(content[index:], '\n')
	if end < 0 {
		return len(content)
	}

	return index + end + 1
}

// endOfBlockComment returns the end of the block comment at index.
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

// EndOfQuotedText returns the end of the quoted text at index.
func EndOfQuotedText(content string, index int) int {
	escapes := content[index] == '\'' && index > 0 && (content[index-1] == 'E' || content[index-1] == 'e')

	return endOfQuotedText(content, index, escapes)
}

// endOfQuotedText returns the end of the quoted text at index.
func endOfQuotedText(content string, index int, escapes bool) int {
	quote := content[index]

	index++

	for index < len(content) {
		if escapes && content[index] == '\\' {
			index += 2

			continue
		}

		if content[index] != quote {
			index++

			continue
		}

		if index+1 < len(content) && content[index+1] == quote {
			index += 2

			continue
		}

		return index + 1
	}

	return len(content)
}

// endOfDollarQuotedText returns the end of the dollar-quoted text at index, or index.
func endOfDollarQuotedText(content string, index int) int {
	tag := dollarQuoteTag(content, index)
	if tag == "" {
		return index
	}

	end := strings.Index(content[index+len(tag):], tag)
	if end < 0 {
		return len(content)
	}

	return index + len(tag) + end + len(tag)
}

// dollarQuoteTag returns the dollar-quote tag at index, or an empty string.
func dollarQuoteTag(content string, index int) string {
	for offset := index + 1; offset < len(content); offset++ {
		letter := content[offset]

		if letter == '$' {
			return content[index : offset+1]
		}

		isTagLetter := isSQLWordStart(letter) || (offset > index+1 && isSQLWordLetter(letter))
		if !isTagLetter {
			return ""
		}
	}

	return ""
}
