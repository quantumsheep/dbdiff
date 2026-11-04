package driversmysql

import (
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type MySQLView struct {
	Name       string
	Definition string
}

func (v *MySQLView) Instructions() []driversshared.Instruction {
	return []driversshared.Instruction{&MySQLCreateViewInstruction{
		Name:       v.Name,
		Definition: v.Definition,
	}}
}

func (v *MySQLView) Diff(other *MySQLView) ([]driversshared.Instruction, error) {
	if v.Definition == other.Definition {
		return nil, nil
	}

	return []driversshared.Instruction{&MySQLCreateViewInstruction{
		Name:       v.Name,
		Definition: v.Definition,
		OrReplace:  true,
	}}, nil
}

// A string literal can hold the qualifier text, so the removal skips each string literal.
// MySQL and MariaDB escape a quote of a literal with a backslash in every SQL mode.
func removeDatabaseQualifier(definition string, databaseQualifier string) string {
	var builder strings.Builder

	position := 0

	for position < len(definition) {
		if strings.HasPrefix(definition[position:], databaseQualifier) {
			position += len(databaseQualifier)

			continue
		}

		end := position + 1

		switch definition[position] {
		case '`':
			end = driversshared.EndOfQuotedText(definition, position)
		case '\'':
			end = endOfStringLiteral(definition, position)
		}

		builder.WriteString(definition[position:end])
		position = end
	}

	return builder.String()
}

func endOfStringLiteral(content string, index int) int {
	index++

	for index < len(content) {
		switch content[index] {
		case '\\':
			index += 2
		case '\'':
			return index + 1
		default:
			index++
		}
	}

	return len(content)
}
