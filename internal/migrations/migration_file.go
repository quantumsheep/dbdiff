package migrations

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/quantumsheep/dbdiff/internal/drivers"
)

func RenderMigrationFile(toolVersion string, generatedAt time.Time, instructions []drivers.Instruction) string {
	var builder strings.Builder

	builder.WriteString("-- dbdiff ")
	builder.WriteString(toolVersion)
	builder.WriteString("\n")
	builder.WriteString("-- generated ")
	builder.WriteString(generatedAt.UTC().Format(time.RFC3339))
	builder.WriteString("\n")

	pendingComment := ""

	for _, instruction := range drivers.AnnotateInstructions(instructions) {
		comment, isComment := instruction.(*drivers.SQLCommentInstruction)
		if isComment {
			pendingComment = comment.String()

			continue
		}

		builder.WriteString("\n")

		if pendingComment != "" {
			builder.WriteString(pendingComment)
			builder.WriteString("\n")
			pendingComment = ""
		}

		builder.WriteString(instruction.String())
		builder.WriteString("\n")
	}

	return builder.String()
}

func WriteMigrationFiles(directory string, slug string, generatedAt time.Time,
	toolVersion string, instructions []drivers.Instruction) ([]string, error) {
	groups := splitMigrationInstructions(instructions)
	if len(groups) == 0 {
		return nil, nil
	}

	err := os.MkdirAll(directory, 0o750)
	if err != nil {
		return nil, err
	}

	used, err := usedMigrationVersions(directory)
	if err != nil {
		return nil, err
	}

	var paths []string

	moment := generatedAt

	for index, group := range groups {
		for used[MigrationVersionOfTime(moment)] {
			moment = moment.Add(time.Second)
		}

		version := MigrationVersionOfTime(moment)
		used[version] = true

		name := slug

		if len(groups) > 1 && index == 0 {
			name = slug + "_types"
		}

		path := filepath.Join(directory, version+"_"+name+".sql")

		content := RenderMigrationFile(toolVersion, moment, group)

		err := os.WriteFile(path, []byte(content), 0o600)
		if err != nil {
			return nil, err
		}

		paths = append(paths, path)

		moment = moment.Add(time.Second)
	}

	return paths, nil
}

// Two generate runs of one second give one version, and the sort of the replay then holds
// no order between the two files. The history table also refuses the second version, which
// its primary key holds already.
func usedMigrationVersions(directory string) (map[string]bool, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	used := make(map[string]bool, len(entries))

	for _, entry := range entries {
		version, _, found := strings.Cut(entry.Name(), "_")
		if found {
			used[version] = true
		}
	}

	return used, nil
}

// PostgreSQL accepts ALTER TYPE ... ADD VALUE in a transaction, and it refuses the new
// value in that same transaction. The values take a file of their own for that reason.
func splitMigrationInstructions(instructions []drivers.Instruction) [][]drivers.Instruction {
	var enumValues []drivers.Instruction
	var others []drivers.Instruction

	for _, instruction := range instructions {
		_, isEnumValue := instruction.(*drivers.PostgresAlterTypeAddValueInstruction)
		if isEnumValue {
			enumValues = append(enumValues, instruction)

			continue
		}

		others = append(others, instruction)
	}

	var groups [][]drivers.Instruction

	if len(enumValues) > 0 {
		groups = append(groups, enumValues)
	}

	if len(others) > 0 {
		groups = append(groups, others)
	}

	return groups
}
