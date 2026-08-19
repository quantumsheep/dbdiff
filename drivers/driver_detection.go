package drivers

import (
	"fmt"
	"slices"
	"strings"
)

const (
	SQLiteDriverName   = "sqlite3"
	PostgresDriverName = "postgres"
)

var SupportedDriverNames = []string{SQLiteDriverName, PostgresDriverName}

// A connection URL and a database file name the engine. SQL text names no engine, so two
// SQL sources give an error, and the user gives the --driver flag.
func DetectDriver(source string, target string) (string, error) {
	sourceDriverName := detectDriverOfArgument(source)
	targetDriverName := detectDriverOfArgument(target)

	if sourceDriverName == "" && targetDriverName == "" {
		return "", fmt.Errorf("cannot detect the driver of %q and %q. Use the --driver flag", source, target)
	}

	if sourceDriverName != "" && targetDriverName != "" && sourceDriverName != targetDriverName {
		return "", fmt.Errorf("%q names the %s driver and %q names the %s driver. Use the --driver flag", source, sourceDriverName, target, targetDriverName)
	}

	if sourceDriverName != "" {
		return sourceDriverName, nil
	}

	return targetDriverName, nil
}

func detectDriverOfArgument(argument string) string {
	if argument == "" {
		return ""
	}

	if strings.HasPrefix(argument, "sqlite://") {
		return SQLiteDriverName
	}

	if strings.HasPrefix(argument, "postgres://") || strings.HasPrefix(argument, "postgresql://") {
		return PostgresDriverName
	}

	if strings.Contains(argument, "://") {
		return ""
	}

	if IsSQLSource(argument) {
		return ""
	}

	if isPostgresKeywordString(argument) {
		return PostgresDriverName
	}

	return SQLiteDriverName
}

var postgresConnectionKeywords = []string{
	"host",
	"hostaddr",
	"port",
	"dbname",
	"user",
	"password",
	"sslmode",
	"application_name",
	"connect_timeout",
	"search_path",
}

func isPostgresKeywordString(argument string) bool {
	fields := strings.Fields(argument)

	if len(fields) == 0 {
		return false
	}

	for _, field := range fields {
		keyword, _, found := strings.Cut(field, "=")
		if !found {
			return false
		}

		if !slices.Contains(postgresConnectionKeywords, keyword) {
			return false
		}
	}

	return true
}
