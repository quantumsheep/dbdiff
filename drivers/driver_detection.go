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

func DetectDriver(target string, source string) (string, error) {
	targetDriverName := detectDriverOfArgument(target)
	sourceDriverName := detectDriverOfArgument(source)

	if targetDriverName == "" && sourceDriverName == "" {
		return "", fmt.Errorf("cannot detect the driver of %q and %q. Use the --driver flag", target, source)
	}

	if targetDriverName != "" && sourceDriverName != "" && targetDriverName != sourceDriverName {
		return "", fmt.Errorf("%q names the %s driver and %q names the %s driver. Use the --driver flag", target, targetDriverName, source, sourceDriverName)
	}

	if targetDriverName != "" {
		return targetDriverName, nil
	}

	return sourceDriverName, nil
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
