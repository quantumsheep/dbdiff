package driversshared

import (
	"fmt"
	"slices"
	"strings"
)

type DriverName string

const (
	SQLiteDriverName   DriverName = "sqlite3"
	PostgresDriverName DriverName = "postgres"
	MySQLDriverName    DriverName = "mysql"
)

var SupportedDriverNames = []DriverName{SQLiteDriverName, PostgresDriverName, MySQLDriverName}

func DetectDriver(source string, target string) (DriverName, error) {
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

func detectDriverOfArgument(argument string) DriverName {
	if argument == "" {
		return ""
	}

	if strings.HasPrefix(argument, "sqlite://") {
		return SQLiteDriverName
	}

	if strings.HasPrefix(argument, "postgres://") || strings.HasPrefix(argument, "postgresql://") {
		return PostgresDriverName
	}

	if strings.HasPrefix(argument, "mysql://") || strings.HasPrefix(argument, "mariadb://") {
		return MySQLDriverName
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
	"application_name",
	"channel_binding",
	"client_encoding",
	"connect_timeout",
	"dbname",
	"fallback_application_name",
	"gssdelegation",
	"gssencmode",
	"gsslib",
	"host",
	"hostaddr",
	"keepalives",
	"keepalives_count",
	"keepalives_idle",
	"keepalives_interval",
	"krbsrvname",
	"load_balance_hosts",
	"options",
	"passfile",
	"password",
	"port",
	"replication",
	"requirepeer",
	"search_path",
	"service",
	"ssl_max_protocol_version",
	"ssl_min_protocol_version",
	"sslcert",
	"sslcertmode",
	"sslcompression",
	"sslcrl",
	"sslcrldir",
	"sslkey",
	"sslmode",
	"sslnegotiation",
	"sslpassword",
	"sslrootcert",
	"sslsni",
	"target_session_attrs",
	"tcp_user_timeout",
	"user",
}

// A value in single quotes can hold a space, and a backslash escapes the next character.
// A pair with an unknown keyword passes, because libpq adds keywords, but at least one
// keyword of the list must appear. Without that rule a file path such as stats=v2.db
// names the PostgreSQL driver.
func isPostgresKeywordString(argument string) bool {
	rest := strings.TrimSpace(argument)

	if rest == "" {
		return false
	}

	holdsKnownKeyword := false

	for rest != "" {
		keyword, remainder, found := strings.Cut(rest, "=")
		if !found {
			return false
		}

		keyword = strings.TrimSpace(keyword)
		if !isConnectionKeyword(keyword) {
			return false
		}

		if slices.Contains(postgresConnectionKeywords, keyword) {
			holdsKnownKeyword = true
		}

		rest = strings.TrimLeft(remainder, " \t")

		if strings.HasPrefix(rest, "'") {
			end := indexAfterQuotedValue(rest)
			if end < 0 {
				return false
			}

			rest = strings.TrimSpace(rest[end:])
			continue
		}

		space := strings.IndexAny(rest, " \t")
		if space < 0 {
			rest = ""
			continue
		}

		rest = strings.TrimSpace(rest[space:])
	}

	return holdsKnownKeyword
}

func isConnectionKeyword(keyword string) bool {
	if keyword == "" {
		return false
	}

	for _, character := range keyword {
		lower := character >= 'a' && character <= 'z'
		digit := character >= '0' && character <= '9'

		if !lower && !digit && character != '_' {
			return false
		}
	}

	return true
}

func indexAfterQuotedValue(rest string) int {
	for position := 1; position < len(rest); position++ {
		if rest[position] == '\\' {
			position++
			continue
		}

		if rest[position] == '\'' {
			return position + 1
		}
	}

	return -1
}
