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

func DetectDriver(current DataSource, desired DataSource) (DriverName, error) {
	currentDriverName := detectDriverOfDataSource(current)
	desiredDriverName := detectDriverOfDataSource(desired)

	if currentDriverName == "" && desiredDriverName == "" {
		return "", fmt.Errorf("cannot detect the driver of the current schema %q and the desired schema %q. Name the driver",
			dataSourceArgument(current), dataSourceArgument(desired))
	}

	if currentDriverName != "" && desiredDriverName != "" && currentDriverName != desiredDriverName {
		return "", fmt.Errorf("the current schema %q names the %s driver and the desired schema %q names the %s driver. Name the driver",
			dataSourceArgument(current), currentDriverName, dataSourceArgument(desired), desiredDriverName)
	}

	if currentDriverName != "" {
		return currentDriverName, nil
	}

	return desiredDriverName, nil
}

// A migration holds one database, so the two-sided detection would name that database
// two times.
func DetectDriverOfDatabase(database DataSource) (DriverName, error) {
	driverName := detectDriverOfDataSource(database)
	if driverName == "" {
		return "", fmt.Errorf("cannot detect the driver of the database %q. Name the driver",
			dataSourceArgument(database))
	}

	return driverName, nil
}

// dataSourceArgument names the value that the user gave for a data source, so the
// detection error can name what defeated it.
func dataSourceArgument(source DataSource) string {
	switch value := source.(type) {
	case FileDataSource:
		return value.Path
	case FolderDataSource:
		return value.Path
	case ConnectionStringDataSource:
		return value.ConnectionString
	default:
		return ""
	}
}

func detectDriverOfDataSource(source DataSource) DriverName {
	connection, ok := source.(ConnectionStringDataSource)
	if !ok {
		return ""
	}

	argument := connection.ConnectionString

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
