package driversmysql

import (
	"runtime"
	"testing"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestMySQLSQLSource(t *testing.T) {
	t.Run("SQLSourceAsDesired", func(t *testing.T) {
		skipWithoutMySQLServer(t)

		if testing.Short() {
			t.Skip("the temporary mysql server needs a download on the first run")
		}

		directory := t.TempDir()
		sqltest.WriteSQLFile(t, directory, "001_users.sql",
			"CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")
		sqltest.WriteSQLFile(t, directory, "002_add_name.sql",
			"ALTER TABLE users ADD COLUMN name varchar(100);")
		sqltest.WriteSQLFile(t, directory, "002_add_name.down.sql",
			"ALTER TABLE users DROP COLUMN name;")

		driver := NewTestMySQLDriverWithDesiredDirectory(t, directory)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "users",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
					{
						Name: "name",
						Type: "varchar(100)",
					},
				},
				PrimaryKey: []string{"id"},
			},
		})
	})

	t.Run("SQLSourceAsDesiredOnAScratchDatabase", func(t *testing.T) {
		requireDiffOnScratchDatabase(t, mysqlTestConnectionString, "mysql://root:password@localhost:3306/",
			driversshared.Instructions{
				&MySQLCreateTableInstruction{
					Name: "users",
					Columns: []*MySQLColumn{
						{
							Name:    "id",
							Type:    "int",
							NotNull: true,
						},
						{
							Name: "age",
							Type: "int",
						},
					},
					PrimaryKey: []string{"id"},
				},
				&MySQLCreateViewInstruction{
					Name:       "adult_users",
					Definition: "select `users`.`id` AS `id` from `users` where (`users`.`age` >= 18)",
				},
			})
	})

	t.Run("ScratchDatabaseConnectionHoldsOneSession", func(t *testing.T) {
		directory := t.TempDir()
		sqltest.WriteSQLFile(t, directory, "001_users.sql", "CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver := newTestMySQLDriverWithDesiredDirectory(t, mysqlTestConnectionString, directory)
		driver.ScratchDatabase = "mysql://root:password@localhost:3306/"

		release, err := driver.openSides(t.Context(), driver.current, driver.desired)
		require.NoError(t, err)

		defer release()

		require.Equal(t, 1, driver.DesiredDatabaseConnection.Stats().MaxOpenConnections)
	})

	t.Run("TwoSQLSources", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary mysql server needs a download on the first run")
		}

		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "desired.sql",
			"CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));")
		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql",
			"CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver := NewMySQLDriver(&MySQLDriverConfig{})

		desired := driversshared.ParseDataSource(desiredPath)
		current := driversshared.ParseDataSource(currentPath)

		instructions, err := driver.Diff(t.Context(), current, desired, driversshared.DiffOptions{})
		require.NoError(t, err)

		require.Equal(t, driversshared.Instructions{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "name",
						Type: "varchar(100)",
					},
				},
			},
		}, instructions)
	})
}

func TestMariaDBSQLSource(t *testing.T) {
	// The type "int(11)" proves that the temporary server ran MariaDB. MySQL writes "int".
	t.Run("SQLSourceAsDesired", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary mariadb server needs a download on the first run")
		}

		if runtime.GOOS == "linux" && runtime.GOARCH == "arm64" {
			t.Skip("MariaDB publishes no binaries for Linux on arm64")
		}

		directory := t.TempDir()
		sqltest.WriteSQLFile(t, directory, "001_users.sql",
			"CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver := NewTestMariaDBDriverWithDesiredDirectory(t, directory)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "users",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int(11)",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
			},
		})
	})

	t.Run("SQLSourceAsDesiredOnAScratchDatabase", func(t *testing.T) {
		requireDiffOnScratchDatabase(t, mariadbTestConnectionString, "mariadb://root:password@localhost:3307/",
			driversshared.Instructions{
				&MySQLCreateTableInstruction{
					Name: "users",
					Columns: []*MySQLColumn{
						{
							Name:    "id",
							Type:    "int(11)",
							NotNull: true,
						},
						{
							Name: "age",
							Type: "int(11)",
						},
					},
					PrimaryKey: []string{"id"},
				},
				&MySQLCreateViewInstruction{
					Name:       "adult_users",
					Definition: "select `users`.`id` AS `id` from `users` where `users`.`age` >= 18",
				},
			})
	})
}

func TestParseMySQLConnectionString(t *testing.T) {
	t.Run("URLWithEveryPart", func(t *testing.T) {
		config, err := ParseMySQLConnectionString("mysql://user:secret@localhost:3307/app?tls=skip-verify")

		require.NoError(t, err)
		require.Equal(t, "user", config.User)
		require.Equal(t, "secret", config.Passwd)
		require.Equal(t, "tcp", config.Net)
		require.Equal(t, "localhost:3307", config.Addr)
		require.Equal(t, "app", config.DBName)
		require.Equal(t, "skip-verify", config.TLSConfig)
		require.Empty(t, config.Params)
		require.True(t, config.MultiStatements)
		require.True(t, config.ParseTime)
	})

	t.Run("URLWithDriverParameters", func(t *testing.T) {
		config, err := parseMySQLConnectionString("mysql://us%40er:p%3Fss@localhost/my%2Fdb?parseTime=true&timeout=5s&sql_mode=%27ANSI%27")

		require.NoError(t, err)
		require.Equal(t, "us@er", config.User)
		require.Equal(t, "p?ss", config.Passwd)
		require.Equal(t, "localhost:3306", config.Addr)
		require.Equal(t, "my/db", config.DBName)
		require.True(t, config.ParseTime)
		require.Equal(t, 5*time.Second, config.Timeout)
		require.Equal(t, map[string]string{"sql_mode": "'ANSI'"}, config.Params)
	})

	t.Run("URLWithNoPort", func(t *testing.T) {
		config, err := ParseMySQLConnectionString("mysql://user@localhost/app")

		require.NoError(t, err)
		require.Equal(t, "localhost:3306", config.Addr)
		require.Empty(t, config.Passwd)
	})

	t.Run("MariaDBURL", func(t *testing.T) {
		config, err := ParseMySQLConnectionString("mariadb://user@localhost/app")

		require.NoError(t, err)
		require.Equal(t, "app", config.DBName)
	})

	t.Run("PlainDataSourceName", func(t *testing.T) {
		config, err := ParseMySQLConnectionString("user:secret@tcp(localhost:3307)/app")

		require.NoError(t, err)
		require.Equal(t, "localhost:3307", config.Addr)
		require.Equal(t, "app", config.DBName)
		require.True(t, config.MultiStatements)
	})

	t.Run("InvalidDataSourceName", func(t *testing.T) {
		_, err := ParseMySQLConnectionString("this is no data source name")

		require.Error(t, err)
	})
}
