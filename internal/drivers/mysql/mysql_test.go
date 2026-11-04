package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

const mysqlTestConnectionString = "root:password@tcp(localhost:3306)/dbdiff"
const mariadbTestConnectionString = "root:password@tcp(localhost:3307)/dbdiff"

// The tests of the MySQL driver need the two servers of docker-compose.yml, and a runner
// of macOS or of Windows starts no service container. The variable stays empty on a
// runner of Linux, because a silent skip hides a server that fails there.
const skipMySQLServerVariable = "DBDIFF_TEST_SKIP_MYSQL"

func skipWithoutMySQLServer(tb testing.TB) {
	tb.Helper()

	if os.Getenv(skipMySQLServerVariable) != "" {
		tb.Skipf("%s stops the tests that need a MySQL server", skipMySQLServerVariable)
	}
}

type TestingMySQLDriver struct {
	*MySQLDriver

	tb   testing.TB
	conn *sql.DB

	current driversshared.DataSource
	desired driversshared.DataSource

	desiredDatabase string
	currentDatabase string

	CompareData bool

	currentConnection *sql.DB
	desiredConnection *sql.DB
}

func NewTestMySQLDriver(tb testing.TB) *TestingMySQLDriver {
	tb.Helper()

	return newTestMySQLDriverWithServer(tb, mysqlTestConnectionString)
}

func NewTestMariaDBDriver(tb testing.TB) *TestingMySQLDriver {
	tb.Helper()

	return newTestMySQLDriverWithServer(tb, mariadbTestConnectionString)
}

func newTestMySQLDriverWithServer(tb testing.TB, serverConnectionString string) *TestingMySQLDriver {
	tb.Helper()

	skipWithoutMySQLServer(tb)

	conn, err := OpenMySQLConnection(serverConnectionString)
	require.NoError(tb, err)

	err = conn.PingContext(tb.Context())
	require.NoError(tb, err)

	id := time.Now().UnixNano()
	desiredDatabase := fmt.Sprintf("desired_%d", id)
	currentDatabase := fmt.Sprintf("current_%d", id)

	_, err = conn.ExecContext(tb.Context(), "CREATE DATABASE "+QuoteIdentifier(desiredDatabase))
	require.NoError(tb, err)
	_, err = conn.ExecContext(tb.Context(), "CREATE DATABASE "+QuoteIdentifier(currentDatabase))
	require.NoError(tb, err)

	// The connection stays open for this cleanup. A closed connection drops no database.
	tb.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP DATABASE "+QuoteIdentifier(desiredDatabase))
		require.NoError(tb, err)

		_, err = conn.ExecContext(context.Background(), "DROP DATABASE "+QuoteIdentifier(currentDatabase))
		require.NoError(tb, err)

		require.NoError(tb, conn.Close())
	})

	desiredConnectionString := testConnectionStringWithDatabase(tb, serverConnectionString, desiredDatabase)
	currentConnectionString := testConnectionStringWithDatabase(tb, serverConnectionString, currentDatabase)

	// The harness diffs a call at a time, and the driver fields go nil between two calls, so
	// the harness keeps its own connection of each side for ExecOnDesired and ExecOnCurrent.
	desiredConnection, err := OpenMySQLConnection(desiredConnectionString)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, desiredConnection.Close())
	})

	currentConnection, err := OpenMySQLConnection(currentConnectionString)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, currentConnection.Close())
	})

	driver := NewMySQLDriver(&MySQLDriverConfig{})

	return &TestingMySQLDriver{
		MySQLDriver:       driver,
		tb:                tb,
		conn:              conn,
		current:           driversshared.ConnectionStringDataSource{ConnectionString: currentConnectionString},
		desired:           driversshared.ConnectionStringDataSource{ConnectionString: desiredConnectionString},
		desiredDatabase:   desiredDatabase,
		currentDatabase:   currentDatabase,
		currentConnection: currentConnection,
		desiredConnection: desiredConnection,
	}
}

func NewTestMySQLDriverWithDesiredDirectory(tb testing.TB, directory string) *TestingMySQLDriver {
	tb.Helper()

	return newTestMySQLDriverWithDesiredDirectory(tb, mysqlTestConnectionString, directory)
}

func NewTestMariaDBDriverWithDesiredDirectory(tb testing.TB, directory string) *TestingMySQLDriver {
	tb.Helper()

	return newTestMySQLDriverWithDesiredDirectory(tb, mariadbTestConnectionString, directory)
}

// The driver materializes the desired directory as a scratch database on a temporary server.
func newTestMySQLDriverWithDesiredDirectory(
	tb testing.TB,
	serverConnectionString string,
	directory string,
) *TestingMySQLDriver {
	tb.Helper()

	skipWithoutMySQLServer(tb)

	conn, err := OpenMySQLConnection(serverConnectionString)
	require.NoError(tb, err)

	err = conn.PingContext(tb.Context())
	require.NoError(tb, err)

	currentDatabase := fmt.Sprintf("current_%d", time.Now().UnixNano())

	_, err = conn.ExecContext(tb.Context(), "CREATE DATABASE "+QuoteIdentifier(currentDatabase))
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP DATABASE "+QuoteIdentifier(currentDatabase))
		require.NoError(tb, err)

		require.NoError(tb, conn.Close())
	})

	currentConnectionString := testConnectionStringWithDatabase(tb, serverConnectionString, currentDatabase)

	currentConnection, err := OpenMySQLConnection(currentConnectionString)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, currentConnection.Close())
	})

	driver := NewMySQLDriver(&MySQLDriverConfig{})

	return &TestingMySQLDriver{
		MySQLDriver:       driver,
		tb:                tb,
		conn:              conn,
		current:           driversshared.ConnectionStringDataSource{ConnectionString: currentConnectionString},
		desired:           driversshared.ParseDataSource(directory),
		currentDatabase:   currentDatabase,
		currentConnection: currentConnection,
	}
}

func testConnectionStringWithDatabase(tb testing.TB, serverConnectionString string, database string) string {
	tb.Helper()

	config, err := ParseMySQLConnectionString(serverConnectionString)
	require.NoError(tb, err)

	config.DBName = database

	return config.FormatDSN()
}

// CREATE USER fails when the user exists already, so IF NOT EXISTS keeps the tests
// repeatable.
func (d *TestingMySQLDriver) CreateTestUser(name string) {
	d.tb.Helper()

	_, err := d.conn.ExecContext(d.tb.Context(),
		"CREATE USER IF NOT EXISTS "+QuoteIdentifier(name)+"@'%';")
	require.NoError(d.tb, err)
}

func (d *TestingMySQLDriver) ExecOnDesired(sqlStatements string) {
	d.tb.Helper()

	_, err := d.desiredConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingMySQLDriver) ExecOnCurrent(sqlStatements string) {
	d.tb.Helper()

	_, err := d.currentConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingMySQLDriver) RequireInstructions(expected driversshared.Instructions) string {
	d.tb.Helper()

	instructions, err := d.Diff(d.tb.Context(), d.current, d.desired, driversshared.DiffOptions{CompareData: d.CompareData})
	require.NoError(d.tb, err)
	require.Equal(d.tb, expected, instructions)

	return instructions.String()
}

func (d *TestingMySQLDriver) RequireDiffApplies(expected driversshared.Instructions) {
	d.tb.Helper()

	d.ExecOnCurrent(d.RequireInstructions(expected))
	d.RequireInstructions(nil)
}

// The driver of MySQL gives an int64 value for an integer column and a byte slice for
// every other column of a plain query. The rows keep the int64 values and turn the byte
// slices into strings.
func (d *TestingMySQLDriver) FetchAllFromCurrent(table string, additionalRules string) []map[string]any {
	d.tb.Helper()

	rows, err := d.currentConnection.Query(
		fmt.Sprintf("SELECT * FROM %s %s;", QuoteIdentifier(table), additionalRules))
	require.NoError(d.tb, err)

	defer func() { require.NoError(d.tb, rows.Close()) }()

	columns, err := rows.Columns()
	require.NoError(d.tb, err)

	var results []map[string]any

	for rows.Next() {
		columnValues := make([]any, len(columns))

		columnPointers := make([]any, len(columns))
		for i := range columnPointers {
			columnPointers[i] = &columnValues[i]
		}

		err := rows.Scan(columnPointers...)
		require.NoError(d.tb, err)

		row := make(map[string]any)

		for i, column := range columns {
			value := columnValues[i]

			text, isText := value.([]byte)
			if isText {
				value = string(text)
			}

			row[column] = value
		}

		results = append(results, row)
	}

	require.NoError(d.tb, rows.Err())

	return results
}

func TestMySQLDriver(t *testing.T) {
	t.Run("NoDifference", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireInstructions(nil)
	})

	t.Run("CreateTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id int NOT NULL AUTO_INCREMENT,
				name varchar(100) NOT NULL,
				age int DEFAULT 18,
				PRIMARY KEY (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "users",
				Columns: []*MySQLColumn{
					{
						Name:          "id",
						Type:          "int",
						NotNull:       true,
						AutoIncrement: true,
					},
					{
						Name:    "name",
						Type:    "varchar(100)",
						NotNull: true,
					},
					{
						Name: "age",
						Type: "int",
						Default: sql.NullString{
							String: "18",
							Valid:  true,
						},
					},
				},
				PrimaryKey: []string{"id"},
			},
		})
	})

	t.Run("DropTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnCurrent("CREATE TABLE old_users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropTableInstruction{
				Name: "old_users",
			},
		})
	})

	t.Run("AddColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, email varchar(255), PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "email",
						Type: "varchar(255)",
					},
				},
			},
		})
	})

	t.Run("RemoveColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, email varchar(255), PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLDropColumnAction{
					ColumnName: "email",
				},
			},
		})
	})

	t.Run("ModifyColumnType", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, age bigint, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users (id, age) VALUES (1, 42);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "age",
						Type: "bigint",
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "age": int64(42)},
		}, driver.FetchAllFromCurrent("users", ""))
	})

	t.Run("RenameColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, full_name varchar(100), PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users (id, name) VALUES (1, 'Alice');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "name",
					NewColumnName: "full_name",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "full_name": "Alice"},
		}, driver.FetchAllFromCurrent("users", ""))
	})

	t.Run("ExpressionDefaultAndOnUpdate", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE events (
				id int NOT NULL,
				updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "events",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
					{
						Name: "updated_at",
						Type: "timestamp",
						Default: sql.NullString{
							String: "CURRENT_TIMESTAMP",
							Valid:  true,
						},
						DefaultIsExpression: true,
						OnUpdate:            "CURRENT_TIMESTAMP",
					},
				},
				PrimaryKey: []string{"id"},
			},
		})
	})

	t.Run("CreateIndex", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			CREATE INDEX idx_users_name ON users (name);
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{"`name`"},
			},
		})
	})

	t.Run("DropIndex", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			CREATE INDEX idx_users_name ON users (name);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
			},
		})
	})

	t.Run("ChangeIndexToUnique", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			CREATE UNIQUE INDEX idx_users_name ON users (name);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			CREATE INDEX idx_users_name ON users (name);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
			},
			&MySQLCreateIndexInstruction{
				Kind:      "UNIQUE",
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{"`name`"},
			},
		})
	})

	t.Run("IndexWithPrefixLengthAndDescendingKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE articles (id int NOT NULL, title text, score int, PRIMARY KEY (id));
			CREATE INDEX idx_articles ON articles (title(10), score DESC);
		`)
		driver.ExecOnCurrent("CREATE TABLE articles (id int NOT NULL, title text, score int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateIndexInstruction{
				Name:      "idx_articles",
				TableName: "articles",
				Keys:      []string{"`title`(10)", "`score` DESC"},
			},
		})
	})

	t.Run("CreateTablesWithForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id) ON DELETE CASCADE
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLSetForeignKeyChecksInstruction{},
			&MySQLCreateTableInstruction{
				Name: "authors",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
			},
			&MySQLCreateTableInstruction{
				Name: "books",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
					{
						Name: "author_id",
						Type: "int",
					},
				},
				PrimaryKey: []string{"id"},
				ForeignKeys: []*MySQLForeignKey{
					{
						Name:              "fk_books_author",
						Columns:           []string{"author_id"},
						ReferencedTable:   "authors",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "NO ACTION",
						OnDelete:          "CASCADE",
					},
				},
			},
			&MySQLSetForeignKeyChecksInstruction{
				Enabled: true,
			},
		})
	})

	t.Run("AddForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, author_id int, PRIMARY KEY (id));
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "books",
				Action: &MySQLAddForeignKeyAction{
					ForeignKey: &MySQLForeignKey{
						Name:              "fk_books_author",
						Columns:           []string{"author_id"},
						ReferencedTable:   "authors",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "NO ACTION",
						OnDelete:          "NO ACTION",
					},
				},
			},
		})
	})

	t.Run("AddForeignKeyToANewTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE publishers (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, publisher_id int, PRIMARY KEY (id));
			ALTER TABLE books ADD CONSTRAINT fk_books_publisher
				FOREIGN KEY (publisher_id) REFERENCES publishers (id);
		`)
		driver.ExecOnCurrent("CREATE TABLE books (id int NOT NULL, publisher_id int, PRIMARY KEY (id));")

		// The name order puts the change of the kept table before the creation of the
		// referenced table, so the enforcement of the foreign keys goes off.
		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLSetForeignKeyChecksInstruction{},
			&MySQLAlterTableInstruction{
				Name: "books",
				Action: &MySQLAddForeignKeyAction{
					ForeignKey: &MySQLForeignKey{
						Name:              "fk_books_publisher",
						Columns:           []string{"publisher_id"},
						ReferencedTable:   "publishers",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "NO ACTION",
						OnDelete:          "NO ACTION",
					},
				},
			},
			&MySQLCreateTableInstruction{
				Name: "publishers",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
			},
			&MySQLSetForeignKeyChecksInstruction{
				Enabled: true,
			},
		})
	})

	t.Run("RemoveForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, author_id int, PRIMARY KEY (id));
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "books",
				Action: &MySQLDropForeignKeyAction{
					Name: "fk_books_author",
				},
			},
			&MySQLDropIndexInstruction{
				Name:      "fk_books_author",
				TableName: "books",
			},
		})
	})

	t.Run("DropTableWithForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLSetForeignKeyChecksInstruction{},
			&MySQLDropTableInstruction{
				Name: "books",
			},
			&MySQLSetForeignKeyChecksInstruction{
				Enabled: true,
			},
		})
	})

	t.Run("AddCheckConstraint", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id int NOT NULL,
				age int,
				PRIMARY KEY (id),
				CONSTRAINT chk_users_age CHECK (age > 0)
			);
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:       "chk_users_age",
						Expression: "(`age` > 0)",
					},
				},
			},
		})
	})

	t.Run("RemoveCheckConstraint", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id int NOT NULL,
				age int,
				PRIMARY KEY (id),
				CONSTRAINT chk_users_age CHECK (age > 0)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLDropCheckConstraintAction{
					Name: "chk_users_age",
				},
			},
		})
	})

	t.Run("ChangePrimaryKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE members (id int NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id, tenant_id));")
		driver.ExecOnCurrent("CREATE TABLE members (id int NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name:   "members",
				Action: &MySQLDropPrimaryKeyAction{},
			},
			&MySQLAlterTableInstruction{
				Name: "members",
				Action: &MySQLAddPrimaryKeyAction{
					Columns: []string{"id", "tenant_id"},
				},
			},
		})
	})

	t.Run("RemoveAutoIncrementAndPrimaryKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE logs (id int NOT NULL, message text);")
		driver.ExecOnCurrent("CREATE TABLE logs (id int NOT NULL AUTO_INCREMENT, message text, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "logs",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name:   "logs",
				Action: &MySQLDropPrimaryKeyAction{},
			},
		})
	})

	t.Run("GeneratedColumns", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE prices (
				id int NOT NULL,
				net int,
				gross int GENERATED ALWAYS AS (net * 2) STORED,
				doubled int GENERATED ALWAYS AS (net + net) VIRTUAL,
				PRIMARY KEY (id)
			);
		`)
		driver.ExecOnCurrent("CREATE TABLE prices (id int NOT NULL, net int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:                "gross",
						Type:                "int",
						GeneratedExpression: "(`net` * 2)",
						GeneratedStored:     true,
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:                "doubled",
						Type:                "int",
						GeneratedExpression: "(`net` + `net`)",
					},
				},
			},
		})
	})

	t.Run("ChangeColumnToGenerated", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE prices (
				id int NOT NULL,
				net int,
				gross int GENERATED ALWAYS AS (net * 2) STORED,
				PRIMARY KEY (id)
			);
		`)
		driver.ExecOnCurrent("CREATE TABLE prices (id int NOT NULL, net int, gross int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLDropColumnAction{
					ColumnName: "gross",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:                "gross",
						Type:                "int",
						GeneratedExpression: "(`net` * 2)",
						GeneratedStored:     true,
					},
				},
			},
		})
	})

	t.Run("CreateView", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateViewInstruction{
				Name:       "adult_users",
				Definition: "select `users`.`id` AS `id` from `users` where (`users`.`age` >= 18)",
			},
		})
	})

	t.Run("ChangeView", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 21;
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateViewInstruction{
				Name:       "adult_users",
				Definition: "select `users`.`id` AS `id` from `users` where (`users`.`age` >= 21)",
				OrReplace:  true,
			},
		})
	})

	t.Run("DropView", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropViewInstruction{
				Name: "adult_users",
			},
		})
	})

	t.Run("CreateViewKeepsDatabaseNameInStringLiteral", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		label := "`" + driver.desiredDatabase + "`.x"

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));" +
			"CREATE VIEW labeled_users AS SELECT id, '" + label + "' AS label FROM users;")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateViewInstruction{
				Name:       "labeled_users",
				Definition: "select `users`.`id` AS `id`,'" + label + "' AS `label` from `users`",
			},
		})
	})

	t.Run("CreateTableWithEngineAndCollation", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE logs (id int NOT NULL, PRIMARY KEY (id)) ENGINE = MyISAM DEFAULT COLLATE = utf8mb4_bin;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "logs",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
				Engine:     "MyISAM",
				Collation:  "utf8mb4_bin",
			},
		})
	})

	t.Run("ChangeEngine", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE logs (id int NOT NULL, PRIMARY KEY (id)) ENGINE = MyISAM;")
		driver.ExecOnCurrent("CREATE TABLE logs (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "logs",
				Action: &MySQLEngineAction{
					Engine: "MyISAM",
				},
			},
		})
	})

	t.Run("ChangeTableCollation", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE messages (id int NOT NULL, body varchar(100), PRIMARY KEY (id)) DEFAULT COLLATE = utf8mb4_bin;")
		driver.ExecOnCurrent("CREATE TABLE messages (id int NOT NULL, body varchar(100), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO messages (id, body) VALUES (1, 'hello');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "messages",
				Action: &MySQLConvertToCharacterSetAction{
					CharacterSet: "utf8mb4",
					Collation:    "utf8mb4_bin",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "body": "hello"},
		}, driver.FetchAllFromCurrent("messages", ""))
	})

	t.Run("ChangeTableCollationWithAColumnCollation", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE messages (id int NOT NULL, body varchar(100) COLLATE utf8mb4_general_ci, PRIMARY KEY (id)) DEFAULT COLLATE = utf8mb4_bin;")
		driver.ExecOnCurrent("CREATE TABLE messages (id int NOT NULL, body varchar(100) COLLATE utf8mb4_general_ci, PRIMARY KEY (id));")

		// The conversion rewrites the collation of the body column, so the column needs
		// its definition again in the same run.
		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "messages",
				Action: &MySQLConvertToCharacterSetAction{
					CharacterSet: "utf8mb4",
					Collation:    "utf8mb4_bin",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "messages",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:      "body",
						Type:      "varchar(100)",
						Collation: "utf8mb4_general_ci",
					},
				},
			},
		})
	})

	t.Run("CreatePartitionedTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 4;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "metrics",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
				Partition:  "PARTITION BY HASH (`id`)\nPARTITIONS 4",
			},
		})
	})

	t.Run("AddPartitioning", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY KEY (id) PARTITIONS 2;")
		driver.ExecOnCurrent("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO metrics (id) VALUES (1), (2);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "metrics",
				Action: &MySQLPartitionAction{
					Clause: "PARTITION BY KEY (id)\nPARTITIONS 2",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1)},
			{"id": int64(2)},
		}, driver.FetchAllFromCurrent("metrics", "ORDER BY id"))
	})

	t.Run("RemovePartitioning", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 4;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name:   "metrics",
				Action: &MySQLRemovePartitioningAction{},
			},
		})
	})

	t.Run("CreateTrigger", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, updated_count int DEFAULT 0, PRIMARY KEY (id));
			CREATE TRIGGER trg_users_update BEFORE UPDATE ON users FOR EACH ROW
			BEGIN
				SET NEW.updated_count = OLD.updated_count + 1;
			END;
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, updated_count int DEFAULT 0, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users (id) VALUES (1);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTriggerInstruction{
				Name:      "trg_users_update",
				Timing:    "BEFORE",
				Event:     "UPDATE",
				TableName: "users",
				Statement: "BEGIN\n\t\t\t\tSET NEW.updated_count = OLD.updated_count + 1;\n\t\t\tEND",
			},
		})

		driver.ExecOnCurrent("UPDATE users SET id = 1 WHERE id = 1;")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "updated_count": int64(1)},
		}, driver.FetchAllFromCurrent("users", ""))
	})

	t.Run("ChangeTrigger", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_users_insert BEFORE INSERT ON users FOR EACH ROW SET NEW.note = 'new';
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_users_insert BEFORE INSERT ON users FOR EACH ROW SET NEW.note = 'old';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropTriggerInstruction{
				Name: "trg_users_insert",
			},
			&MySQLCreateTriggerInstruction{
				Name:      "trg_users_insert",
				Timing:    "BEFORE",
				Event:     "INSERT",
				TableName: "users",
				Statement: "SET NEW.note = 'new'",
			},
		})
	})

	t.Run("DropTrigger", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TRIGGER trg_users_delete AFTER DELETE ON users FOR EACH ROW SET @deleted = 1;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropTriggerInstruction{
				Name: "trg_users_delete",
			},
		})
	})

	t.Run("CreateRoutines", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE PROCEDURE prune_users(IN max_age int) DELETE FROM users WHERE age > max_age;
			CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x * 2;
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateRoutineInstruction{
				Type:       "FUNCTION",
				Name:       "double_it",
				Definition: "CREATE FUNCTION `double_it`(x int) RETURNS int\n    DETERMINISTIC\nRETURN x * 2",
			},
			&MySQLCreateRoutineInstruction{
				Type:       "PROCEDURE",
				Name:       "prune_users",
				Definition: "CREATE PROCEDURE `prune_users`(IN max_age int)\nDELETE FROM users WHERE age > max_age",
			},
		})
	})

	t.Run("ChangeFunction", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x + x;")
		driver.ExecOnCurrent("CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x * 2;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropRoutineInstruction{
				Type: "FUNCTION",
				Name: "double_it",
			},
			&MySQLCreateRoutineInstruction{
				Type:       "FUNCTION",
				Name:       "double_it",
				Definition: "CREATE FUNCTION `double_it`(x int) RETURNS int\n    DETERMINISTIC\nRETURN x + x",
			},
		})
	})

	t.Run("DropProcedure", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnCurrent("CREATE PROCEDURE noop() SET @noop = 1;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropRoutineInstruction{
				Type: "PROCEDURE",
				Name: "noop",
			},
		})
	})

	t.Run("ViewThatCallsAFunction", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x * 2;
			CREATE VIEW doubled_users AS SELECT double_it(id) AS doubled FROM users;
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateRoutineInstruction{
				Type:       "FUNCTION",
				Name:       "double_it",
				Definition: "CREATE FUNCTION `double_it`(x int) RETURNS int\n    DETERMINISTIC\nRETURN x * 2",
			},
			&MySQLCreateViewInstruction{
				Name:       "doubled_users",
				Definition: "select `double_it`(`users`.`id`) AS `doubled` from `users`",
			},
		})
	})

	t.Run("CreateAndDropEvent", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")
		driver.ExecOnCurrent("CREATE EVENT old_nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateEventInstruction{
				Name:       "nightly",
				Definition: "CREATE EVENT `nightly` ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' ON COMPLETION NOT PRESERVE ENABLE DO SET @cleaned = 1",
			},
			&MySQLDropEventInstruction{
				Name: "old_nightly",
			},
		})
	})

	t.Run("ChangeEvent", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE EVENT nightly ON SCHEDULE EVERY 2 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")
		driver.ExecOnCurrent("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropEventInstruction{
				Name: "nightly",
			},
			&MySQLCreateEventInstruction{
				Name:       "nightly",
				Definition: "CREATE EVENT `nightly` ON SCHEDULE EVERY 2 DAY STARTS '2030-01-01 00:00:00' ON COMPLETION NOT PRESERVE ENABLE DO SET @cleaned = 1",
			},
		})
	})

	t.Run("EventsWithTwoStartTimesCompareAsEqual", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")
		driver.ExecOnCurrent("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2031-06-15 12:00:00' DO SET @cleaned = 1;")

		driver.RequireInstructions(nil)
	})

	t.Run("RevokeGrantOption", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE posts (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT ON users TO 'dbdiff_reader'@'%' WITH GRANT OPTION;
			GRANT SELECT ON posts TO 'dbdiff_reader'@'%' WITH GRANT OPTION;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropTableInstruction{
				Name: "posts",
			},
			&MySQLRevokeInstruction{
				Privileges: []string{"GRANT OPTION"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
			&MySQLRevokeInstruction{
				Privileges: []string{"SELECT", "GRANT OPTION"},
				TableName:  "posts",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})
	})

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

	t.Run("DataComparison", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), age int, PRIMARY KEY (id));
			INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (3, 'Cara', NULL);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), age int, PRIMARY KEY (id));
			INSERT INTO users (id, name, age) VALUES (1, 'Alicia', 30), (2, 'Bob', 40);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name", "age"},
				Expressions: []string{"3", "'Cara'", "NULL"},
			},
			&MySQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*MySQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Alice'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&MySQLDeleteInstruction{
				TableName: "users",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "age": int64(30)},
			{"id": int64(3), "name": "Cara", "age": nil},
		}, driver.FetchAllFromCurrent("users", "ORDER BY id"))
	})

	t.Run("DataOfANewTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		driver.ExecOnDesired(`
			CREATE TABLE readings (
				id int NOT NULL,
				taken_at datetime,
				payload varbinary(4),
				price decimal(5,2),
				PRIMARY KEY (id)
			);
			INSERT INTO readings VALUES (1, '2030-01-02 03:04:05', X'DEADBEEF', 12.50);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "readings",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
					{
						Name: "taken_at",
						Type: "datetime",
					},
					{
						Name: "payload",
						Type: "varbinary(4)",
					},
					{
						Name: "price",
						Type: "decimal(5,2)",
					},
				},
				PrimaryKey: []string{"id"},
			},
			&MySQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "taken_at", "payload", "price"},
				Expressions: []string{"1", "'2030-01-02 03:04:05'", "X'deadbeef'", "12.50"},
			},
		})
	})

	t.Run("DataOfATableWithAGeneratedPrimaryKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		schema := "CREATE TABLE users (id int NOT NULL, code int GENERATED ALWAYS AS (id * 10) STORED NOT NULL, name varchar(100), PRIMARY KEY (code));"

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');")
		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent("INSERT INTO users (id, name) VALUES (1, 'Alicia'), (3, 'Cara');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"2", "'Bob'"},
			},
			&MySQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*MySQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Alice'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
							ColumnName: "code",
							Expression: "10",
						},
					},
				},
			},
			&MySQLDeleteInstruction{
				TableName: "users",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
							ColumnName: "code",
							Expression: "30",
						},
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "code": int64(10), "name": "Alice"},
			{"id": int64(2), "code": int64(20), "name": "Bob"},
		}, driver.FetchAllFromCurrent("users", "ORDER BY code"))
	})

	t.Run("DataOfATableWithNoPrimaryKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		driver.ExecOnDesired("CREATE TABLE notes (body text);")
		driver.ExecOnCurrent("CREATE TABLE notes (body text);")
		driver.ExecOnDesired("INSERT INTO notes VALUES ('hello');")

		driver.RequireInstructions([]driversshared.Instruction{
			&driversshared.SQLCommentInstruction{
				Text: `The table "notes" holds no primary key, so dbdiff compares no row of it.`,
			},
		})
	})

	t.Run("GrantTablePrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT, UPDATE ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLGrantInstruction{
				Privileges: []string{"SELECT", "UPDATE"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})
	})

	t.Run("RevokeAndNarrowPrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE posts (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE posts (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT, DELETE ON users TO 'dbdiff_reader'@'%';
			GRANT SELECT ON posts TO 'dbdiff_reader'@'%';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLRevokeInstruction{
				Privileges: []string{"DELETE"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
			&MySQLRevokeInstruction{
				Privileges: []string{"SELECT"},
				TableName:  "posts",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})
	})

	t.Run("GrantColumnPrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));
			GRANT SELECT (id, name) ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnCurrent(
			"CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLGrantInstruction{
				Privileges: []string{"SELECT"},
				Columns:    []string{"id", "name"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})
	})

	t.Run("NarrowColumnPrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));
			GRANT SELECT (id, email) ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));
			GRANT SELECT (id, name) ON users TO 'dbdiff_reader'@'%';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLGrantInstruction{
				Privileges: []string{"SELECT"},
				Columns:    []string{"email"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
			&MySQLRevokeInstruction{
				Privileges: []string{"SELECT"},
				Columns:    []string{"name"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})
	})

	t.Run("RevokeColumnPrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired(
			"CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));
			GRANT SELECT (id, name) ON users TO 'dbdiff_reader'@'%';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLRevokeInstruction{
				Privileges: []string{"SELECT"},
				Columns:    []string{"id", "name"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})
	})

	t.Run("DatabasePrivilegesAndGrantOption", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired("GRANT SELECT ON * TO 'dbdiff_reader'@'%' WITH GRANT OPTION;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLGrantInstruction{
				Privileges:      []string{"SELECT"},
				Grantee:         "'dbdiff_reader'@'%'",
				WithGrantOption: true,
			},
		})
	})

	t.Run("MigrationHistoryTableStaysHidden", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnCurrent("CREATE TABLE dbdiff_migrations (version varchar(255) NOT NULL, PRIMARY KEY (version));")

		driver.RequireInstructions(nil)
	})

	t.Run("ColumnComment", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL COMMENT 'the identifier', PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "int",
						NotNull: true,
						Comment: "the identifier",
					},
				},
			},
		})
	})

	t.Run("IgnoredTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, email varchar(255), PRIMARY KEY (id));
			CREATE TABLE ignored_created (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_changed (id int NOT NULL, name varchar(255), PRIMARY KEY (id));
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_dropped (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_changed (id int NOT NULL, description varchar(255), PRIMARY KEY (id));
		`)

		driver.IgnoreTables = []string{"ignored_created", "ignored_dropped", "ignored_changed"}

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "email",
						Type: "varchar(255)",
					},
				},
			},
		})
	})
}

// MariaDB reads the same driver code, and its catalog reports the types, the defaults,
// the check clauses, and the view definitions with another text.
func TestMariaDBDriver(t *testing.T) {
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

	t.Run("CreateTableWithDefaults", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id int NOT NULL AUTO_INCREMENT,
				name varchar(100) NOT NULL DEFAULT 'anonymous',
				age int DEFAULT 18,
				created_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "users",
				Columns: []*MySQLColumn{
					{
						Name:          "id",
						Type:          "int(11)",
						NotNull:       true,
						AutoIncrement: true,
					},
					{
						Name:    "name",
						Type:    "varchar(100)",
						NotNull: true,
						Default: sql.NullString{
							String: "'anonymous'",
							Valid:  true,
						},
						DefaultIsExpression: true,
					},
					{
						Name: "age",
						Type: "int(11)",
						Default: sql.NullString{
							String: "18",
							Valid:  true,
						},
						DefaultIsExpression: true,
					},
					{
						Name: "created_at",
						Type: "timestamp",
						Default: sql.NullString{
							String: "current_timestamp()",
							Valid:  true,
						},
						DefaultIsExpression: true,
						OnUpdate:            "current_timestamp()",
					},
				},
				PrimaryKey: []string{"id"},
			},
		})
	})

	t.Run("CheckConstraint", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id int NOT NULL,
				age int,
				PRIMARY KEY (id),
				CONSTRAINT chk_users_age CHECK (age > 0)
			);
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:       "chk_users_age",
						Expression: "`age` > 0",
					},
				},
			},
		})
	})

	t.Run("GeneratedColumn", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE prices (
				id int NOT NULL,
				net int,
				gross int GENERATED ALWAYS AS (net * 2) STORED,
				PRIMARY KEY (id)
			);
		`)
		driver.ExecOnCurrent("CREATE TABLE prices (id int NOT NULL, net int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:                "gross",
						Type:                "int(11)",
						GeneratedExpression: "`net` * 2",
						GeneratedStored:     true,
					},
				},
			},
		})
	})

	t.Run("ForeignKey", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, author_id int, PRIMARY KEY (id));
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "books",
				Action: &MySQLAddForeignKeyAction{
					ForeignKey: &MySQLForeignKey{
						Name:              "fk_books_author",
						Columns:           []string{"author_id"},
						ReferencedTable:   "authors",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "RESTRICT",
						OnDelete:          "RESTRICT",
					},
				},
			},
		})
	})

	t.Run("Sequences", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE SEQUENCE order_numbers START WITH 1000 INCREMENT BY 10;")
		driver.ExecOnCurrent("CREATE SEQUENCE old_numbers;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateSequenceInstruction{
				Name:      "order_numbers",
				Start:     1000,
				Minimum:   1,
				Maximum:   9223372036854775806,
				Increment: 10,
				Cache:     1000,
			},
			&MySQLDropSequenceInstruction{
				Name: "old_numbers",
			},
		})
	})

	t.Run("ChangeSequence", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE SEQUENCE order_numbers INCREMENT BY 5;")
		driver.ExecOnCurrent("CREATE SEQUENCE order_numbers INCREMENT BY 1;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropSequenceInstruction{
				Name: "order_numbers",
			},
			&MySQLCreateSequenceInstruction{
				Name:      "order_numbers",
				Start:     1,
				Minimum:   1,
				Maximum:   9223372036854775806,
				Increment: 5,
				Cache:     1000,
			},
		})
	})

	t.Run("ChangeSequenceOptions", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE SEQUENCE order_numbers MINVALUE 5 MAXVALUE 500 CACHE 20 CYCLE;")
		driver.ExecOnCurrent("CREATE SEQUENCE order_numbers MINVALUE 5 MAXVALUE 500 CACHE 10 NOCYCLE;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropSequenceInstruction{
				Name: "order_numbers",
			},
			&MySQLCreateSequenceInstruction{
				Name:      "order_numbers",
				Start:     5,
				Minimum:   5,
				Maximum:   500,
				Increment: 1,
				Cache:     20,
				Cycle:     true,
			},
		})
	})

	t.Run("Trigger", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_users_insert BEFORE INSERT ON users FOR EACH ROW SET NEW.note = 'new';
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, note varchar(20), PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTriggerInstruction{
				Name:      "trg_users_insert",
				Timing:    "BEFORE",
				Event:     "INSERT",
				TableName: "users",
				Statement: "SET NEW.note = 'new'",
			},
		})
	})

	t.Run("Function", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x * 2;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateRoutineInstruction{
				Type:       "FUNCTION",
				Name:       "double_it",
				Definition: "CREATE FUNCTION `double_it`(x int) RETURNS int(11)\n    DETERMINISTIC\nRETURN x * 2",
			},
		})
	})

	t.Run("Event", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateEventInstruction{
				Name:       "nightly",
				Definition: "CREATE EVENT `nightly` ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' ON COMPLETION NOT PRESERVE ENABLE DO SET @cleaned = 1",
			},
		})
	})

	t.Run("PartitionedTable", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 4;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "metrics",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int(11)",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
				Partition:  "PARTITION BY HASH (`id`)\nPARTITIONS 4",
			},
		})
	})

	t.Run("DataComparison", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)
		driver.CompareData = true

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			INSERT INTO users (id, name) VALUES (1, 'Alice');
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			INSERT INTO users (id, name) VALUES (1, 'Alicia'), (2, 'Bob');
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*MySQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Alice'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&MySQLDeleteInstruction{
				TableName: "users",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})
	})

	t.Run("View", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateViewInstruction{
				Name:       "adult_users",
				Definition: "select `users`.`id` AS `id` from `users` where `users`.`age` >= 18",
			},
		})
	})

	t.Run("IgnoredTable", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, email varchar(255), PRIMARY KEY (id));
			CREATE TABLE ignored_created (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_changed (id int NOT NULL, name varchar(255), PRIMARY KEY (id));
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_dropped (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_changed (id int NOT NULL, description varchar(255), PRIMARY KEY (id));
		`)

		driver.IgnoreTables = []string{"ignored_created", "ignored_dropped", "ignored_changed"}

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "email",
						Type: "varchar(255)",
					},
				},
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
