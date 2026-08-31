package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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

	source driversshared.DataSource
	target driversshared.DataSource

	targetDatabase string
	sourceDatabase string

	CompareData bool

	sourceConnection *sql.DB
	targetConnection *sql.DB
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
	targetDatabase := fmt.Sprintf("target_%d", id)
	sourceDatabase := fmt.Sprintf("source_%d", id)

	_, err = conn.ExecContext(tb.Context(), "CREATE DATABASE "+QuoteIdentifier(targetDatabase))
	require.NoError(tb, err)
	_, err = conn.ExecContext(tb.Context(), "CREATE DATABASE "+QuoteIdentifier(sourceDatabase))
	require.NoError(tb, err)

	// The connection stays open for this cleanup. A closed connection drops no database.
	tb.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP DATABASE "+QuoteIdentifier(targetDatabase))
		require.NoError(tb, err)

		_, err = conn.ExecContext(context.Background(), "DROP DATABASE "+QuoteIdentifier(sourceDatabase))
		require.NoError(tb, err)

		require.NoError(tb, conn.Close())
	})

	targetConnectionString := testConnectionStringWithDatabase(tb, serverConnectionString, targetDatabase)
	sourceConnectionString := testConnectionStringWithDatabase(tb, serverConnectionString, sourceDatabase)

	// The harness diffs a call at a time, and the driver fields go nil between two calls, so
	// the harness keeps its own connection of each side for ExecOnTarget and ExecOnSource.
	targetConnection, err := OpenMySQLConnection(targetConnectionString)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, targetConnection.Close())
	})

	sourceConnection, err := OpenMySQLConnection(sourceConnectionString)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, sourceConnection.Close())
	})

	driver := NewMySQLDriver(&MySQLDriverConfig{})

	return &TestingMySQLDriver{
		MySQLDriver:      driver,
		tb:               tb,
		conn:             conn,
		source:           driversshared.ConnectionStringDataSource{ConnectionString: sourceConnectionString},
		target:           driversshared.ConnectionStringDataSource{ConnectionString: targetConnectionString},
		targetDatabase:   targetDatabase,
		sourceDatabase:   sourceDatabase,
		sourceConnection: sourceConnection,
		targetConnection: targetConnection,
	}
}

// The driver materializes the target directory as a scratch database on the server of
// the source side.
func NewTestMySQLDriverWithTargetDirectory(tb testing.TB, directory string) *TestingMySQLDriver {
	tb.Helper()

	skipWithoutMySQLServer(tb)

	conn, err := OpenMySQLConnection(mysqlTestConnectionString)
	require.NoError(tb, err)

	err = conn.PingContext(tb.Context())
	require.NoError(tb, err)

	sourceDatabase := fmt.Sprintf("source_%d", time.Now().UnixNano())

	_, err = conn.ExecContext(tb.Context(), "CREATE DATABASE "+QuoteIdentifier(sourceDatabase))
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP DATABASE "+QuoteIdentifier(sourceDatabase))
		require.NoError(tb, err)

		require.NoError(tb, conn.Close())
	})

	sourceConnectionString := testConnectionStringWithDatabase(tb, mysqlTestConnectionString, sourceDatabase)

	sourceConnection, err := OpenMySQLConnection(sourceConnectionString)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, sourceConnection.Close())
	})

	driver := NewMySQLDriver(&MySQLDriverConfig{})

	return &TestingMySQLDriver{
		MySQLDriver:      driver,
		tb:               tb,
		conn:             conn,
		source:           driversshared.ConnectionStringDataSource{ConnectionString: sourceConnectionString},
		target:           driversshared.ParseDataSource(directory),
		sourceDatabase:   sourceDatabase,
		sourceConnection: sourceConnection,
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

func (d *TestingMySQLDriver) ExecOnTarget(sqlStatements string) {
	d.tb.Helper()

	_, err := d.targetConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingMySQLDriver) ExecOnSource(sqlStatements string) {
	d.tb.Helper()

	_, err := d.sourceConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingMySQLDriver) RequireInstructions(expected []driversshared.Instruction) string {
	d.tb.Helper()

	instructions, err := d.Diff(d.tb.Context(), d.source, d.target, driversshared.DiffOptions{CompareData: d.CompareData})
	require.NoError(d.tb, err)
	require.Equal(d.tb, expected, instructions)

	return driversshared.RenderInstructions(instructions)
}

// The driver of MySQL gives an int64 value for an integer column and a byte slice for
// every other column of a plain query. The rows keep the int64 values and turn the byte
// slices into strings.
func (d *TestingMySQLDriver) FetchAllFromSource(table string, additionalRules string) []map[string]any {
	d.tb.Helper()

	rows, err := d.sourceConnection.Query(
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

		driver.ExecOnTarget("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireInstructions(nil)
	})

	t.Run("CreateTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id int NOT NULL AUTO_INCREMENT,
				name varchar(100) NOT NULL,
				age int DEFAULT 18,
				PRIMARY KEY (id)
			);
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("DropTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnSource("CREATE TABLE old_users (id int NOT NULL, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLDropTableInstruction{
				Name: "old_users",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("AddColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE users (id int NOT NULL, email varchar(255), PRIMARY KEY (id));")
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("RemoveColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, email varchar(255), PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLDropColumnAction{
					ColumnName: "email",
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ModifyColumnType", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE users (id int NOT NULL, age bigint, PRIMARY KEY (id));")
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")
		driver.ExecOnSource("INSERT INTO users (id, age) VALUES (1, 42);")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)

		require.Equal(t, []map[string]any{
			{"id": int64(1), "age": int64(42)},
		}, driver.FetchAllFromSource("users", ""))
	})

	t.Run("RenameColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE users (id int NOT NULL, full_name varchar(100), PRIMARY KEY (id));")
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));")
		driver.ExecOnSource("INSERT INTO users (id, name) VALUES (1, 'Alice');")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "name",
					NewColumnName: "full_name",
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)

		require.Equal(t, []map[string]any{
			{"id": int64(1), "full_name": "Alice"},
		}, driver.FetchAllFromSource("users", ""))
	})

	t.Run("ExpressionDefaultAndOnUpdate", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE events (
				id int NOT NULL,
				updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id)
			);
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			CREATE INDEX idx_users_name ON users (name);
		`)
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLCreateIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{"`name`"},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("DropIndex", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));")
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			CREATE INDEX idx_users_name ON users (name);
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLDropIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ChangeIndexToUnique", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			CREATE UNIQUE INDEX idx_users_name ON users (name);
		`)
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			CREATE INDEX idx_users_name ON users (name);
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("IndexWithPrefixLengthAndDescendingKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE articles (id int NOT NULL, title text, score int, PRIMARY KEY (id));
			CREATE INDEX idx_articles ON articles (title(10), score DESC);
		`)
		driver.ExecOnSource("CREATE TABLE articles (id int NOT NULL, title text, score int, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLCreateIndexInstruction{
				Name:      "idx_articles",
				TableName: "articles",
				Keys:      []string{"`title`(10)", "`score` DESC"},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateTablesWithForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id) ON DELETE CASCADE
			);
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("AddForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)
		driver.ExecOnSource(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, author_id int, PRIMARY KEY (id));
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("AddForeignKeyToANewTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE publishers (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, publisher_id int, PRIMARY KEY (id));
			ALTER TABLE books ADD CONSTRAINT fk_books_publisher
				FOREIGN KEY (publisher_id) REFERENCES publishers (id);
		`)
		driver.ExecOnSource("CREATE TABLE books (id int NOT NULL, publisher_id int, PRIMARY KEY (id));")

		// The name order puts the change of the kept table before the creation of the
		// referenced table, so the enforcement of the foreign keys goes off.
		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("RemoveForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, author_id int, PRIMARY KEY (id));
		`)
		driver.ExecOnSource(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("DropTableWithForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnSource(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLSetForeignKeyChecksInstruction{},
			&MySQLDropTableInstruction{
				Name: "books",
			},
			&MySQLSetForeignKeyChecksInstruction{
				Enabled: true,
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("AddCheckConstraint", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id int NOT NULL,
				age int,
				PRIMARY KEY (id),
				CONSTRAINT chk_users_age CHECK (age > 0)
			);
		`)
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("RemoveCheckConstraint", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")
		driver.ExecOnSource(`
			CREATE TABLE users (
				id int NOT NULL,
				age int,
				PRIMARY KEY (id),
				CONSTRAINT chk_users_age CHECK (age > 0)
			);
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLDropCheckConstraintAction{
					Name: "chk_users_age",
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ChangePrimaryKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE members (id int NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id, tenant_id));")
		driver.ExecOnSource("CREATE TABLE members (id int NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("RemoveAutoIncrementAndPrimaryKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE logs (id int NOT NULL, message text);")
		driver.ExecOnSource("CREATE TABLE logs (id int NOT NULL AUTO_INCREMENT, message text, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("GeneratedColumns", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE prices (
				id int NOT NULL,
				net int,
				gross int GENERATED ALWAYS AS (net * 2) STORED,
				doubled int GENERATED ALWAYS AS (net + net) VIRTUAL,
				PRIMARY KEY (id)
			);
		`)
		driver.ExecOnSource("CREATE TABLE prices (id int NOT NULL, net int, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ChangeColumnToGenerated", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE prices (
				id int NOT NULL,
				net int,
				gross int GENERATED ALWAYS AS (net * 2) STORED,
				PRIMARY KEY (id)
			);
		`)
		driver.ExecOnSource("CREATE TABLE prices (id int NOT NULL, net int, gross int, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateView", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
		`)
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLCreateViewInstruction{
				Name:       "adult_users",
				Definition: "select `users`.`id` AS `id` from `users` where (`users`.`age` >= 18)",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ChangeView", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 21;
		`)
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLCreateViewInstruction{
				Name:       "adult_users",
				Definition: "select `users`.`id` AS `id` from `users` where (`users`.`age` >= 21)",
				OrReplace:  true,
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("DropView", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLDropViewInstruction{
				Name: "adult_users",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateTableWithEngineAndCollation", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE logs (id int NOT NULL, PRIMARY KEY (id)) ENGINE = MyISAM DEFAULT COLLATE = utf8mb4_bin;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ChangeEngine", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE logs (id int NOT NULL, PRIMARY KEY (id)) ENGINE = MyISAM;")
		driver.ExecOnSource("CREATE TABLE logs (id int NOT NULL, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "logs",
				Action: &MySQLEngineAction{
					Engine: "MyISAM",
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ChangeTableCollation", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE messages (id int NOT NULL, body varchar(100), PRIMARY KEY (id)) DEFAULT COLLATE = utf8mb4_bin;")
		driver.ExecOnSource("CREATE TABLE messages (id int NOT NULL, body varchar(100), PRIMARY KEY (id));")
		driver.ExecOnSource("INSERT INTO messages (id, body) VALUES (1, 'hello');")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "messages",
				Action: &MySQLConvertToCharacterSetAction{
					CharacterSet: "utf8mb4",
					Collation:    "utf8mb4_bin",
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)

		require.Equal(t, []map[string]any{
			{"id": int64(1), "body": "hello"},
		}, driver.FetchAllFromSource("messages", ""))
	})

	t.Run("ChangeTableCollationWithAColumnCollation", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE messages (id int NOT NULL, body varchar(100) COLLATE utf8mb4_general_ci, PRIMARY KEY (id)) DEFAULT COLLATE = utf8mb4_bin;")
		driver.ExecOnSource("CREATE TABLE messages (id int NOT NULL, body varchar(100) COLLATE utf8mb4_general_ci, PRIMARY KEY (id));")

		// The conversion rewrites the collation of the body column, so the column needs
		// its definition again in the same run.
		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CreatePartitionedTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 4;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("AddPartitioning", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY KEY (id) PARTITIONS 2;")
		driver.ExecOnSource("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnSource("INSERT INTO metrics (id) VALUES (1), (2);")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "metrics",
				Action: &MySQLPartitionAction{
					Clause: "PARTITION BY KEY (id)\nPARTITIONS 2",
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)

		require.Equal(t, []map[string]any{
			{"id": int64(1)},
			{"id": int64(2)},
		}, driver.FetchAllFromSource("metrics", "ORDER BY id"))
	})

	t.Run("RemovePartitioning", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnSource("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 4;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name:   "metrics",
				Action: &MySQLRemovePartitioningAction{},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateTrigger", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, updated_count int DEFAULT 0, PRIMARY KEY (id));
			CREATE TRIGGER trg_users_update BEFORE UPDATE ON users FOR EACH ROW
			BEGIN
				SET NEW.updated_count = OLD.updated_count + 1;
			END;
		`)
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, updated_count int DEFAULT 0, PRIMARY KEY (id));")
		driver.ExecOnSource("INSERT INTO users (id) VALUES (1);")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLCreateTriggerInstruction{
				Name:      "trg_users_update",
				Timing:    "BEFORE",
				Event:     "UPDATE",
				TableName: "users",
				Statement: "BEGIN\n\t\t\t\tSET NEW.updated_count = OLD.updated_count + 1;\n\t\t\tEND",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)

		driver.ExecOnSource("UPDATE users SET id = 1 WHERE id = 1;")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "updated_count": int64(1)},
		}, driver.FetchAllFromSource("users", ""))
	})

	t.Run("ChangeTrigger", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_users_insert BEFORE INSERT ON users FOR EACH ROW SET NEW.note = 'new';
		`)
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_users_insert BEFORE INSERT ON users FOR EACH ROW SET NEW.note = 'old';
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("DropTrigger", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TRIGGER trg_users_delete AFTER DELETE ON users FOR EACH ROW SET @deleted = 1;
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLDropTriggerInstruction{
				Name: "trg_users_delete",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateRoutines", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE PROCEDURE prune_users(IN max_age int) DELETE FROM users WHERE age > max_age;
			CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x * 2;
		`)
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ChangeFunction", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x + x;")
		driver.ExecOnSource("CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x * 2;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("DropProcedure", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnSource("CREATE PROCEDURE noop() SET @noop = 1;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLDropRoutineInstruction{
				Type: "PROCEDURE",
				Name: "noop",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ViewThatCallsAFunction", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x * 2;
			CREATE VIEW doubled_users AS SELECT double_it(id) AS doubled FROM users;
		`)
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateAndDropEvent", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")
		driver.ExecOnSource("CREATE EVENT old_nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLCreateEventInstruction{
				Name:       "nightly",
				Definition: "CREATE EVENT `nightly` ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' ON COMPLETION NOT PRESERVE ENABLE DO SET @cleaned = 1",
			},
			&MySQLDropEventInstruction{
				Name: "old_nightly",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ChangeEvent", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE EVENT nightly ON SCHEDULE EVERY 2 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")
		driver.ExecOnSource("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLDropEventInstruction{
				Name: "nightly",
			},
			&MySQLCreateEventInstruction{
				Name:       "nightly",
				Definition: "CREATE EVENT `nightly` ON SCHEDULE EVERY 2 DAY STARTS '2030-01-01 00:00:00' ON COMPLETION NOT PRESERVE ENABLE DO SET @cleaned = 1",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("EventsWithTwoStartTimesCompareAsEqual", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")
		driver.ExecOnSource("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2031-06-15 12:00:00' DO SET @cleaned = 1;")

		driver.RequireInstructions(nil)
	})

	t.Run("RevokeGrantOption", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE posts (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT ON users TO 'dbdiff_reader'@'%' WITH GRANT OPTION;
			GRANT SELECT ON posts TO 'dbdiff_reader'@'%' WITH GRANT OPTION;
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("SQLSourceAsTarget", func(t *testing.T) {
		skipWithoutMySQLServer(t)

		directory := t.TempDir()
		sqltest.WriteSQLFile(t, directory, "001_users.sql",
			"CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")
		sqltest.WriteSQLFile(t, directory, "002_add_name.sql",
			"ALTER TABLE users ADD COLUMN name varchar(100);")
		sqltest.WriteSQLFile(t, directory, "002_add_name.down.sql",
			"ALTER TABLE users DROP COLUMN name;")

		driver := NewTestMySQLDriverWithTargetDirectory(t, directory)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("TwoSQLSourcesGiveAnError", func(t *testing.T) {
		driver := NewMySQLDriver(&MySQLDriverConfig{})

		target := driversshared.ParseDataSource(t.TempDir())
		source := driversshared.ParseDataSource(t.TempDir())

		_, err := driver.Diff(t.Context(), source, target, driversshared.DiffOptions{})

		require.Error(t, err)
		require.Contains(t, err.Error(), "Give a database")
	})

	t.Run("DataComparison", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), age int, PRIMARY KEY (id));
			INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (3, 'Cara', NULL);
		`)
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), age int, PRIMARY KEY (id));
			INSERT INTO users (id, name, age) VALUES (1, 'Alicia', 30), (2, 'Bob', 40);
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "age": int64(30)},
			{"id": int64(3), "name": "Cara", "age": nil},
		}, driver.FetchAllFromSource("users", "ORDER BY id"))
	})

	t.Run("DataOfANewTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		driver.ExecOnTarget(`
			CREATE TABLE readings (
				id int NOT NULL,
				taken_at datetime,
				payload varbinary(4),
				price decimal(5,2),
				PRIMARY KEY (id)
			);
			INSERT INTO readings VALUES (1, '2030-01-02 03:04:05', X'DEADBEEF', 12.50);
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("DataOfATableWithNoPrimaryKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		driver.ExecOnTarget("CREATE TABLE notes (body text);")
		driver.ExecOnSource("CREATE TABLE notes (body text);")
		driver.ExecOnTarget("INSERT INTO notes VALUES ('hello');")

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

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT, UPDATE ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLGrantInstruction{
				Privileges: []string{"SELECT", "UPDATE"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("RevokeAndNarrowPrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE posts (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE posts (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT, DELETE ON users TO 'dbdiff_reader'@'%';
			GRANT SELECT ON posts TO 'dbdiff_reader'@'%';
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("GrantColumnPrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));
			GRANT SELECT (id, name) ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnSource(
			"CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLGrantInstruction{
				Privileges: []string{"SELECT"},
				Columns:    []string{"id", "name"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("NarrowColumnPrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));
			GRANT SELECT (id, email) ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));
			GRANT SELECT (id, name) ON users TO 'dbdiff_reader'@'%';
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("RevokeColumnPrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnTarget(
			"CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));")
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));
			GRANT SELECT (id, name) ON users TO 'dbdiff_reader'@'%';
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLRevokeInstruction{
				Privileges: []string{"SELECT"},
				Columns:    []string{"id", "name"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("DatabasePrivilegesAndGrantOption", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnTarget("GRANT SELECT ON * TO 'dbdiff_reader'@'%' WITH GRANT OPTION;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLGrantInstruction{
				Privileges:      []string{"SELECT"},
				Grantee:         "'dbdiff_reader'@'%'",
				WithGrantOption: true,
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("MigrationHistoryTableStaysHidden", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnSource("CREATE TABLE dbdiff_migrations (version varchar(255) NOT NULL, PRIMARY KEY (version));")

		driver.RequireInstructions(nil)
	})

	t.Run("ColumnComment", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget("CREATE TABLE users (id int NOT NULL COMMENT 'the identifier', PRIMARY KEY (id));")
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("IgnoredTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, email varchar(255), PRIMARY KEY (id));
			CREATE TABLE ignored_created (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_changed (id int NOT NULL, name varchar(255), PRIMARY KEY (id));
		`)
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_dropped (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_changed (id int NOT NULL, description varchar(255), PRIMARY KEY (id));
		`)

		driver.IgnoreTables = []string{"ignored_created", "ignored_dropped", "ignored_changed"}

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})
}

// MariaDB reads the same driver code, and its catalog reports the types, the defaults,
// the check clauses, and the view definitions with another text.
func TestMariaDBDriver(t *testing.T) {
	t.Run("CreateTableWithDefaults", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id int NOT NULL AUTO_INCREMENT,
				name varchar(100) NOT NULL DEFAULT 'anonymous',
				age int DEFAULT 18,
				created_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id)
			);
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CheckConstraint", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id int NOT NULL,
				age int,
				PRIMARY KEY (id),
				CONSTRAINT chk_users_age CHECK (age > 0)
			);
		`)
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("GeneratedColumn", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE prices (
				id int NOT NULL,
				net int,
				gross int GENERATED ALWAYS AS (net * 2) STORED,
				PRIMARY KEY (id)
			);
		`)
		driver.ExecOnSource("CREATE TABLE prices (id int NOT NULL, net int, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ForeignKey", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)
		driver.ExecOnSource(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, author_id int, PRIMARY KEY (id));
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("Sequences", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget("CREATE SEQUENCE order_numbers START WITH 1000 INCREMENT BY 10;")
		driver.ExecOnSource("CREATE SEQUENCE old_numbers;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ChangeSequence", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget("CREATE SEQUENCE order_numbers INCREMENT BY 5;")
		driver.ExecOnSource("CREATE SEQUENCE order_numbers INCREMENT BY 1;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ChangeSequenceOptions", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget("CREATE SEQUENCE order_numbers MINVALUE 5 MAXVALUE 500 CACHE 20 CYCLE;")
		driver.ExecOnSource("CREATE SEQUENCE order_numbers MINVALUE 5 MAXVALUE 500 CACHE 10 NOCYCLE;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("Trigger", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_users_insert BEFORE INSERT ON users FOR EACH ROW SET NEW.note = 'new';
		`)
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, note varchar(20), PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLCreateTriggerInstruction{
				Name:      "trg_users_insert",
				Timing:    "BEFORE",
				Event:     "INSERT",
				TableName: "users",
				Statement: "SET NEW.note = 'new'",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("Function", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget("CREATE FUNCTION double_it(x int) RETURNS int DETERMINISTIC RETURN x * 2;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLCreateRoutineInstruction{
				Type:       "FUNCTION",
				Name:       "double_it",
				Definition: "CREATE FUNCTION `double_it`(x int) RETURNS int(11)\n    DETERMINISTIC\nRETURN x * 2",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("Event", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLCreateEventInstruction{
				Name:       "nightly",
				Definition: "CREATE EVENT `nightly` ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' ON COMPLETION NOT PRESERVE ENABLE DO SET @cleaned = 1",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("PartitionedTable", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 4;")

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("DataComparison", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)
		driver.CompareData = true

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			INSERT INTO users (id, name) VALUES (1, 'Alice');
		`)
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			INSERT INTO users (id, name) VALUES (1, 'Alicia'), (2, 'Bob');
		`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("View", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
			CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
		`)
		driver.ExecOnSource("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&MySQLCreateViewInstruction{
				Name:       "adult_users",
				Definition: "select `users`.`id` AS `id` from `users` where `users`.`age` >= 18",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("IgnoredTable", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id int NOT NULL, email varchar(255), PRIMARY KEY (id));
			CREATE TABLE ignored_created (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_changed (id int NOT NULL, name varchar(255), PRIMARY KEY (id));
		`)
		driver.ExecOnSource(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_dropped (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_changed (id int NOT NULL, description varchar(255), PRIMARY KEY (id));
		`)

		driver.IgnoreTables = []string{"ignored_created", "ignored_dropped", "ignored_changed"}

		diff := driver.RequireInstructions([]driversshared.Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
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
		require.Equal(t, map[string]string{"tls": "skip-verify"}, config.Params)
		require.True(t, config.MultiStatements)
		require.True(t, config.ParseTime)
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
