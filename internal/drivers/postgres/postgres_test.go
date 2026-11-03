package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

const postgresTestConnectionString = "postgres://user:password@localhost:5432/dbdiff?sslmode=disable"

// The tests of the PostgreSQL driver need a server on the port 5432, and a runner of macOS
// or of Windows starts no service container. The variable stays empty on a runner of Linux,
// because a silent skip hides a server that fails there.
const skipPostgresServerVariable = "DBDIFF_TEST_SKIP_POSTGRES"

func skipWithoutPostgresServer(tb testing.TB) {
	tb.Helper()

	if os.Getenv(skipPostgresServerVariable) != "" {
		tb.Skipf("%s stops the tests that need a PostgreSQL server", skipPostgresServerVariable)
	}
}

type TestingPostgresDriver struct {
	*PostgresDriver

	tb   testing.TB
	conn *sql.DB

	source driversshared.DataSource
	target driversshared.DataSource

	targetSchema string
	sourceSchema string

	CompareData bool

	sourceConnection *sql.DB
	targetConnection *sql.DB
}

func NewTestPostgresDriver(tb testing.TB) *TestingPostgresDriver {
	tb.Helper()

	connectionString := postgresTestConnectionString
	conn, err := sql.Open("pgx", connectionString)
	require.NoError(tb, err)

	err = conn.PingContext(tb.Context())
	require.NoError(tb, err)

	id := time.Now().UnixNano()
	targetSchema := fmt.Sprintf("target_%d", id)
	sourceSchema := fmt.Sprintf("source_%d", id)

	// CREATE ROLE fails when the role exists already, so the DO block tests it first.
	_, err = conn.ExecContext(tb.Context(), `
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbdiff_reader') THEN
				CREATE ROLE dbdiff_reader;
			END IF;
		END
		$$;
	`)
	require.NoError(tb, err)

	_, err = conn.ExecContext(tb.Context(), fmt.Sprintf("CREATE SCHEMA %s", targetSchema))
	require.NoError(tb, err)
	_, err = conn.ExecContext(tb.Context(), fmt.Sprintf("CREATE SCHEMA %s", sourceSchema))
	require.NoError(tb, err)

	// The connection stays open for this cleanup. A closed connection drops no schema.
	tb.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", targetSchema))
		require.NoError(tb, err)

		_, err = conn.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", sourceSchema))
		require.NoError(tb, err)

		require.NoError(tb, conn.Close())
	})

	targetConnectionString := fmt.Sprintf("%s&search_path=%s", connectionString, targetSchema)
	sourceConnectionString := fmt.Sprintf("%s&search_path=%s", connectionString, sourceSchema)

	// The harness diffs a call at a time, and the driver fields go nil between two calls, so
	// the harness keeps its own connection of each side for ExecOnTarget and ExecOnSource.
	targetConnection, err := OpenPostgresConnection(targetConnectionString, "")
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, targetConnection.Close())
	})

	sourceConnection, err := OpenPostgresConnection(sourceConnectionString, "")
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, sourceConnection.Close())
	})

	driver := NewPostgresDriver(&PostgresDriverConfig{})

	return &TestingPostgresDriver{
		PostgresDriver:   driver,
		tb:               tb,
		conn:             conn,
		source:           driversshared.ConnectionStringDataSource{ConnectionString: sourceConnectionString},
		target:           driversshared.ConnectionStringDataSource{ConnectionString: targetConnectionString},
		targetSchema:     targetSchema,
		sourceSchema:     sourceSchema,
		sourceConnection: sourceConnection,
		targetConnection: targetConnection,
	}
}

// PostgreSQL holds one extension one time per database, so a test of an extension needs
// this harness instead of NewTestPostgresDriver.
func NewTestPostgresDriverWithTwoDatabases(tb testing.TB) *TestingPostgresDriver {
	tb.Helper()

	adminDSN := "postgres://user:password@localhost:5432/dbdiff?sslmode=disable"
	adminConn, err := sql.Open("pgx", adminDSN)
	require.NoError(tb, err)

	err = adminConn.PingContext(tb.Context())
	require.NoError(tb, err)

	// A CREATE DATABASE statement needs a connection outside a transaction.
	id := time.Now().UnixNano()
	targetDatabase := fmt.Sprintf("dbdiff_target_%d", id)
	sourceDatabase := fmt.Sprintf("dbdiff_source_%d", id)

	_, err = adminConn.ExecContext(tb.Context(), fmt.Sprintf("CREATE DATABASE %s", targetDatabase))
	require.NoError(tb, err)
	_, err = adminConn.ExecContext(tb.Context(), fmt.Sprintf("CREATE DATABASE %s", sourceDatabase))
	require.NoError(tb, err)

	// The admin connection stays open for this cleanup.
	tb.Cleanup(func() {
		_, err := adminConn.ExecContext(context.Background(), fmt.Sprintf("DROP DATABASE %s", targetDatabase))
		require.NoError(tb, err)

		_, err = adminConn.ExecContext(context.Background(), fmt.Sprintf("DROP DATABASE %s", sourceDatabase))
		require.NoError(tb, err)

		require.NoError(tb, adminConn.Close())
	})

	targetConnectionString := fmt.Sprintf("postgres://user:password@localhost:5432/%s?sslmode=disable", targetDatabase)
	sourceConnectionString := fmt.Sprintf("postgres://user:password@localhost:5432/%s?sslmode=disable", sourceDatabase)

	targetConnection, err := OpenPostgresConnection(targetConnectionString, "")
	require.NoError(tb, err)

	sourceConnection, err := OpenPostgresConnection(sourceConnectionString, "")
	require.NoError(tb, err)

	// Go calls a cleanup in the reverse order of its registration, so this one closes every
	// connection before the database drop above. A drop fails while a connection uses it.
	tb.Cleanup(func() {
		require.NoError(tb, targetConnection.Close())
		require.NoError(tb, sourceConnection.Close())
	})

	driver := NewPostgresDriver(&PostgresDriverConfig{})

	return &TestingPostgresDriver{
		PostgresDriver:   driver,
		tb:               tb,
		conn:             adminConn,
		source:           driversshared.ConnectionStringDataSource{ConnectionString: sourceConnectionString},
		target:           driversshared.ConnectionStringDataSource{ConnectionString: targetConnectionString},
		targetSchema:     "public",
		sourceSchema:     "public",
		sourceConnection: sourceConnection,
		targetConnection: targetConnection,
	}
}

func NewTestPostgresDriverWithPrivileges(tb testing.TB) *TestingPostgresDriver {
	tb.Helper()

	driver := NewTestPostgresDriver(tb)
	driver.ComparePrivileges = true

	return driver
}

func (d *TestingPostgresDriver) ExecOnTarget(sqlStatements string) {
	d.tb.Helper()
	_, err := d.targetConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingPostgresDriver) ExecOnSource(sqlStatements string) {
	d.tb.Helper()
	_, err := d.sourceConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingPostgresDriver) RequireInstructions(expected driversshared.Instructions) string {
	d.tb.Helper()

	instructions, err := d.Diff(d.tb.Context(), d.source, d.target, driversshared.DiffOptions{CompareData: d.CompareData})
	require.NoError(d.tb, err)
	require.Equal(d.tb, expected, instructions)

	return instructions.String()
}

func (d *TestingPostgresDriver) RequireDiffApplies(expected driversshared.Instructions) {
	d.tb.Helper()

	d.ExecOnSource(d.RequireInstructions(expected))
	d.RequireInstructions(nil)
}

func (d *TestingPostgresDriver) FetchAllFromSource(table string, additionalRules string) []map[string]any {
	d.tb.Helper()

	rows, err := d.sourceConnection.Query(fmt.Sprintf("SELECT * FROM %s %s;", driversshared.QuoteIdentifier(table), additionalRules))
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
			row[column] = columnValues[i]
		}

		results = append(results, row)
	}

	require.NoError(d.tb, rows.Err())

	return results
}

// A test that diffs two SQL sources builds no live connection of its own, because the
// scratch server that materializes them stops as soon as Diff returns. This helper gives
// such a test a schema on the already-running test server, applies the SQL source to it,
// and returns a connection that stays open so the test can prove that the generated SQL of
// the diff runs.
func openVerificationConnection(tb testing.TB, sqlSourcePath string) *sql.DB {
	tb.Helper()

	adminConnection, err := sql.Open("pgx", postgresTestConnectionString)
	require.NoError(tb, err)

	schemaName := fmt.Sprintf("verify_%d", time.Now().UnixNano())

	_, err = adminConnection.ExecContext(tb.Context(), fmt.Sprintf("CREATE SCHEMA %s", schemaName))
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_, err := adminConnection.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
		require.NoError(tb, err)

		require.NoError(tb, adminConnection.Close())
	})

	connectionString := fmt.Sprintf("%s&search_path=%s", postgresTestConnectionString, schemaName)

	connection, err := OpenPostgresConnection(connectionString, "")
	require.NoError(tb, err)

	tb.Cleanup(func() {
		require.NoError(tb, connection.Close())
	})

	sqlSource, err := driversshared.NewSQLSource(sqlSourcePath)
	require.NoError(tb, err)

	require.NoError(tb, sqlSource.ApplyTo(tb.Context(), connection))

	return connection
}

func TestPostgresDriver(t *testing.T) {
	skipWithoutPostgresServer(t)

	t.Run("CreateTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE simple (id INT, name TEXT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "simple",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
					{
						Name: "name",
						Type: "text",
					},
				},
			},
		})
	})

	t.Run("DropTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "users"},
		})
	})

	t.Run("AddColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "name",
							Type: "text",
						},
					},
				},
			},
		})
	})

	t.Run("DropColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "name"},
				},
			},
		})
	})

	t.Run("AlterColumnType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name VARCHAR(50));`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "name",
						DataType:   "text",
					},
				},
			},
		})
	})

	t.Run("AlterColumnTypeWithAutomaticCast", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, score INT);`)
		driver.ExecOnSource(`INSERT INTO users (id, score) VALUES (1, 42);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, score BIGINT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "score",
						DataType:   "bigint",
					},
				},
			},
		})

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "score": int64(42)},
		}, rows)
	})

	t.Run("AlterColumnTypeWithoutAutomaticCast", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, score TEXT);`)
		driver.ExecOnSource(`INSERT INTO users (id, score) VALUES (1, '42');`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, score INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "score",
						DataType:   "integer",
						UsingCast:  true,
					},
				},
			},
		})

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "score": int64(42)},
		}, rows)
	})

	t.Run("CreateTableWithArrayColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE tags (id INT, labels TEXT[]);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "tags",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
					{
						Name: "labels",
						Type: "text[]",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithEnumColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TYPE mood AS ENUM ('sad', 'ok');
			CREATE TABLE users (id INT, mood mood);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Labels: []string{"sad", "ok"},
			},
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
					{
						Name: "mood",
						Type: "mood",
					},
				},
			},
		})
	})

	t.Run("AlterColumnTypeToArray", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, tags INT[]);`)
		driver.ExecOnSource(`INSERT INTO users (id, tags) VALUES (1, ARRAY[5, 6]);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, tags BIGINT[]);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "tags",
						DataType:   "bigint[]",
						UsingCast:  true,
					},
				},
			},
		})

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "tags": "{5,6}"},
		}, rows)
	})

	t.Run("CreateTableWithIdentityColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY, name TEXT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:     "id",
						Type:     "integer",
						NotNull:  true,
						Identity: "ALWAYS",
					},
					{
						Name: "name",
						Type: "text",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithSeveralConstraints", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE items (
				id INT PRIMARY KEY,
				code TEXT,
				price INT,
				CONSTRAINT z_price_is_positive CHECK (price > 0),
				CONSTRAINT a_code_is_unique UNIQUE (code)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "items",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
					{
						Name: "code",
						Type: "text",
					},
					{
						Name: "price",
						Type: "integer",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "a_code_is_unique",
						Type: "u",
						Def:  "UNIQUE (code)",
					},
					{
						Name: "items_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
					{
						Name: "z_price_is_positive",
						Type: "c",
						Def:  "CHECK ((price > 0))",
					},
				},
			},
		})
	})

	t.Run("CreateAPartitionWithItsOwnObjects", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE measurements (id INT PRIMARY KEY) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (100);
			COMMENT ON TABLE measurements_low IS 'low range';
			CREATE FUNCTION touch() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
			CREATE TRIGGER trg BEFORE INSERT ON measurements_low FOR EACH ROW EXECUTE FUNCTION touch();
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION touch()\n RETURNS trigger\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN NEW; END; $function$",
			},
			&PostgresCreateTableInstruction{
				Name: "measurements",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "measurements_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
				PartitionKey: "RANGE (id)",
			},
			&PostgresCreateTablePartitionInstruction{
				Name:       "measurements_low",
				ParentName: "measurements",
				Bound:      "FOR VALUES FROM (0) TO (100)",
			},
			&PostgresCommentOnTableInstruction{
				Name: "measurements_low",
				Text: "low range",
			},
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER trg BEFORE INSERT ON measurements_low FOR EACH ROW EXECUTE FUNCTION touch()",
			},
		})
	})

	t.Run("CreatePartitionedTableWithAForeignKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE zoo (id INT PRIMARY KEY);
			CREATE TABLE animal (id INT, zoo_id INT REFERENCES zoo(id)) PARTITION BY RANGE (id);
			CREATE TABLE animal_low PARTITION OF animal FOR VALUES FROM (0) TO (100);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "zoo",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "zoo_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&PostgresCreateTableInstruction{
				Name: "animal",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
					{
						Name: "zoo_id",
						Type: "integer",
					},
				},
				PartitionKey: "RANGE (id)",
			},
			&PostgresCreateTablePartitionInstruction{
				Name:       "animal_low",
				ParentName: "animal",
				Bound:      "FOR VALUES FROM (0) TO (100)",
			},
			&PostgresAlterTableInstruction{
				Name: "animal",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "animal_zoo_id_fkey",
							Type: "f",
							Def:  "FOREIGN KEY (zoo_id) REFERENCES zoo(id)",
						},
					},
				},
			},
		})
	})

	t.Run("CreateTablesWithAForeignKeyCycle", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT PRIMARY KEY, org_id INT);
			CREATE TABLE orgs (id INT PRIMARY KEY, owner_id INT REFERENCES users(id));
			ALTER TABLE users ADD CONSTRAINT users_org_id_fkey FOREIGN KEY (org_id) REFERENCES orgs(id);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
					{
						Name: "org_id",
						Type: "integer",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "users_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&PostgresCreateTableInstruction{
				Name: "orgs",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
					{
						Name: "owner_id",
						Type: "integer",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "orgs_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "users_org_id_fkey",
							Type: "f",
							Def:  "FOREIGN KEY (org_id) REFERENCES orgs(id)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "orgs",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "orgs_owner_id_fkey",
							Type: "f",
							Def:  "FOREIGN KEY (owner_id) REFERENCES users(id)",
						},
					},
				},
			},
		})
	})

	t.Run("CreateTablesInForeignKeyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE zoo (id INT PRIMARY KEY);
			CREATE TABLE animal (id INT PRIMARY KEY, zoo_id INT REFERENCES zoo(id));
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "zoo",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "zoo_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&PostgresCreateTableInstruction{
				Name: "animal",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
					{
						Name: "zoo_id",
						Type: "integer",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "animal_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "animal",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "animal_zoo_id_fkey",
							Type: "f",
							Def:  "FOREIGN KEY (zoo_id) REFERENCES zoo(id)",
						},
					},
				},
			},
		})
	})

	t.Run("DropTablesInForeignKeyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE animal (id INT PRIMARY KEY);
			CREATE TABLE zoo (id INT PRIMARY KEY, animal_id INT REFERENCES animal(id));
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "zoo"},
			&driversshared.SQLDropTableInstruction{Name: "animal"},
		})
	})

	t.Run("AddForeignKeyToANewTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE animal (id INT PRIMARY KEY, zoo_id INT);`)
		driver.ExecOnTarget(`
			CREATE TABLE zoo (id INT PRIMARY KEY);
			CREATE TABLE animal (id INT PRIMARY KEY, zoo_id INT REFERENCES zoo(id));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "zoo",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "zoo_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "animal",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "animal_zoo_id_fkey",
							Type: "f",
							Def:  "FOREIGN KEY (zoo_id) REFERENCES zoo(id)",
						},
					},
				},
			},
		})
	})

	t.Run("CreateTableWithSerialColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:               "id",
						Type:               "integer",
						NotNull:            true,
						Serial:             "serial",
						SerialSequenceName: "users_id_seq",
					},
					{
						Name: "name",
						Type: "text",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "users_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
		})

		driver.ExecOnSource(`INSERT INTO users (name) VALUES ('alice');`)

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "alice"},
		}, rows)
	})

	t.Run("AddSerialColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (name TEXT, id BIGSERIAL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:               "id",
							Type:               "bigint",
							NotNull:            true,
							Serial:             "bigserial",
							SerialSequenceName: "users_id_seq",
						},
					},
				},
			},
		})
	})

	t.Run("EqualSerialColumns", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id SERIAL PRIMARY KEY);`)
		driver.ExecOnTarget(`CREATE TABLE users (id SERIAL PRIMARY KEY);`)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateTableWithGeneratedColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 2) STORED);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "measures",
				Columns: []*PostgresColumn{
					{
						Name: "value",
						Type: "integer",
					},
					{
						Name:                "doubled",
						Type:                "integer",
						GeneratedExpression: "(value * 2)",
					},
				},
			},
		})
	})

	t.Run("AddIdentityToColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT GENERATED BY DEFAULT AS IDENTITY);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "id"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddIdentityAction{
						ColumnName: "id",
						Identity:   "BY DEFAULT",
					},
				},
			},
		})
	})

	t.Run("ChangeIdentityGeneration", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT GENERATED BY DEFAULT AS IDENTITY);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetIdentityAction{
						ColumnName: "id",
						Identity:   "BY DEFAULT",
					},
				},
			},
		})
	})

	t.Run("DropIdentityFromColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropIdentityAction{ColumnName: "id"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropNotNullAction{ColumnName: "id"},
				},
			},
		})
	})

	t.Run("ModifyGeneratedExpression", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 2) STORED);`)
		driver.ExecOnSource(`INSERT INTO measures (value) VALUES (5);`)
		driver.ExecOnTarget(`CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 3) STORED);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "doubled"},
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:                "doubled",
							Type:                "integer",
							GeneratedExpression: "(value * 3)",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromSource("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":   int64(5),
				"doubled": int64(15),
			},
		}, rows)
	})

	t.Run("CreatePartitionedTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE events (id BIGINT, created DATE NOT NULL, label TEXT)
				PARTITION BY RANGE (created);
			CREATE TABLE events_2024 PARTITION OF events
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
			CREATE TABLE events_default PARTITION OF events DEFAULT;
			CREATE INDEX events_label ON events (label);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "events",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "bigint",
					},
					{
						Name:    "created",
						Type:    "date",
						NotNull: true,
					},
					{
						Name: "label",
						Type: "text",
					},
				},
				PartitionKey: "RANGE (created)",
			},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE INDEX events_label ON events USING btree (label)",
			},
			&PostgresCreateTablePartitionInstruction{
				Name:       "events_2024",
				ParentName: "events",
				Bound:      "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
			},
			&PostgresCreateTablePartitionInstruction{
				Name:       "events_default",
				ParentName: "events",
				Bound:      "DEFAULT",
			},
		})
	})

	t.Run("DropPartitionedTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE events (id BIGINT, created DATE NOT NULL)
				PARTITION BY RANGE (created);
			CREATE TABLE events_2024 PARTITION OF events
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "events"},
		})
	})

	t.Run("AddPartitionToExistingTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE events (id BIGINT, created DATE NOT NULL)
				PARTITION BY RANGE (created);
			CREATE TABLE events_2024 PARTITION OF events
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE events (id BIGINT, created DATE NOT NULL)
				PARTITION BY RANGE (created);
			CREATE TABLE events_2024 PARTITION OF events
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
			CREATE TABLE events_2025 PARTITION OF events
				FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTablePartitionInstruction{
				Name:       "events_2025",
				ParentName: "events",
				Bound:      "FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
			},
		})
	})

	t.Run("CreateMaterializedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, active BOOLEAN);`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT, active BOOLEAN);
			CREATE MATERIALIZED VIEW active_users AS SELECT id FROM users WHERE active;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateMaterializedViewInstruction{
				Name:  "active_users",
				Query: " SELECT id\n   FROM users\n  WHERE active;",
			},
		})
	})

	t.Run("DropMaterializedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW all_users AS SELECT id FROM users;
		`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropMaterializedViewInstruction{Name: "all_users"},
		})
	})

	t.Run("ModifyMaterializedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT, active BOOLEAN);
			CREATE MATERIALIZED VIEW selected_users AS SELECT id FROM users;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT, active BOOLEAN);
			CREATE MATERIALIZED VIEW selected_users AS SELECT id FROM users WHERE active;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropMaterializedViewInstruction{Name: "selected_users"},
			&PostgresCreateMaterializedViewInstruction{
				Name:  "selected_users",
				Query: " SELECT id\n   FROM users\n  WHERE active;",
			},
		})
	})

	t.Run("CreateMaterializedViewsInDependencyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW first_view AS SELECT id FROM users;
			CREATE MATERIALIZED VIEW second_view AS SELECT id FROM first_view;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateMaterializedViewInstruction{
				Name:  "first_view",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCreateMaterializedViewInstruction{
				Name:  "second_view",
				Query: " SELECT id\n   FROM first_view;",
			},
		})
	})

	t.Run("CreateViewOfAMaterializedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW all_users AS SELECT id FROM users;
			CREATE VIEW user_identifiers AS SELECT id FROM all_users;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateMaterializedViewInstruction{
				Name:  "all_users",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCreateViewInstruction{
				Name:  "user_identifiers",
				Query: " SELECT id\n   FROM all_users;",
			},
		})
	})

	t.Run("DropViewOfAMaterializedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW all_users AS SELECT id FROM users;
			CREATE VIEW user_identifiers AS SELECT id FROM all_users;
		`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "user_identifiers"},
			&PostgresDropMaterializedViewInstruction{Name: "all_users"},
		})
	})

	t.Run("CreateMaterializedViewWithIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW all_users AS SELECT id FROM users;
			CREATE UNIQUE INDEX all_users_id ON all_users (id);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateMaterializedViewInstruction{
				Name:  "all_users",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE UNIQUE INDEX all_users_id ON all_users USING btree (id)",
			},
		})
	})

	t.Run("CreateTableWithColumnCollation", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE people (name TEXT COLLATE "C", other TEXT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "people",
				Columns: []*PostgresColumn{
					{
						Name:      "name",
						Type:      "text",
						Collation: "C",
					},
					{
						Name: "other",
						Type: "text",
					},
				},
			},
		})
	})

	t.Run("AlterColumnCollation", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE people (name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE people (name TEXT COLLATE "C");`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "people",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "name",
						DataType:   "text",
						Collation:  "C",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithComments", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			COMMENT ON TABLE users IS 'the people';
			COMMENT ON COLUMN users.id IS 'the key';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						Comment: "the key",
					},
				},
			},
			&PostgresCommentOnTableInstruction{
				Name: "users",
				Text: "the people",
			},
			&PostgresCommentOnColumnInstruction{
				TableName:  "users",
				ColumnName: "id",
				Text:       "the key",
			},
		})
	})

	t.Run("ModifyComments", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT, name TEXT);
			COMMENT ON TABLE users IS 'the old comment';
			COMMENT ON COLUMN users.id IS 'the old key';
			COMMENT ON COLUMN users.name IS 'this one goes away';
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT, name TEXT);
			COMMENT ON TABLE users IS 'the new comment';
			COMMENT ON COLUMN users.id IS 'the new key';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnTableInstruction{
				Name: "users",
				Text: "the new comment",
			},
			&PostgresCommentOnColumnInstruction{
				TableName:  "users",
				ColumnName: "id",
				Text:       "the new key",
			},
			&PostgresCommentOnColumnInstruction{
				TableName:  "users",
				ColumnName: "name",
			},
		})
	})

	t.Run("ModifyViewComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE users (id INT);
			CREATE VIEW all_users AS SELECT id FROM users;
		`
		driver.ExecOnSource(schema)
		driver.ExecOnSource(`COMMENT ON VIEW all_users IS 'the old comment';`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`COMMENT ON VIEW all_users IS 'the new comment';`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnViewInstruction{
				Name: "all_users",
				Text: "the new comment",
			},
		})
	})

	t.Run("CreateViewWithComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE VIEW all_users AS SELECT id FROM users;
			COMMENT ON VIEW all_users IS 'every person';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateViewInstruction{
				Name:  "all_users",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCommentOnViewInstruction{
				Name: "all_users",
				Text: "every person",
			},
		})
	})

	t.Run("ModifyMaterializedViewComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW all_users AS SELECT id FROM users;
		`
		driver.ExecOnSource(schema)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`COMMENT ON MATERIALIZED VIEW all_users IS 'every person';`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnMaterializedViewInstruction{
				Name: "all_users",
				Text: "every person",
			},
		})
	})

	t.Run("ModifyIndexComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE users (id INT, email TEXT);
			CREATE INDEX users_email ON users (email);
		`
		driver.ExecOnSource(schema)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`COMMENT ON INDEX users_email IS 'the lookup key';`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnIndexInstruction{
				Name: "users_email",
				Text: "the lookup key",
			},
		})
	})

	t.Run("ModifyFunctionComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `CREATE FUNCTION add_one(integer) RETURNS integer AS $$ SELECT $1 + 1; $$ LANGUAGE sql;`
		driver.ExecOnSource(schema)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`COMMENT ON FUNCTION add_one(integer) IS 'the next value';`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnRoutineInstruction{
				Name:      "add_one",
				Arguments: "integer",
				Text:      "the next value",
			},
		})
	})

	t.Run("ModifyTypeComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `CREATE TYPE mood AS ENUM ('sad', 'happy');`
		driver.ExecOnSource(schema)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`COMMENT ON TYPE mood IS 'the state of a person';`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnTypeInstruction{
				Name: "mood",
				Text: "the state of a person",
			},
		})
	})

	t.Run("CreateTableWithRowLevelSecurity", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE docs (id INT, secret BOOLEAN);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
			ALTER TABLE docs FORCE ROW LEVEL SECURITY;
			CREATE POLICY docs_read ON docs FOR SELECT USING (NOT secret);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "docs",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
					{
						Name: "secret",
						Type: "boolean",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresRowLevelSecurityAction{Mode: "ENABLE"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresRowLevelSecurityAction{Mode: "FORCE"},
				},
			},
			&PostgresCreatePolicyInstruction{
				Name:       "docs_read",
				TableName:  "docs",
				Permissive: "PERMISSIVE",
				Command:    "SELECT",
				Roles:      []string{"public"},
				Using:      "(NOT secret)",
			},
		})
	})

	t.Run("DisableRowLevelSecurity", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE docs (id INT);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
		`)
		driver.ExecOnTarget(`CREATE TABLE docs (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresRowLevelSecurityAction{Mode: "DISABLE"},
				},
			},
		})
	})

	t.Run("ModifyPolicy", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE docs (id INT, secret BOOLEAN);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
			CREATE POLICY docs_read ON docs FOR SELECT USING (secret);
			CREATE POLICY docs_old ON docs FOR DELETE USING (true);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE docs (id INT, secret BOOLEAN);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
			CREATE POLICY docs_read ON docs FOR SELECT USING (NOT secret);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropPolicyInstruction{
				Name:      "docs_read",
				TableName: "docs",
			},
			&PostgresCreatePolicyInstruction{
				Name:       "docs_read",
				TableName:  "docs",
				Permissive: "PERMISSIVE",
				Command:    "SELECT",
				Roles:      []string{"public"},
				Using:      "(NOT secret)",
			},
			&PostgresDropPolicyInstruction{
				Name:      "docs_old",
				TableName: "docs",
			},
		})
	})

	t.Run("CreatePartitionThatSortsBeforeItsParent", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE zebra (id BIGINT, created DATE NOT NULL)
				PARTITION BY RANGE (created);
			CREATE TABLE alpha PARTITION OF zebra
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "zebra",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "bigint",
					},
					{
						Name:    "created",
						Type:    "date",
						NotNull: true,
					},
				},
				PartitionKey: "RANGE (created)",
			},
			&PostgresCreateTablePartitionInstruction{
				Name:       "alpha",
				ParentName: "zebra",
				Bound:      "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
			},
		})
	})

	t.Run("CreateTableThatInheritsAnotherTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE zparent (a INT);
			CREATE TABLE achild (b INT) INHERITS (zparent);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "zparent",
				Columns: []*PostgresColumn{
					{
						Name: "a",
						Type: "integer",
					},
				},
			},
			&PostgresCreateTableInstruction{
				Name: "achild",
				Columns: []*PostgresColumn{
					{
						Name: "a",
						Type: "integer",
					},
					{
						Name: "b",
						Type: "integer",
					},
				},
				Inherits: []string{"zparent"},
			},
		})
	})

	t.Run("CreateTableWithIdentityOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:            "id",
						Type:            "integer",
						NotNull:         true,
						Identity:        "ALWAYS",
						IdentityOptions: "START WITH 100 INCREMENT BY 5",
					},
				},
			},
		})
	})

	t.Run("AlterTwoIdentityOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetIdentityOptionsAction{
						ColumnName: "id",
						Options:    "START WITH 100 INCREMENT BY 5",
					},
				},
			},
		})
	})

	t.Run("MakeAColumnSerial", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id integer);`)
		driver.ExecOnTarget(`CREATE TABLE users (id serial);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "id"},
				},
			},
			&PostgresCreateOwnedSequenceInstruction{
				Name:       "users_id_seq",
				TableName:  "users",
				ColumnName: "id",
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "id",
						Expression: `nextval('"users_id_seq"'::regclass)`,
					},
				},
			},
		})
	})

	t.Run("MakeASerialColumnPlain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id serial);`)
		driver.ExecOnTarget(`CREATE TABLE users (id integer);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropDefaultAction{ColumnName: "id"},
				},
			},
			&PostgresDropSequenceInstruction{Name: "users_id_seq"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropNotNullAction{ColumnName: "id"},
				},
			},
		})
	})

	t.Run("ResetIdentityOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5));
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetIdentityOptionsAction{
						ColumnName: "id",
						Options:    "START WITH 1 INCREMENT BY 1",
					},
				},
			},
		})
	})

	t.Run("AlterIdentityOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY (INCREMENT BY 5));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetIdentityOptionsAction{
						ColumnName: "id",
						Options:    "INCREMENT BY 5",
					},
				},
			},
		})
	})

	t.Run("CreateUnloggedTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE UNLOGGED TABLE cache (id INT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "cache",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
				},
				Unlogged: true,
			},
		})
	})

	t.Run("AlterTablePersistence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE cache (id INT);`)
		driver.ExecOnTarget(`CREATE UNLOGGED TABLE cache (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "cache",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetPersistenceAction{Persistence: "UNLOGGED"},
				},
			},
		})
	})

	t.Run("CreateTableWithAReplicaIdentity", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE events (id INT);
			ALTER TABLE events REPLICA IDENTITY FULL;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "events",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "events",
				Actions: []driversshared.AlterTableAction{
					&PostgresReplicaIdentityAction{Mode: "FULL"},
				},
			},
		})
	})

	t.Run("AlterReplicaIdentity", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE events (id INT);`)
		driver.ExecOnTarget(`
			CREATE TABLE events (id INT);
			ALTER TABLE events REPLICA IDENTITY NOTHING;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "events",
				Actions: []driversshared.AlterTableAction{
					&PostgresReplicaIdentityAction{Mode: "NOTHING"},
				},
			},
		})
	})

	t.Run("ReplicaIdentityUsingAnIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE events (id INT NOT NULL, code TEXT NOT NULL);`)
		driver.ExecOnTarget(`
			CREATE TABLE events (id INT NOT NULL, code TEXT NOT NULL);
			CREATE UNIQUE INDEX events_code_key ON events (code);
			ALTER TABLE events REPLICA IDENTITY USING INDEX events_code_key;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateIndexInstruction{
				Definition: "CREATE UNIQUE INDEX events_code_key ON events USING btree (code)",
			},
			&PostgresAlterTableInstruction{
				Name: "events",
				Actions: []driversshared.AlterTableAction{
					&PostgresReplicaIdentityAction{
						Mode:      "USING INDEX",
						IndexName: "events_code_key",
					},
				},
			},
		})
	})

	t.Run("ReplicaIdentityBeforeAnIndexRemoval", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE events (id INT NOT NULL, code TEXT NOT NULL);
			CREATE UNIQUE INDEX events_code_key ON events (code);
			ALTER TABLE events REPLICA IDENTITY USING INDEX events_code_key;
		`)
		driver.ExecOnTarget(`CREATE TABLE events (id INT NOT NULL, code TEXT NOT NULL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "events",
				Actions: []driversshared.AlterTableAction{
					&PostgresReplicaIdentityAction{Mode: "DEFAULT"},
				},
			},
			&driversshared.SQLDropIndexInstruction{Name: "events_code_key"},
		})
	})

	t.Run("CreateTableWithStorageParameters", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE tuned (id INT) WITH (fillfactor = 70);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "tuned",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
				},
				StorageParameters: []string{"fillfactor=70"},
			},
		})
	})

	t.Run("AlterStorageParameters", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE tuned (id INT) WITH (fillfactor = 70);`)
		driver.ExecOnTarget(`CREATE TABLE tuned (id INT) WITH (fillfactor = 90);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "tuned",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStorageParametersAction{
						Parameters: []string{"fillfactor=90"},
					},
				},
			},
		})
	})

	t.Run("ResetStorageParameters", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE tuned (id INT) WITH (fillfactor = 70);`)
		driver.ExecOnTarget(`CREATE TABLE tuned (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "tuned",
				Actions: []driversshared.AlterTableAction{
					&PostgresResetStorageParametersAction{
						Names: []string{"fillfactor"},
					},
				},
			},
		})
	})

	t.Run("CreateViewWithACheckOption", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE base (a INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE base (a INT);
			CREATE VIEW positive AS SELECT a FROM base WHERE a > 0 WITH CASCADED CHECK OPTION;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateViewInstruction{
				Name:        "positive",
				Query:       " SELECT a\n   FROM base\n  WHERE (a > 0);",
				CheckOption: "CASCADED",
			},
		})
	})

	t.Run("ModifyViewCheckOption", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE base (a INT);
			CREATE VIEW positive AS SELECT a FROM base WHERE a > 0;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE base (a INT);
			CREATE VIEW positive AS SELECT a FROM base WHERE a > 0 WITH LOCAL CHECK OPTION;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "positive"},
			&PostgresCreateViewInstruction{
				Name:        "positive",
				Query:       " SELECT a\n   FROM base\n  WHERE (a > 0);",
				CheckOption: "LOCAL",
			},
		})
	})

	t.Run("ModifyAViewOfAnotherOwner", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		// information_schema.views gives no definition of a view that the connecting role
		// does not own, so the diff connects as a role that owns no view.
		driver.ExecOnSource(`
			DO $$
			BEGIN
				IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbdiff_view_owner') THEN
					CREATE ROLE dbdiff_view_owner;
				END IF;
				IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbdiff_view_reader') THEN
					CREATE ROLE dbdiff_view_reader LOGIN PASSWORD 'password';
				END IF;
			END
			$$;
		`)

		for _, schema := range []string{driver.sourceSchema, driver.targetSchema} {
			_, err := driver.conn.ExecContext(t.Context(), fmt.Sprintf(`
				GRANT USAGE ON SCHEMA %[1]s TO dbdiff_view_reader;
				CREATE TABLE %[1]s.base (a integer);
				GRANT SELECT ON ALL TABLES IN SCHEMA %[1]s TO dbdiff_view_reader;
			`, schema))
			require.NoError(t, err)
		}

		driver.ExecOnSource(`
			CREATE VIEW positive AS SELECT a FROM base WHERE a > 0;
			ALTER VIEW positive OWNER TO dbdiff_view_owner;
			GRANT SELECT ON positive TO dbdiff_view_reader;
		`)

		driver.ExecOnTarget(`
			CREATE VIEW positive AS SELECT a FROM base WHERE a > 1 WITH LOCAL CHECK OPTION;
			ALTER VIEW positive OWNER TO dbdiff_view_owner;
			GRANT SELECT ON positive TO dbdiff_view_reader;
		`)

		readerConnectionString := strings.Replace(postgresTestConnectionString,
			"user:password", "dbdiff_view_reader:password", 1)
		driver.source = driversshared.ConnectionStringDataSource{
			ConnectionString: fmt.Sprintf("%s&search_path=%s", readerConnectionString, driver.sourceSchema),
		}
		driver.target = driversshared.ConnectionStringDataSource{
			ConnectionString: fmt.Sprintf("%s&search_path=%s", readerConnectionString, driver.targetSchema),
		}

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "positive"},
			&PostgresCreateViewInstruction{
				Name:        "positive",
				Query:       " SELECT a\n   FROM base\n  WHERE (a > 1);",
				CheckOption: "LOCAL",
			},
		})
	})

	t.Run("CreateRule", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE base (a INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE base (a INT);
			CREATE RULE no_delete AS ON DELETE TO base DO INSTEAD NOTHING;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateRuleInstruction{
				Definition: "CREATE RULE no_delete AS\n    ON DELETE TO base DO INSTEAD NOTHING;",
			},
		})
	})

	t.Run("DropRule", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE base (a INT);
			CREATE RULE no_delete AS ON DELETE TO base DO INSTEAD NOTHING;
		`)
		driver.ExecOnTarget(`CREATE TABLE base (a INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropRuleInstruction{
				Name:      "no_delete",
				TableName: "base",
			},
		})
	})

	t.Run("ModifyRule", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE base (a INT);
			CREATE RULE guard AS ON UPDATE TO base DO INSTEAD NOTHING;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE base (a INT);
			CREATE RULE guard AS ON DELETE TO base DO INSTEAD NOTHING;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropRuleInstruction{
				Name:      "guard",
				TableName: "base",
			},
			&PostgresCreateRuleInstruction{
				Definition: "CREATE RULE guard AS\n    ON DELETE TO base DO INSTEAD NOTHING;",
			},
		})
	})

	t.Run("ViewRuleIsIgnored", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE base (a INT);
			CREATE VIEW v AS SELECT a FROM base;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE base (a INT);
			CREATE VIEW v AS SELECT a FROM base;
		`)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateRuleThatNamesASecondTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE base (a INT);
			CREATE TABLE zlog (a INT);
			CREATE RULE log_insert AS ON INSERT TO base DO ALSO INSERT INTO zlog VALUES (NEW.a);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "base",
				Columns: []*PostgresColumn{
					{
						Name: "a",
						Type: "integer",
					},
				},
			},
			&PostgresCreateTableInstruction{
				Name: "zlog",
				Columns: []*PostgresColumn{
					{
						Name: "a",
						Type: "integer",
					},
				},
			},
			&PostgresCreateRuleInstruction{
				Definition: "CREATE RULE log_insert AS\n    ON INSERT TO base DO  INSERT INTO zlog (a)\n  VALUES (new.a);",
			},
		})
	})

	t.Run("CreateStatistics", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE t (a INT, b INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE t (a INT, b INT);
			CREATE STATISTICS st_ab ON a, b FROM t;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateStatisticsInstruction{
				Definition: "CREATE STATISTICS st_ab ON a, b FROM t",
			},
		})
	})

	t.Run("DropStatistics", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE t (a INT, b INT);
			CREATE STATISTICS st_ab ON a, b FROM t;
		`)
		driver.ExecOnTarget(`CREATE TABLE t (a INT, b INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropStatisticsInstruction{Name: "st_ab"},
		})
	})

	t.Run("ModifyStatistics", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE t (a INT, b INT, c INT);
			CREATE STATISTICS st_ab ON a, b FROM t;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE t (a INT, b INT, c INT);
			CREATE STATISTICS st_ab ON a, c FROM t;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropStatisticsInstruction{Name: "st_ab"},
			&PostgresCreateStatisticsInstruction{
				Definition: "CREATE STATISTICS st_ab ON a, c FROM t",
			},
		})
	})

	t.Run("ComparePrivilegesIsOffByDefault", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			GRANT SELECT ON users TO dbdiff_reader;
		`)
		driver.RequireInstructions(nil)
	})

	t.Run("ComparePrivileges", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			GRANT SELECT, INSERT ON users TO dbdiff_reader;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresGrantInstruction{
				Privileges: []string{"INSERT", "SELECT"},
				ObjectType: "TABLE",
				ObjectName: "users",
				Grantee:    "dbdiff_reader",
			},
		})
	})

	t.Run("RevokePrivileges", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			GRANT SELECT, INSERT ON users TO dbdiff_reader;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			GRANT SELECT ON users TO dbdiff_reader;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresRevokeInstruction{
				Privileges: []string{"INSERT"},
				ObjectType: "TABLE",
				ObjectName: "users",
				Grantee:    "dbdiff_reader",
			},
		})
	})

	t.Run("ARecreatedViewKeepsItsGrants", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnSource(`
			CREATE VIEW numbers AS SELECT 1 AS value;
			GRANT SELECT ON numbers TO dbdiff_reader;
		`)

		driver.ExecOnTarget(`
			CREATE VIEW numbers AS SELECT 2 AS value;
			GRANT SELECT ON numbers TO dbdiff_reader;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "numbers"},
			&PostgresCreateViewInstruction{
				Name:  "numbers",
				Query: " SELECT 2 AS value;",
			},
			&PostgresSetOwnerInstruction{
				ObjectType: "VIEW",
				ObjectName: "numbers",
				Owner:      "user",
			},
			&PostgresGrantInstruction{
				Privileges: []string{"SELECT"},
				ObjectType: "TABLE",
				ObjectName: "numbers",
				Grantee:    "dbdiff_reader",
			},
		})
	})

	t.Run("NoRevokeForADroppedView", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnSource(`
			CREATE VIEW numbers AS SELECT 1 AS value;
			GRANT SELECT ON numbers TO dbdiff_reader;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "numbers"},
		})
	})

	t.Run("AddAndRemoveAnInheritsParent", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE old_parents (note text);
			CREATE TABLE new_parents (note text);
			CREATE TABLE children (id integer) INHERITS (old_parents);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE old_parents (note text);
			CREATE TABLE new_parents (note text);
			CREATE TABLE children (id integer) INHERITS (new_parents);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresInheritAction{ParentName: "new_parents"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresNoInheritAction{ParentName: "old_parents"},
				},
			},
		})
	})

	t.Run("AlterColumnNotNull", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT NOT NULL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "name"},
				},
			},
		})
	})

	t.Run("AlterColumnDefault", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT DEFAULT 'anon');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "name",
						Expression: "'anon'::text",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithAColumnStorage", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE MAIN;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "docs",
				Columns: []*PostgresColumn{
					{
						Name:    "body",
						Type:    "text",
						Storage: "MAIN",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "MAIN",
					},
				},
			},
		})
	})

	t.Run("AlterColumnStorage", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE EXTERNAL;
		`)
		driver.ExecOnTarget(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE MAIN;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "MAIN",
					},
				},
			},
		})
	})

	t.Run("ResetColumnStorage", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE MAIN;
		`)
		driver.ExecOnTarget(`CREATE TABLE docs (body TEXT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "DEFAULT",
					},
				},
			},
		})
	})

	t.Run("ColumnStorageAfterATypeChange", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE docs (body VARCHAR(200));
			ALTER TABLE docs ALTER COLUMN body SET STORAGE MAIN;
		`)
		driver.ExecOnTarget(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE MAIN;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "body",
						DataType:   "text",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "MAIN",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithAStatisticsTarget", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 500;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "docs",
				Columns: []*PostgresColumn{
					{
						Name:             "body",
						Type:             "text",
						StatisticsTarget: sql.NullInt64{Int64: 500, Valid: true},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName: "body",
						Source:     500,
					},
				},
			},
		})
	})

	t.Run("AlterColumnStatisticsTarget", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 100;
		`)
		driver.ExecOnTarget(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 500;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName: "body",
						Source:     500,
					},
				},
			},
		})
	})

	t.Run("ResetColumnStatisticsTarget", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 500;
		`)
		driver.ExecOnTarget(`CREATE TABLE docs (body TEXT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName: "body",
						Source:     -1,
					},
				},
			},
		})
	})

	t.Run("AddColumnWithAStorageAndAStatisticsTarget", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE docs (id INT);`)
		driver.ExecOnTarget(`
			CREATE TABLE docs (id INT, body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE EXTERNAL;
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 250;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:             "body",
							Type:             "text",
							Storage:          "EXTERNAL",
							StatisticsTarget: sql.NullInt64{Int64: 250, Valid: true},
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "EXTERNAL",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName: "body",
						Source:     250,
					},
				},
			},
		})
	})

	t.Run("ConstraintsPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT PRIMARY KEY);`)

		driver.ExecOnTarget(`DROP TABLE users; CREATE TABLE users (id INT, CONSTRAINT pk_users PRIMARY KEY (id));`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "id"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "pk_users",
							Type: "p",
							Def:  "PRIMARY KEY (id)",
						},
					},
				},
			},
		})
	})

	t.Run("ConstraintsUnique", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (email TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (email TEXT, CONSTRAINT uq_email UNIQUE (email));`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "uq_email",
							Type: "u",
							Def:  "UNIQUE (email)",
						},
					},
				},
			},
		})
	})

	t.Run("ConstraintsForeignKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE roles (id INT PRIMARY KEY);
			CREATE TABLE users (role_id INT);
		`)
		driver.ExecOnTarget(`
			CREATE TABLE roles (id INT PRIMARY KEY);
			CREATE TABLE users (role_id INT, CONSTRAINT fk_role FOREIGN KEY (role_id) REFERENCES roles(id));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "fk_role",
							Type: "f",
							Def:  "FOREIGN KEY (role_id) REFERENCES roles(id)",
						},
					},
				},
			},
		})
	})

	t.Run("ConstraintsCheck", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE items (
				price INT,
				CONSTRAINT positive_price CHECK (price > 0),
				CONSTRAINT small_price CHECK (price < 10)
			);
		`)
		driver.ExecOnTarget(`CREATE TABLE items (price INT, CONSTRAINT positive_price CHECK (price >= 0));`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "positive_price"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "positive_price",
							Type: "c",
							Def:  "CHECK ((price >= 0))",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "small_price"},
				},
			},
		})
	})

	t.Run("DropColumnWithPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, code INT, CONSTRAINT pk_users PRIMARY KEY (code));`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "pk_users"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "code"},
				},
			},
		})
	})

	t.Run("DropColumnWithUniqueConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, email TEXT, CONSTRAINT uq_email UNIQUE (email));`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "uq_email"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})
	})

	t.Run("DropColumnKeepsOtherConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INT NOT NULL,
				email TEXT,
				CONSTRAINT pk_users PRIMARY KEY (id),
				CONSTRAINT uq_email UNIQUE (email)
			);
		`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT NOT NULL, CONSTRAINT pk_users PRIMARY KEY (id));`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "uq_email"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})
	})

	t.Run("DropColumnOfCompositeConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, email TEXT, CONSTRAINT uq_users UNIQUE (id, email));`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, CONSTRAINT uq_users UNIQUE (id));`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "uq_users"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "uq_users",
							Type: "u",
							Def:  "UNIQUE (id)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})
	})

	t.Run("Indexes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (name TEXT); CREATE INDEX idx_name ON users(name);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_name ON users USING btree (name)"},
		})
	})

	t.Run("IndexesAndTriggersKeepTheNameOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (name TEXT, email TEXT);`)
		driver.ExecOnTarget(`
			CREATE TABLE users (name TEXT, email TEXT);
			CREATE INDEX idx_b_name ON users(name);
			CREATE INDEX idx_a_email ON users(email);
			CREATE FUNCTION touch() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
			CREATE TRIGGER trg_b_update BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION touch();
			CREATE TRIGGER trg_a_insert BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION touch();
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION touch()\n RETURNS trigger\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN NEW; END; $function$",
			},
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_a_email ON users USING btree (email)"},
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_b_name ON users USING btree (name)"},
			&PostgresCreateTriggerInstruction{Definition: "CREATE TRIGGER trg_a_insert BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION touch()"},
			&PostgresCreateTriggerInstruction{Definition: "CREATE TRIGGER trg_b_update BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION touch()"},
		})
	})

	t.Run("EqualIndexes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `CREATE TABLE users (name TEXT); CREATE INDEX idx_name ON users(name);`
		driver.ExecOnSource(schema)
		driver.ExecOnTarget(schema)
		driver.RequireInstructions(nil)
	})

	t.Run("DropColumnDropsItsIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, email TEXT); CREATE INDEX idx_email ON users(email);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "idx_email"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})
	})

	t.Run("DropColumnKeepsAnotherIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, email TEXT, name TEXT); CREATE INDEX idx_email ON users(email);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT); CREATE INDEX idx_name ON users(name);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_name ON users USING btree (name)"},
			&driversshared.SQLDropIndexInstruction{Name: "idx_email"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})
	})

	t.Run("DropColumnAndModifyAnotherIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, email TEXT, name TEXT); CREATE INDEX idx_email ON users(email); CREATE INDEX idx_name ON users(name);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT); CREATE UNIQUE INDEX idx_name ON users(name);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "idx_name"},
			&PostgresCreateIndexInstruction{Definition: "CREATE UNIQUE INDEX idx_name ON users USING btree (name)"},
			&driversshared.SQLDropIndexInstruction{Name: "idx_email"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})
	})

	t.Run("Triggers", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE OR REPLACE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = NOW();
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
		`
		driver.ExecOnSource(setup)
		driver.ExecOnSource(`CREATE TABLE users (updated_at TIMESTAMP);`)

		driver.ExecOnTarget(setup)
		driver.ExecOnTarget(`
			CREATE TABLE users (updated_at TIMESTAMP);
			CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp()",
			},
		})
	})

	t.Run("CreateADisabledTrigger", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE OR REPLACE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = NOW();
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
		`
		driver.ExecOnSource(setup)
		driver.ExecOnSource(`CREATE TABLE users (updated_at TIMESTAMP);`)

		driver.ExecOnTarget(setup)
		driver.ExecOnTarget(`
			CREATE TABLE users (updated_at TIMESTAMP);
			CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
			ALTER TABLE users DISABLE TRIGGER set_timestamp;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp()",
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresTriggerEnableAction{
						Mode:        "DISABLE",
						TriggerName: "set_timestamp",
					},
				},
			},
		})
	})

	t.Run("AlterTriggerEnableMode", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE OR REPLACE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = NOW();
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;

			CREATE TABLE users (updated_at TIMESTAMP);
			CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
		`
		driver.ExecOnSource(schema)
		driver.ExecOnSource(`ALTER TABLE users DISABLE TRIGGER set_timestamp;`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`ALTER TABLE users ENABLE ALWAYS TRIGGER set_timestamp;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresTriggerEnableAction{
						Mode:        "ENABLE ALWAYS",
						TriggerName: "set_timestamp",
					},
				},
			},
		})
	})

	t.Run("AlterTriggerDefinitionAndEnableMode", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE OR REPLACE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = NOW();
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;

			CREATE TABLE users (updated_at TIMESTAMP);
		`
		driver.ExecOnSource(setup)
		driver.ExecOnSource(`
			CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
		`)

		driver.ExecOnTarget(setup)
		driver.ExecOnTarget(`
			CREATE TRIGGER set_timestamp BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
			ALTER TABLE users ENABLE REPLICA TRIGGER set_timestamp;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropTriggerInstruction{
				Name:      "set_timestamp",
				TableName: "users",
			},
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER set_timestamp BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp()",
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresTriggerEnableAction{
						Mode:        "ENABLE REPLICA",
						TriggerName: "set_timestamp",
					},
				},
			},
		})
	})

	t.Run("EqualTriggers", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE OR REPLACE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = NOW();
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;

			CREATE TABLE users (updated_at TIMESTAMP);
			CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
		`
		driver.ExecOnSource(schema)
		driver.ExecOnTarget(schema)
		driver.RequireInstructions(nil)
	})

	t.Run("Views", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT); CREATE VIEW user_ids AS SELECT id FROM users;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateViewInstruction{
				Name:  "user_ids",
				Query: " SELECT id\n   FROM users;",
			},
		})
	})

	t.Run("CreateSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE SEQUENCE counter;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateSequenceInstruction{
				Name:      "counter",
				DataType:  "bigint",
				Increment: 1,
				Min:       1,
				Max:       9223372036854775807,
				Start:     1,
				Cache:     1,
			},
		})
	})

	t.Run("DropSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropSequenceInstruction{Name: "counter"},
		})
	})

	t.Run("AlterSequenceCache", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)
		driver.ExecOnTarget(`CREATE SEQUENCE counter CACHE 20;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Cache: sql.NullInt64{
					Int64: 20,
					Valid: true,
				},
			},
		})
	})

	t.Run("AlterSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)
		driver.ExecOnTarget(`CREATE SEQUENCE counter INCREMENT BY 2 MAXVALUE 100 CYCLE;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Increment: sql.NullInt64{
					Int64: 2,
					Valid: true,
				},
				Max: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
				Cycle: sql.NullBool{
					Bool:  true,
					Valid: true,
				},
			},
		})
	})

	t.Run("SequenceRestartOnHigherMinimum", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)
		driver.ExecOnSource(`SELECT nextval('counter');`)

		driver.ExecOnTarget(`CREATE SEQUENCE counter MINVALUE 100 START WITH 100;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Min: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
				Start: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
				Restart: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
			},
		})
	})

	t.Run("SequenceRestartOnLowerMaximum", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)
		driver.ExecOnSource(`SELECT nextval('counter') FROM generate_series(1, 10);`)

		driver.ExecOnTarget(`CREATE SEQUENCE counter MAXVALUE 5;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Max: sql.NullInt64{
					Int64: 5,
					Valid: true,
				},
				Restart: sql.NullInt64{
					Int64: 1,
					Valid: true,
				},
			},
		})
	})

	t.Run("SequenceNoRestartWithinRange", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)
		driver.ExecOnSource(`SELECT nextval('counter');`)

		driver.ExecOnTarget(`CREATE SEQUENCE counter INCREMENT BY 2 MAXVALUE 100 CYCLE;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Increment: sql.NullInt64{
					Int64: 2,
					Valid: true,
				},
				Max: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
				Cycle: sql.NullBool{
					Bool:  true,
					Valid: true,
				},
			},
		})
	})

	t.Run("SequenceOfSerialColumnIsIgnored", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE users (id SERIAL);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:               "id",
						Type:               "integer",
						NotNull:            true,
						Serial:             "serial",
						SerialSequenceName: "users_id_seq",
					},
				},
			},
		})
	})

	t.Run("CreateEnumType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Labels: []string{"sad", "ok"},
			},
		})
	})

	t.Run("AddEnumValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)
		driver.ExecOnTarget(`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTypeAddValueInstruction{
				Name:  "mood",
				Value: "happy",
			},
		})
	})

	t.Run("RemoveEnumValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)
		driver.ExecOnTarget(`CREATE TYPE mood AS ENUM ('sad');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropTypeInstruction{Name: "mood"},
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Labels: []string{"sad"},
			},
		})
	})

	t.Run("DropEnumType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE mood AS ENUM ('sad');`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropTypeInstruction{Name: "mood"},
		})
	})

	t.Run("CreateDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE DOMAIN positive_int AS integer NOT NULL DEFAULT 1 CHECK (VALUE > 0);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateDomainInstruction{
				Name:     "positive_int",
				BaseType: "integer",
				Default: sql.NullString{
					String: "1",
					Valid:  true,
				},
				NotNull: true,
				Constraints: []*PostgresDomainConstraint{
					{
						Name: "positive_int_check",
						Def:  "CHECK ((VALUE > 0))",
					},
				},
			},
		})
	})

	t.Run("AlterDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE DOMAIN positive_int AS integer DEFAULT 1;`)
		driver.ExecOnTarget(`CREATE DOMAIN positive_int AS integer NOT NULL DEFAULT 2 CHECK (VALUE > 0);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterDomainInstruction{
				Name:   "positive_int",
				Action: &PostgresSetDomainDefaultAction{Expression: "2"},
			},
			&PostgresAlterDomainInstruction{
				Name:   "positive_int",
				Action: &PostgresSetDomainNotNullAction{},
			},
			&PostgresAlterDomainInstruction{
				Name: "positive_int",
				Action: &PostgresAddDomainConstraintAction{
					ConstraintName: "positive_int_check",
					Definition:     "CHECK ((VALUE > 0))",
				},
			},
		})
	})

	t.Run("RecreateDomainOnNewBaseType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE DOMAIN short_text AS integer;`)
		driver.ExecOnTarget(`CREATE DOMAIN short_text AS varchar(10);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropDomainInstruction{Name: "short_text"},
			&PostgresCreateDomainInstruction{
				Name:     "short_text",
				BaseType: "character varying(10)",
			},
		})
	})

	t.Run("DropDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropDomainInstruction{Name: "positive_int"},
		})
	})

	t.Run("CreateCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TYPE address AS (street TEXT, city VARCHAR(10));`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateCompositeTypeInstruction{
				Name: "address",
				Attributes: []*PostgresCompositeTypeAttribute{
					{
						Name: "street",
						Type: "text",
					},
					{
						Name: "city",
						Type: "character varying(10)",
					},
				},
			},
		})
	})

	t.Run("ModifyCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE address AS (street TEXT);`)
		driver.ExecOnTarget(`CREATE TYPE address AS (street TEXT, city TEXT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropTypeInstruction{Name: "address"},
			&PostgresCreateCompositeTypeInstruction{
				Name: "address",
				Attributes: []*PostgresCompositeTypeAttribute{
					{
						Name: "street",
						Type: "text",
					},
					{
						Name: "city",
						Type: "text",
					},
				},
			},
		})
	})

	t.Run("DropCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE address AS (street TEXT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropTypeInstruction{Name: "address"},
		})
	})

	t.Run("CreateAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnTarget(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateAggregateInstruction{
				Name:               "total",
				Arguments:          "integer",
				TransitionFunction: "int_add",
				StateType:          "integer",
				InitialCondition: sql.NullString{
					String: "0",
					Valid:  true,
				},
			},
		})
	})

	t.Run("ModifyAggregateCombineFunctionAndParallel", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnSource(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer);`)

		driver.ExecOnTarget(setup)
		driver.ExecOnTarget(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, COMBINEFUNC = int_add, PARALLEL = SAFE);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropAggregateInstruction{
				Name:      "total",
				Arguments: "integer",
			},
			&PostgresCreateAggregateInstruction{
				Name:               "total",
				Arguments:          "integer",
				TransitionFunction: "int_add",
				StateType:          "integer",
				CombineFunction: sql.NullString{
					String: "int_add",
					Valid:  true,
				},
				Parallel: "s",
			},
		})
	})

	t.Run("ModifyAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnSource(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '10');`)

		driver.ExecOnTarget(setup)
		driver.ExecOnTarget(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropAggregateInstruction{
				Name:      "total",
				Arguments: "integer",
			},
			&PostgresCreateAggregateInstruction{
				Name:               "total",
				Arguments:          "integer",
				TransitionFunction: "int_add",
				StateType:          "integer",
				InitialCondition: sql.NullString{
					String: "0",
					Valid:  true,
				},
			},
		})
	})

	t.Run("DropAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnSource(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)

		driver.ExecOnTarget(setup)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropAggregateInstruction{
				Name:      "total",
				Arguments: "integer",
			},
		})
	})

	t.Run("DropAggregateBeforeFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`)
		driver.ExecOnSource(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropAggregateInstruction{
				Name:      "total",
				Arguments: "integer",
			},
			&PostgresDropFunctionInstruction{
				Name:      "int_add",
				Arguments: "integer, integer",
			},
		})
	})

	t.Run("DropOperatorBeforeFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`)
		driver.ExecOnSource(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropOperatorInstruction{
				Name: "===",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
			&PostgresDropFunctionInstruction{
				Name:      "int_add",
				Arguments: "integer, integer",
			},
		})
	})

	t.Run("CreateOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnTarget(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateOperatorInstruction{
				Name:     "===",
				Function: "int_add",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
		})
	})

	t.Run("ModifyOperatorCommutatorAndNegator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE FUNCTION int_eq(integer, integer) RETURNS boolean AS $$ SELECT $1 = $2; $$ LANGUAGE sql IMMUTABLE;
			CREATE FUNCTION int_ne(integer, integer) RETURNS boolean AS $$ SELECT $1 <> $2; $$ LANGUAGE sql IMMUTABLE;
			CREATE OPERATOR !== (FUNCTION = int_ne, LEFTARG = integer, RIGHTARG = integer);
		`
		driver.ExecOnSource(setup)
		driver.ExecOnSource(`CREATE OPERATOR === (FUNCTION = int_eq, LEFTARG = integer, RIGHTARG = integer);`)

		driver.ExecOnTarget(setup)
		driver.ExecOnTarget(`
			CREATE OPERATOR === (
				FUNCTION = int_eq, LEFTARG = integer, RIGHTARG = integer,
				COMMUTATOR = ===, NEGATOR = !==, RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES
			);
		`)
		// The NEGATOR clause of the target writes the reverse link into the other
		// operator too, so the diff recreates both operators.
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropOperatorInstruction{
				Name: "!==",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
			&PostgresCreateOperatorInstruction{
				Name:     "!==",
				Function: "int_ne",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				Negator: sql.NullString{
					String: "===",
					Valid:  true,
				},
			},
			&PostgresDropOperatorInstruction{
				Name: "===",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
			&PostgresCreateOperatorInstruction{
				Name:     "===",
				Function: "int_eq",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				Commutator: sql.NullString{
					String: "===",
					Valid:  true,
				},
				Negator: sql.NullString{
					String: "!==",
					Valid:  true,
				},
				RestrictFunction: sql.NullString{
					String: "eqsel",
					Valid:  true,
				},
				JoinFunction: sql.NullString{
					String: "eqjoinsel",
					Valid:  true,
				},
				CanHash:  true,
				CanMerge: true,
			},
		})
	})

	t.Run("ModifyOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;
			CREATE FUNCTION int_sub(integer, integer) RETURNS integer AS $$ SELECT $1 - $2; $$ LANGUAGE sql IMMUTABLE;
		`
		driver.ExecOnSource(setup)
		driver.ExecOnSource(`CREATE OPERATOR === (FUNCTION = int_sub, LEFTARG = integer, RIGHTARG = integer);`)

		driver.ExecOnTarget(setup)
		driver.ExecOnTarget(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropOperatorInstruction{
				Name: "===",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
			&PostgresCreateOperatorInstruction{
				Name:     "===",
				Function: "int_add",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
		})
	})

	t.Run("DropOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnSource(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)

		driver.ExecOnTarget(setup)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropOperatorInstruction{
				Name: "===",
				LeftArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
				RightArgument: sql.NullString{
					String: "integer",
					Valid:  true,
				},
			},
		})
	})

	t.Run("CreateFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE FUNCTION increment(a integer) RETURNS integer AS $$
			BEGIN
				RETURN a + 1;
			END;
			$$ LANGUAGE plpgsql;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(a integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$\n\t\t\tBEGIN\n\t\t\t\tRETURN a + 1;\n\t\t\tEND;\n\t\t\t$function$",
			},
		})
	})

	t.Run("ReplaceFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 2; END; $$ LANGUAGE plpgsql;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(a integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN a + 2; END; $function$",
			},
		})
	})

	t.Run("ReplaceFunctionBodyOnly", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 3; END; $$ LANGUAGE plpgsql;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(a integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN a + 3; END; $function$",
			},
		})
	})

	t.Run("ReplaceFunctionReturnType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION calculate(a integer) RETURNS integer AS $$ BEGIN RETURN a; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION calculate(a integer) RETURNS text AS $$ BEGIN RETURN a::text; END; $$ LANGUAGE plpgsql;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropFunctionInstruction{
				Name:      "calculate",
				Arguments: "a integer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION calculate(a integer)\n RETURNS text\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN a::text; END; $function$",
			},
		})
	})

	t.Run("RenameAFunctionParameter", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION increment(b integer) RETURNS integer AS $$ BEGIN RETURN b + 1; END; $$ LANGUAGE plpgsql;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropFunctionInstruction{
				Name:      "increment",
				Arguments: "a integer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(b integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN b + 1; END; $function$",
			},
		})
	})

	t.Run("DropFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropFunctionInstruction{
				Name:      "increment",
				Arguments: "a integer",
			},
		})
	})

	t.Run("CreateCast", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE TYPE point2d AS (x integer, y integer);
			CREATE FUNCTION point2d_to_text(point2d) RETURNS text AS $$
				SELECT '(' || ($1).x::text || ',' || ($1).y::text || ')';
			$$ LANGUAGE sql IMMUTABLE;
		`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnTarget(`CREATE CAST (point2d AS text) WITH FUNCTION point2d_to_text(point2d) AS ASSIGNMENT;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateCastInstruction{
				SourceType: "point2d",
				TargetType: "text",
				Method:     "f",
				Context:    "a",
				Function:   "point2d_to_text(point2d)",
			},
		})
	})

	t.Run("DropCast", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE TYPE point2d AS (x integer, y integer);
			CREATE FUNCTION point2d_to_text(point2d) RETURNS text AS $$
				SELECT '(' || ($1).x::text || ',' || ($1).y::text || ')';
			$$ LANGUAGE sql IMMUTABLE;
		`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnSource(`CREATE CAST (point2d AS text) WITH FUNCTION point2d_to_text(point2d) AS ASSIGNMENT;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropCastInstruction{
				SourceType: "point2d",
				TargetType: "text",
			},
		})
	})

	t.Run("ModifyCast", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE TYPE point2d AS (x integer, y integer);
			CREATE FUNCTION point2d_to_text(point2d) RETURNS text AS $$
				SELECT '(' || ($1).x::text || ',' || ($1).y::text || ')';
			$$ LANGUAGE sql IMMUTABLE;
		`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnSource(`CREATE CAST (point2d AS text) WITH FUNCTION point2d_to_text(point2d);`)
		driver.ExecOnTarget(`CREATE CAST (point2d AS text) WITH FUNCTION point2d_to_text(point2d) AS IMPLICIT;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropCastInstruction{
				SourceType: "point2d",
				TargetType: "text",
			},
			&PostgresCreateCastInstruction{
				SourceType: "point2d",
				TargetType: "text",
				Method:     "f",
				Context:    "i",
				Function:   "point2d_to_text(point2d)",
			},
		})
	})

	t.Run("Procedures", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE PROCEDURE record_run() LANGUAGE sql AS $$ SELECT 1 $$;`)
		driver.ExecOnSource(`CREATE PROCEDURE outdated() LANGUAGE sql AS $$ SELECT 1 $$;`)
		driver.ExecOnSource(`CREATE PROCEDURE untouched() LANGUAGE sql AS $$ SELECT 1 $$;`)
		driver.ExecOnTarget(`CREATE PROCEDURE record_run() LANGUAGE sql AS $$ SELECT 2 $$;`)
		driver.ExecOnTarget(`CREATE PROCEDURE untouched() LANGUAGE sql AS $$ SELECT 1 $$;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateProcedureInstruction{
				Definition: "CREATE OR REPLACE PROCEDURE record_run()\n LANGUAGE sql\nAS $procedure$ SELECT 2 $procedure$",
			},
			&PostgresDropProcedureInstruction{
				Name:      "outdated",
				Arguments: "",
			},
		})
	})

	t.Run("CreateExtension", func(t *testing.T) {
		driver := NewTestPostgresDriverWithTwoDatabases(t)

		driver.ExecOnTarget(`CREATE EXTENSION pg_trgm;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateExtensionInstruction{Name: "pg_trgm"},
		})
	})

	t.Run("DropExtension", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE EXTENSION pg_trgm;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropExtensionInstruction{Name: "pg_trgm"},
		})
	})

	t.Run("DropExtensionThatOwnsAView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE EXTENSION pg_buffercache;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropExtensionInstruction{Name: "pg_buffercache"},
		})
	})

	t.Run("DropTableBeforeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TYPE mood AS ENUM ('sad');
			CREATE TABLE events (id INT, feeling mood);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "events"},
			&PostgresDropTypeInstruction{Name: "mood"},
		})
	})

	t.Run("DropViewBeforeTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			CREATE VIEW user_ids AS SELECT id FROM users;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "user_ids"},
			&driversshared.SQLDropTableInstruction{Name: "users"},
		})
	})

	t.Run("DropViewBeforeColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT, label TEXT);
			CREATE VIEW user_labels AS SELECT label FROM users;
		`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "user_labels"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "label"},
				},
			},
		})
	})

	t.Run("RecreateViewOnColumnTypeChange", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT, label TEXT);
			CREATE VIEW user_labels AS SELECT label FROM users;
		`)
		driver.ExecOnTarget(`
			CREATE TABLE users (id INT, label VARCHAR);
			CREATE VIEW user_labels AS SELECT label FROM users;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "user_labels"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "label",
						DataType:   "character varying",
					},
				},
			},
			&PostgresCreateViewInstruction{
				Name:  "user_labels",
				Query: " SELECT label\n   FROM users;",
			},
		})
	})

	t.Run("CreateViewsInDependencyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE VIEW view_a AS SELECT id FROM view_b;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateViewInstruction{
				Name:  "view_b",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCreateViewInstruction{
				Name:  "view_a",
				Query: " SELECT id\n   FROM view_b;",
			},
		})
	})

	t.Run("DropViewsInDependencyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE VIEW view_a AS SELECT id FROM view_b;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "view_a"},
			&driversshared.SQLDropViewInstruction{Name: "view_b"},
			&driversshared.SQLDropTableInstruction{Name: "users"},
		})
	})

	t.Run("RecreateTheViewsThatReadARecreatedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE VIEW view_a AS SELECT id FROM view_b;
		`)
		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users WHERE id > 0;
			CREATE VIEW view_a AS SELECT id FROM view_b;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "view_a"},
			&driversshared.SQLDropViewInstruction{Name: "view_b"},
			&PostgresCreateViewInstruction{
				Name:  "view_b",
				Query: " SELECT id\n   FROM users\n  WHERE (id > 0);",
			},
			&PostgresCreateViewInstruction{
				Name:  "view_a",
				Query: " SELECT id\n   FROM view_b;",
			},
		})
	})

	t.Run("RecreateAViewThatCountsTheRowsOfARecreatedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE VIEW view_a AS SELECT count(*) AS total FROM view_b;
		`)
		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users WHERE id > 0;
			CREATE VIEW view_a AS SELECT count(*) AS total FROM view_b;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "view_a"},
			&driversshared.SQLDropViewInstruction{Name: "view_b"},
			&PostgresCreateViewInstruction{
				Name:  "view_b",
				Query: " SELECT id\n   FROM users\n  WHERE (id > 0);",
			},
			&PostgresCreateViewInstruction{
				Name:  "view_a",
				Query: " SELECT count(*) AS total\n   FROM view_b;",
			},
		})
	})

	t.Run("RecreateAChainOfViewsAndMaterializedViews", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE MATERIALIZED VIEW view_a AS SELECT id FROM view_b;
			CREATE VIEW view_c AS SELECT id FROM view_a;
		`)
		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users WHERE id > 0;
			CREATE MATERIALIZED VIEW view_a AS SELECT id FROM view_b;
			CREATE VIEW view_c AS SELECT id FROM view_a;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "view_c"},
			&PostgresDropMaterializedViewInstruction{Name: "view_a"},
			&driversshared.SQLDropViewInstruction{Name: "view_b"},
			&PostgresCreateViewInstruction{
				Name:  "view_b",
				Query: " SELECT id\n   FROM users\n  WHERE (id > 0);",
			},
			&PostgresCreateMaterializedViewInstruction{
				Name:  "view_a",
				Query: " SELECT id\n   FROM view_b;",
			},
			&PostgresCreateViewInstruction{
				Name:  "view_c",
				Query: " SELECT id\n   FROM view_a;",
			},
		})
	})

	t.Run("ARecreatedDependentViewKeepsItsGrants", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnSource(`
			CREATE VIEW view_b AS SELECT 1 AS value;
			CREATE VIEW view_a AS SELECT value FROM view_b;
			GRANT SELECT ON view_a TO dbdiff_reader;
		`)
		driver.ExecOnTarget(`
			CREATE VIEW view_b AS SELECT 2 AS value;
			CREATE VIEW view_a AS SELECT value FROM view_b;
			GRANT SELECT ON view_a TO dbdiff_reader;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "view_a"},
			&driversshared.SQLDropViewInstruction{Name: "view_b"},
			&PostgresCreateViewInstruction{
				Name:  "view_b",
				Query: " SELECT 2 AS value;",
			},
			&PostgresCreateViewInstruction{
				Name:  "view_a",
				Query: " SELECT value\n   FROM view_b;",
			},
			&PostgresSetOwnerInstruction{
				ObjectType: "VIEW",
				ObjectName: "view_a",
				Owner:      "user",
			},
			&PostgresSetOwnerInstruction{
				ObjectType: "VIEW",
				ObjectName: "view_b",
				Owner:      "user",
			},
			&PostgresGrantInstruction{
				Privileges: []string{"SELECT"},
				ObjectType: "TABLE",
				ObjectName: "view_a",
				Grantee:    "dbdiff_reader",
			},
		})
	})

	t.Run("CompareRows", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (id INT PRIMARY KEY, name TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (4, 'Dave');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Robert'), (3, 'Carol');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"3", "'Carol'"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Robert'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
			&driversshared.SQLDeleteInstruction{
				TableName: "users",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "4",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Robert"},
			{"id": int64(3), "name": "Carol"},
		}, rows)
	})

	t.Run("CompareRowsOfATableWithAGeneratedPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (id INT NOT NULL, code INT GENERATED ALWAYS AS (id * 10) STORED, name TEXT, PRIMARY KEY (code));`

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');`)
		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO users (id, name) VALUES (1, 'Alicia'), (3, 'Cara');`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"2", "'Bob'"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Alice'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "code",
							Expression: "10",
						},
					},
				},
			},
			&driversshared.SQLDeleteInstruction{
				TableName: "users",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
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
		}, driver.FetchAllFromSource("users", "ORDER BY code"))
	})

	t.Run("CompareRowsOfANewTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
			INSERT INTO users (id, name) VALUES (2, 'Bob'), (1, 'Alice');
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
					{
						Name: "name",
						Type: "text",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "users_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"1", "'Alice'"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"2", "'Bob'"},
			},
		})

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("CompareRowsWithAGeneratedColumnAndAnIdentityColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE items (
				id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
				price integer,
				total integer GENERATED ALWAYS AS (price * 2) STORED
			);
		`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO items (id, price) OVERRIDING SYSTEM VALUE VALUES (1, 10), (2, 20);`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO items (id, price) OVERRIDING SYSTEM VALUE VALUES (1, 15), (3, 30);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresInsertOverridingInstruction{
				TableName:   "items",
				ColumnNames: []string{"id", "price"},
				Expressions: []string{"3", "30"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "items",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "price",
						Expression: "15",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&driversshared.SQLDeleteInstruction{
				TableName: "items",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})
	})

	t.Run("ChangeAPartitionBound", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE measurements (id integer PRIMARY KEY) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (100);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE measurements (id integer PRIMARY KEY) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (200);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDetachPartitionInstruction{
				ParentName:    "measurements",
				PartitionName: "measurements_low",
			},
			&PostgresAttachPartitionInstruction{
				ParentName:    "measurements",
				PartitionName: "measurements_low",
				Bound:         "FOR VALUES FROM (0) TO (200)",
			},
		})
	})

	t.Run("ModifyTheOwnObjectsOfAPartition", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE measurements (id integer PRIMARY KEY, value integer) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (100);
			CREATE FUNCTION touch() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
		`

		driver.ExecOnSource(schema + `
			CREATE INDEX measurements_low_old ON measurements_low (id, value);
		`)

		driver.ExecOnTarget(schema + `
			CREATE INDEX measurements_value ON measurements (value);
			CREATE TRIGGER parent_trigger BEFORE INSERT ON measurements
				FOR EACH ROW EXECUTE FUNCTION touch();
			COMMENT ON TABLE measurements_low IS 'low range';
			ALTER TABLE measurements_low ENABLE ROW LEVEL SECURITY;
			CREATE POLICY low_read ON measurements_low FOR SELECT USING (value > 0);
			CREATE INDEX measurements_low_id ON measurements_low (id);
			CREATE TRIGGER low_trigger BEFORE INSERT ON measurements_low
				FOR EACH ROW EXECUTE FUNCTION touch();
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "measurements_low_old"},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE INDEX measurements_value ON measurements USING btree (value)",
			},
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER parent_trigger BEFORE INSERT ON measurements FOR EACH ROW EXECUTE FUNCTION touch()",
			},
			&PostgresAlterTableInstruction{
				Name: "measurements_low",
				Actions: []driversshared.AlterTableAction{
					&PostgresRowLevelSecurityAction{Mode: "ENABLE"},
				},
			},
			&PostgresCreatePolicyInstruction{
				Name:       "low_read",
				TableName:  "measurements_low",
				Permissive: "PERMISSIVE",
				Command:    "SELECT",
				Roles:      []string{"public"},
				Using:      "(value > 0)",
			},
			&PostgresCommentOnTableInstruction{
				Name: "measurements_low",
				Text: "low range",
			},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE INDEX measurements_low_id ON measurements_low USING btree (id)",
			},
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER low_trigger BEFORE INSERT ON measurements_low FOR EACH ROW EXECUTE FUNCTION touch()",
			},
		})
	})

	t.Run("ReplaceAPartitionIndexWithAParentIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE measurements (id integer PRIMARY KEY, value integer) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (100);
		`

		driver.ExecOnSource(schema + `
			CREATE INDEX measurements_low_value ON measurements_low (value);
		`)

		driver.ExecOnTarget(schema + `
			CREATE INDEX measurements_value ON measurements (value);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "measurements_low_value"},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE INDEX measurements_value ON measurements USING btree (value)",
			},
		})
	})

	t.Run("ChangeATableToAPartition", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE measurements (id integer PRIMARY KEY) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low (id integer PRIMARY KEY);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE measurements (id integer PRIMARY KEY) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (100);
		`)

		_, err := driver.Diff(t.Context(), driver.source, driver.target, driversshared.DiffOptions{})
		require.EqualError(t, err,
			`the table "measurements_low" changes from a table to a partition, and dbdiff cannot diff this change`)
	})

	t.Run("CompareRowsOfAPartitionedTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE measurements (
				id integer PRIMARY KEY,
				value integer
			) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (100);
		`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO measurements (id, value) VALUES (1, 5);`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO measurements (id, value) VALUES (1, 5), (2, 7);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "measurements",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"2", "7"},
			},
		})
	})

	t.Run("CompareRowsWithAJSONValueAndAByteaValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE documents (
				id integer PRIMARY KEY,
				body jsonb,
				raw bytea
			);
		`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO documents (id, body, raw) VALUES (1, '{"a": 1}', '\x01');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO documents (id, body, raw) VALUES (1, '{"a": 2}', '\x02'), (2, '{"b": 3}', NULL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "documents",
				ColumnNames: []string{"id", "body", "raw"},
				Expressions: []string{"2", `'{"b": 3}'`, "NULL"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "documents",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "body",
						Expression: `'{"a": 2}'`,
					},
					{
						ColumnName: "raw",
						Expression: `'\x02'`,
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
		})
	})

	t.Run("CompareRowsDeletesAChildRowFirst", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE parents (id integer PRIMARY KEY);
			CREATE TABLE children (
				id integer PRIMARY KEY,
				parent integer REFERENCES parents(id)
			);
		`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`
			INSERT INTO parents (id) VALUES (1);
			INSERT INTO children (id, parent) VALUES (10, 1);
		`)

		driver.ExecOnTarget(schema)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDeleteInstruction{
				TableName: "children",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "10",
						},
					},
				},
			},
			&driversshared.SQLDeleteInstruction{
				TableName: "parents",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
		})
	})

	t.Run("CompareRowsOfATableWithoutAPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE logs (message TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO logs (message) VALUES ('stop');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO logs (message) VALUES ('start');`)
		diff := driver.RequireInstructions([]driversshared.Instruction{
			&driversshared.SQLCommentInstruction{
				Text: `The table "logs" holds no primary key, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("logs", "")

		require.Equal(t, []map[string]any{
			{"message": "stop"},
		}, rows)
	})

	t.Run("CompareRowsWithAQuoteAndWithNull", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE notes (id INT PRIMARY KEY, body TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO notes (id, body) VALUES (1, 'plain'), (2, 'not empty');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO notes (id, body) VALUES (1, 'it''s a note'), (2, NULL), (3, NULL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "notes",
				ColumnNames: []string{"id", "body"},
				Expressions: []string{"3", "NULL"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "notes",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "body",
						Expression: "'it''s a note'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "notes",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "body",
						Expression: "NULL",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromSource("notes", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "body": "it's a note"},
			{"id": int64(2), "body": nil},
			{"id": int64(3), "body": nil},
		}, rows)
	})

	t.Run("CompareRowsWithATimestampValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE events (id integer PRIMARY KEY, at timestamptz);`

		driver.ExecOnSource(schema)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO events (id, at) VALUES (1, '2024-01-01T05:00:00+02:00');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "events",
				ColumnNames: []string{"id", "at"},
				Expressions: []string{"1", "'2024-01-01 03:00:00+00:00'"},
			},
		})
	})

	t.Run("CompareRowsWithSpecialFloatValues", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE readings (id integer PRIMARY KEY, value double precision);`

		driver.ExecOnSource(schema)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO readings (id, value) VALUES (1, 'NaN'), (2, 'Infinity'), (3, '-Infinity'), (4, 1.5);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"1", "'NaN'"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"2", "'Infinity'"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"3", "'-Infinity'"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"4", "1.5"},
			},
		})
	})

	t.Run("CompareRowsOfATableWithTheKeyOnAnotherColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		driver.ExecOnSource(`CREATE TABLE items (a integer PRIMARY KEY, b integer);`)
		driver.ExecOnSource(`INSERT INTO items (a, b) VALUES (1, 2);`)

		driver.ExecOnTarget(`CREATE TABLE items (a integer, b integer PRIMARY KEY);`)
		driver.ExecOnTarget(`INSERT INTO items (a, b) VALUES (1, 2);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "b"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "items_pkey"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "items_pkey",
							Type: "p",
							Def:  "PRIMARY KEY (b)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropNotNullAction{ColumnName: "a"},
				},
			},
			&driversshared.SQLCommentInstruction{
				Text: `The table "items" holds another primary key in the source, so dbdiff compares no row of it.`,
			},
		})
	})

	t.Run("CompareRowsOfATableWithAnotherPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		// A new NOT NULL column needs an empty table, so the source holds no row here.
		driver.ExecOnSource(`CREATE TABLE items (label TEXT);`)

		driver.ExecOnTarget(`CREATE TABLE items (code INT PRIMARY KEY, label TEXT);`)
		driver.ExecOnTarget(`INSERT INTO items (code, label) VALUES (1, 'first');`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:    "code",
							Type:    "integer",
							NotNull: true,
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "items_pkey",
							Type: "p",
							Def:  "PRIMARY KEY (code)",
						},
					},
				},
			},
			&driversshared.SQLCommentInstruction{
				Text: `The table "items" holds another primary key in the source, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("items", "ORDER BY code")

		require.Equal(t, []map[string]any(nil), rows)
	})

	t.Run("CompareNoRowWithoutTheDataFlag", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `CREATE TABLE users (id INT PRIMARY KEY, name TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO users (id, name) VALUES (2, 'Bob');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO users (id, name) VALUES (1, 'Alice');`)
		driver.RequireDiffApplies(nil)

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("TableNameThatNeedsQuotes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE "order ""list""" (id INT NOT NULL);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: `order "list"`,
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
				},
			},
		})
	})

	t.Run("ExplicitSchema", func(t *testing.T) {
		harness := NewTestPostgresDriver(t)

		harness.ExecOnTarget(`CREATE TABLE users (id INT NOT NULL);`)

		// The two connection strings hold no search path, so the schema fields of the
		// driver select it.
		driver := NewPostgresDriver(&PostgresDriverConfig{})
		driver.TargetSchema = harness.targetSchema
		driver.SourceSchema = harness.sourceSchema

		source := driversshared.ConnectionStringDataSource{ConnectionString: postgresTestConnectionString}
		target := driversshared.ConnectionStringDataSource{ConnectionString: postgresTestConnectionString}

		instructions, err := driver.Diff(t.Context(), source, target, driversshared.DiffOptions{})
		require.NoError(t, err)

		diff := instructions.String()
		require.Equal(t, `CREATE TABLE "users" (
	"id" integer NOT NULL
);`, diff)

		harness.ExecOnSource(diff)
	})

	t.Run("UnknownSchema", func(t *testing.T) {
		driver := NewPostgresDriver(&PostgresDriverConfig{
			Schema: "schema_that_does_not_exist",
		})

		source := driversshared.ConnectionStringDataSource{ConnectionString: postgresTestConnectionString}
		target := driversshared.ConnectionStringDataSource{ConnectionString: postgresTestConnectionString}

		_, err := driver.Diff(t.Context(), source, target, driversshared.DiffOptions{})
		require.EqualError(t, err, `the target database has no schema with the name "schema_that_does_not_exist"`)
	})

	t.Run("SQLFileTarget", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		migrationsDirectory := t.TempDir()

		sqltest.WriteSQLFile(t, migrationsDirectory, "001_create_users.up.sql", `
			CREATE TABLE users (id INT NOT NULL);
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "002_add_email.up.sql", `
			ALTER TABLE users ADD COLUMN email TEXT;
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "002_add_email.down.sql", `
			ALTER TABLE users DROP COLUMN email;
		`)

		sourcePath := sqltest.WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE users (id INT NOT NULL);
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		source := driversshared.ParseDataSource(sourcePath)
		target := driversshared.ParseDataSource(migrationsDirectory)

		instructions, err := driver.Diff(t.Context(), source, target, driversshared.DiffOptions{})
		require.NoError(t, err)

		require.Equal(t, driversshared.Instructions{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "email",
							Type: "text",
						},
					},
				},
			},
		}, instructions)

		verifyConnection := openVerificationConnection(t, sourcePath)
		_, err = verifyConnection.ExecContext(t.Context(), instructions.String())
		require.NoError(t, err)
	})

	t.Run("EmptyDirectoryTarget", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		sourcePath := sqltest.WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE users (id INT NOT NULL);
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		source := driversshared.ParseDataSource(sourcePath)
		target := driversshared.ParseDataSource(t.TempDir())

		instructions, err := driver.Diff(t.Context(), source, target, driversshared.DiffOptions{})
		require.NoError(t, err)

		require.Equal(t, driversshared.Instructions{
			&driversshared.SQLDropTableInstruction{
				Name: "users",
			},
		}, instructions)

		verifyConnection := openVerificationConnection(t, sourcePath)
		_, err = verifyConnection.ExecContext(t.Context(), instructions.String())
		require.NoError(t, err)
	})

	t.Run("SQLSourceWithConcurrentIndex", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		migrationsDirectory := t.TempDir()

		sqltest.WriteSQLFile(t, migrationsDirectory, "001_create_users.sql", `-- dbdiff:no-transaction
			CREATE TABLE users (id INT NOT NULL, email TEXT, username TEXT);
			CREATE UNIQUE INDEX CONCURRENTLY "uq_users_email" ON users (email);
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "002_set_username_not_null.sql", `-- dbdiff:no-transaction
			DROP INDEX CONCURRENTLY "uq_users_email";
			ALTER TABLE users ALTER COLUMN username SET NOT NULL;
			CREATE INDEX CONCURRENTLY "ix_users_email" ON users (email);
		`)

		sourcePath := sqltest.WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE users (id INT NOT NULL, email TEXT, username TEXT NOT NULL);
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		source := driversshared.ParseDataSource(sourcePath)
		target := driversshared.ParseDataSource(migrationsDirectory)

		instructions, err := driver.Diff(t.Context(), source, target, driversshared.DiffOptions{})
		require.NoError(t, err)

		require.Equal(t, driversshared.Instructions{
			&PostgresCreateIndexInstruction{
				Definition: `CREATE INDEX ix_users_email ON users USING btree (email)`,
			},
		}, instructions)

		verifyConnection := openVerificationConnection(t, sourcePath)
		_, err = verifyConnection.ExecContext(t.Context(), instructions.String())
		require.NoError(t, err)
	})

	t.Run("SQLSourceWithRenamedNotNullColumn", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		targetPath := sqltest.WriteSQLFile(t, t.TempDir(), "target.sql", `
			CREATE TABLE mfa (id INT, login_mfa_id INT NOT NULL);
		`)

		sourcePath := sqltest.WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE mfa (id INT, login_id INT NOT NULL);
			ALTER TABLE mfa RENAME COLUMN login_id TO login_mfa_id;
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		source := driversshared.ParseDataSource(sourcePath)
		target := driversshared.ParseDataSource(targetPath)

		instructions, err := driver.Diff(t.Context(), source, target, driversshared.DiffOptions{})
		require.NoError(t, err)
		require.Empty(t, instructions)
	})

	t.Run("SQLSourceWithNotNullChange", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		targetPath := sqltest.WriteSQLFile(t, t.TempDir(), "target.sql", `
			CREATE TABLE mfa (id INT NOT NULL, label TEXT);
		`)

		sourcePath := sqltest.WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE mfa (id INT, label TEXT NOT NULL);
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		source := driversshared.ParseDataSource(sourcePath)
		target := driversshared.ParseDataSource(targetPath)

		instructions, err := driver.Diff(t.Context(), source, target, driversshared.DiffOptions{})
		require.NoError(t, err)

		require.Equal(t, driversshared.Instructions{
			&PostgresAlterTableInstruction{
				Name: "mfa",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{
						ColumnName: "id",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "mfa",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropNotNullAction{
						ColumnName: "label",
					},
				},
			},
		}, instructions)

		verifyConnection := openVerificationConnection(t, sourcePath)
		_, err = verifyConnection.ExecContext(t.Context(), instructions.String())
		require.NoError(t, err)
	})

	t.Run("DetectScratchVersion", func(t *testing.T) {
		connection, err := sql.Open("pgx", postgresTestConnectionString)
		require.NoError(t, err)

		defer func() {
			require.NoError(t, connection.Close())
		}()

		var versionNumber int

		row := connection.QueryRowContext(t.Context(), "SELECT current_setting('server_version_num')::int")
		require.NoError(t, row.Scan(&versionNumber))

		major := versionNumber / 10000

		version := DetectPostgresScratchVersion(t.Context(), postgresTestConnectionString)
		require.Equal(t, postgresScratchVersions[major], version)
		require.True(t, strings.HasPrefix(string(version), fmt.Sprintf("%d.", major)), version)
	})

	t.Run("DetectScratchVersionOfUnreachableServer", func(t *testing.T) {
		version := DetectPostgresScratchVersion(t.Context(), "postgres://user:password@127.0.0.1:1/absent?sslmode=disable")
		require.Equal(t, embeddedpostgres.PostgresVersion(""), version)
	})

	t.Run("ScratchVersionOfSources", func(t *testing.T) {
		sqlPath := sqltest.WriteSQLFile(t, t.TempDir(), "schema.sql", `CREATE TABLE users (id INT);`)

		liveVersion := DetectPostgresScratchVersion(t.Context(), postgresTestConnectionString)
		require.NotEmpty(t, liveVersion)

		sqlSource := driversshared.ParseDataSource(sqlPath)
		liveSource := driversshared.ParseDataSource(postgresTestConnectionString)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		version, err := driver.scratchVersionOfSources(t.Context(), liveSource, sqlSource)
		require.NoError(t, err)
		require.Equal(t, liveVersion, version)

		version, err = driver.scratchVersionOfSources(t.Context(), sqlSource, liveSource)
		require.NoError(t, err)
		require.Equal(t, liveVersion, version)

		version, err = driver.scratchVersionOfSources(t.Context(), sqlSource, sqlSource)
		require.NoError(t, err)
		require.Equal(t, embeddedpostgres.PostgresVersion(""), version)

		versionedDriver := NewPostgresDriver(&PostgresDriverConfig{
			ScratchServerVersion: "17.11.0",
		})

		version, err = versionedDriver.scratchVersionOfSources(t.Context(), sqlSource, sqlSource)
		require.NoError(t, err)
		require.Equal(t, embeddedpostgres.PostgresVersion("17.11.0"), version)

		version, err = versionedDriver.scratchVersionOfSources(t.Context(), sqlSource, liveSource)
		require.NoError(t, err)
		require.Equal(t, liveVersion, version)
	})

	t.Run("ValidateScratchServerVersion", func(t *testing.T) {
		require.NoError(t, ValidateScratchServerVersion(""))
		require.NoError(t, ValidateScratchServerVersion("17"))
		require.NoError(t, ValidateScratchServerVersion("17.11.0"))

		require.EqualError(t, ValidateScratchServerVersion("99"), `unknown postgres version "99"`)
		require.EqualError(t, ValidateScratchServerVersion("5.2"), `unknown postgres version "5.2"`)
	})

	t.Run("SQLFileTargetAgainstDatabase", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		harness := NewTestPostgresDriver(t)
		harness.ExecOnSource(`CREATE TABLE users (id INT NOT NULL);`)

		targetPath := sqltest.WriteSQLFile(t, t.TempDir(), "schema.sql", `
			CREATE TABLE users (id INT NOT NULL, name TEXT);
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		source := harness.source
		target := driversshared.ParseDataSource(targetPath)

		version, err := driver.scratchVersionOfSources(t.Context(), source, target)
		require.NoError(t, err)
		require.Equal(t, DetectPostgresScratchVersion(t.Context(), postgresTestConnectionString), version)

		instructions, err := driver.Diff(t.Context(), source, target, driversshared.DiffOptions{})
		require.NoError(t, err)

		require.Equal(t, driversshared.Instructions{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "name",
							Type: "text",
						},
					},
				},
			},
		}, instructions)

		harness.ExecOnSource(instructions.String())
	})

	t.Run("HistoryTableStaysOutOfTheDiff", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE dbdiff_migrations (
				version text NOT NULL PRIMARY KEY,
				name text NOT NULL,
				checksum text NOT NULL,
				applied_at timestamptz NOT NULL DEFAULT now()
			);
		`)

		driver.RequireInstructions(nil)
	})

	t.Run("HistoryTableStaysOutOfThePrivilegesDiff", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnSource(`
			CREATE TABLE dbdiff_migrations (
				version text NOT NULL PRIMARY KEY,
				name text NOT NULL,
				checksum text NOT NULL,
				applied_at timestamptz NOT NULL DEFAULT now()
			);
			GRANT SELECT ON dbdiff_migrations TO dbdiff_reader;
		`)

		driver.RequireInstructions(nil)
	})

	t.Run("IgnoredTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT, name TEXT);
			CREATE TABLE ignored_created (id INT);
			CREATE TABLE ignored_changed (id INT, name TEXT);
		`)
		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			CREATE TABLE ignored_dropped (id INT);
			CREATE TABLE ignored_changed (id INT, email TEXT);
		`)

		driver.IgnoreTables = []string{"ignored_created", "ignored_dropped", "ignored_changed"}

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "name",
							Type: "text",
						},
					},
				},
			},
		})
	})
}
