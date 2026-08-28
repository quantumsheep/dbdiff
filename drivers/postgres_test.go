package drivers

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
	tb           testing.TB
	conn         *sql.DB
	targetSchema string
	sourceSchema string
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

	driver, err := NewPostgresDriver(tb.Context(), &PostgresDriverConfig{
		TargetConnectionString: targetConnectionString,
		SourceConnectionString: sourceConnectionString,
	})
	require.NoError(tb, err)

	tb.Cleanup(func() {
		require.NoError(tb, driver.Close())
	})

	return &TestingPostgresDriver{
		PostgresDriver: driver,
		tb:             tb,
		conn:           conn,
		targetSchema:   targetSchema,
		sourceSchema:   sourceSchema,
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

	driver, err := NewPostgresDriver(tb.Context(), &PostgresDriverConfig{
		TargetConnectionString: targetConnectionString,
		SourceConnectionString: sourceConnectionString,
	})
	require.NoError(tb, err)

	// Go calls a cleanup in the reverse order of its registration, so this one closes every
	// connection before the database drop above. A drop fails while a connection uses it.
	tb.Cleanup(func() {
		require.NoError(tb, driver.Close())
	})

	return &TestingPostgresDriver{
		PostgresDriver: driver,
		tb:             tb,
		conn:           adminConn,
		targetSchema:   "public",
		sourceSchema:   "public",
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
	_, err := d.TargetDatabaseConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingPostgresDriver) ExecOnSource(sqlStatements string) {
	d.tb.Helper()
	_, err := d.SourceDatabaseConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingPostgresDriver) RequireInstructions(expected []Instruction) string {
	d.tb.Helper()

	instructions, err := d.Diff(context.Background())
	require.NoError(d.tb, err)
	require.Equal(d.tb, expected, instructions)

	return RenderInstructions(instructions)
}

func (d *TestingPostgresDriver) FetchAllFromSource(table string, additionalRules string) []map[string]any {
	d.tb.Helper()

	rows, err := d.SourceDatabaseConnection.Query(fmt.Sprintf("SELECT * FROM %s %s;", QuoteIdentifier(table), additionalRules))
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

func TestPostgresDriver(t *testing.T) {
	skipWithoutPostgresServer(t)

	t.Run("CreateTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE simple (id INT, name TEXT);`)

		driver.RequireInstructions([]Instruction{
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

		driver.RequireInstructions([]Instruction{
			&SQLDropTableInstruction{Name: "users"},
		})
	})

	t.Run("AddColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT);`)
		driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
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
		driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: "name"},
				},
			},
		})
	})

	t.Run("AlterColumnType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name VARCHAR(50));`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT);`)
		driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "score",
						DataType:   "bigint",
					},
				},
			},
		})

		driver.ExecOnSource(diff)

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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "score",
						DataType:   "integer",
						UsingCast:  true,
					},
				},
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "score": int64(42)},
		}, rows)
	})

	t.Run("CreateTableWithArrayColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE tags (id INT, labels TEXT[]);`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithEnumColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TYPE mood AS ENUM ('sad', 'ok');
			CREATE TABLE users (id INT, mood mood);
		`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("AlterColumnTypeToArray", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, tags INT[]);`)
		driver.ExecOnSource(`INSERT INTO users (id, tags) VALUES (1, ARRAY[5, 6]);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, tags BIGINT[]);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "tags",
						DataType:   "bigint[]",
						UsingCast:  true,
					},
				},
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "tags": "{5,6}"},
		}, rows)
	})

	t.Run("CreateTableWithIdentityColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY, name TEXT);`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
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

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
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

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CreatePartitionedTableWithAForeignKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE zoo (id INT PRIMARY KEY);
			CREATE TABLE animal (id INT, zoo_id INT REFERENCES zoo(id)) PARTITION BY RANGE (id);
			CREATE TABLE animal_low PARTITION OF animal FOR VALUES FROM (0) TO (100);
		`)

		diff := driver.RequireInstructions([]Instruction{
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
				Actions: []AlterTableAction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTablesWithAForeignKeyCycle", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT PRIMARY KEY, org_id INT);
			CREATE TABLE orgs (id INT PRIMARY KEY, owner_id INT REFERENCES users(id));
			ALTER TABLE users ADD CONSTRAINT users_org_id_fkey FOREIGN KEY (org_id) REFERENCES orgs(id);
		`)

		diff := driver.RequireInstructions([]Instruction{
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
				Actions: []AlterTableAction{
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
				Actions: []AlterTableAction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTablesInForeignKeyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE zoo (id INT PRIMARY KEY);
			CREATE TABLE animal (id INT PRIMARY KEY, zoo_id INT REFERENCES zoo(id));
		`)

		diff := driver.RequireInstructions([]Instruction{
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
				Actions: []AlterTableAction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("DropTablesInForeignKeyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE animal (id INT PRIMARY KEY);
			CREATE TABLE zoo (id INT PRIMARY KEY, animal_id INT REFERENCES animal(id));
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLDropTableInstruction{Name: "zoo"},
			&SQLDropTableInstruction{Name: "animal"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("AddForeignKeyToANewTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE animal (id INT PRIMARY KEY, zoo_id INT);`)
		driver.ExecOnTarget(`
			CREATE TABLE zoo (id INT PRIMARY KEY);
			CREATE TABLE animal (id INT PRIMARY KEY, zoo_id INT REFERENCES zoo(id));
		`)
		diff := driver.RequireInstructions([]Instruction{
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
				Actions: []AlterTableAction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithSerialColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)

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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
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

		driver.ExecOnSource(diff)
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

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("AddIdentityToColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT GENERATED BY DEFAULT AS IDENTITY);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "id"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresAddIdentityAction{
						ColumnName: "id",
						Identity:   "BY DEFAULT",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ChangeIdentityGeneration", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT GENERATED BY DEFAULT AS IDENTITY);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresSetIdentityAction{
						ColumnName: "id",
						Identity:   "BY DEFAULT",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropIdentityFromColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresDropIdentityAction{ColumnName: "id"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresDropNotNullAction{ColumnName: "id"},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ModifyGeneratedExpression", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 2) STORED);`)
		driver.ExecOnSource(`INSERT INTO measures (value) VALUES (5);`)
		driver.ExecOnTarget(`CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 3) STORED);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: "doubled"},
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

		driver.ExecOnSource(diff)

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

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("DropPartitionedTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE events (id BIGINT, created DATE NOT NULL)
				PARTITION BY RANGE (created);
			CREATE TABLE events_2024 PARTITION OF events
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLDropTableInstruction{Name: "events"},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateTablePartitionInstruction{
				Name:       "events_2025",
				ParentName: "events",
				Bound:      "FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateMaterializedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, active BOOLEAN);`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT, active BOOLEAN);
			CREATE MATERIALIZED VIEW active_users AS SELECT id FROM users WHERE active;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateMaterializedViewInstruction{
				Name:  "active_users",
				Query: " SELECT id\n   FROM users\n  WHERE active;",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropMaterializedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW all_users AS SELECT id FROM users;
		`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropMaterializedViewInstruction{Name: "all_users"},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropMaterializedViewInstruction{Name: "selected_users"},
			&PostgresCreateMaterializedViewInstruction{
				Name:  "selected_users",
				Query: " SELECT id\n   FROM users\n  WHERE active;",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateMaterializedViewsInDependencyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW first_view AS SELECT id FROM users;
			CREATE MATERIALIZED VIEW second_view AS SELECT id FROM first_view;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateMaterializedViewInstruction{
				Name:  "first_view",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCreateMaterializedViewInstruction{
				Name:  "second_view",
				Query: " SELECT id\n   FROM first_view;",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateMaterializedViewWithIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW all_users AS SELECT id FROM users;
			CREATE UNIQUE INDEX all_users_id ON all_users (id);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateMaterializedViewInstruction{
				Name:  "all_users",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE UNIQUE INDEX all_users_id ON all_users USING btree (id)",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithColumnCollation", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE people (name TEXT COLLATE "C", other TEXT);`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("AlterColumnCollation", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE people (name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE people (name TEXT COLLATE "C");`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "people",
				Actions: []AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "name",
						DataType:   "text",
						Collation:  "C",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithComments", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			COMMENT ON TABLE users IS 'the people';
			COMMENT ON COLUMN users.id IS 'the key';
		`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithRowLevelSecurity", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE docs (id INT, secret BOOLEAN);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
			ALTER TABLE docs FORCE ROW LEVEL SECURITY;
			CREATE POLICY docs_read ON docs FOR SELECT USING (NOT secret);
		`)

		diff := driver.RequireInstructions([]Instruction{
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
				Actions: []AlterTableAction{
					&PostgresRowLevelSecurityAction{Mode: "ENABLE"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []AlterTableAction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("DisableRowLevelSecurity", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE docs (id INT);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
		`)
		driver.ExecOnTarget(`CREATE TABLE docs (id INT);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []AlterTableAction{
					&PostgresRowLevelSecurityAction{Mode: "DISABLE"},
				},
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreatePartitionThatSortsBeforeItsParent", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE zebra (id BIGINT, created DATE NOT NULL)
				PARTITION BY RANGE (created);
			CREATE TABLE alpha PARTITION OF zebra
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableThatInheritsAnotherTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE zparent (a INT);
			CREATE TABLE achild (b INT) INHERITS (zparent);
		`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithIdentityOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5)
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("AlterTwoIdentityOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5));
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresSetIdentityOptionsAction{
						ColumnName: "id",
						Options:    "START WITH 100 INCREMENT BY 5",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("MakeAColumnSerial", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id integer);`)
		driver.ExecOnTarget(`CREATE TABLE users (id serial);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
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
				Actions: []AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "id",
						Expression: `nextval('"users_id_seq"'::regclass)`,
					},
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("MakeASerialColumnPlain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id serial);`)
		driver.ExecOnTarget(`CREATE TABLE users (id integer);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresDropDefaultAction{ColumnName: "id"},
				},
			},
			&PostgresDropSequenceInstruction{Name: "users_id_seq"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresDropNotNullAction{ColumnName: "id"},
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("ResetIdentityOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5));
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresSetIdentityOptionsAction{
						ColumnName: "id",
						Options:    "START WITH 1 INCREMENT BY 1",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("AlterIdentityOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY (INCREMENT BY 5));
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresSetIdentityOptionsAction{
						ColumnName: "id",
						Options:    "INCREMENT BY 5",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateUnloggedTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE UNLOGGED TABLE cache (id INT);`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("AlterTablePersistence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE cache (id INT);`)
		driver.ExecOnTarget(`CREATE UNLOGGED TABLE cache (id INT);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "cache",
				Actions: []AlterTableAction{
					&PostgresSetPersistenceAction{Persistence: "UNLOGGED"},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithAReplicaIdentity", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE events (id INT);
			ALTER TABLE events REPLICA IDENTITY FULL;
		`)

		diff := driver.RequireInstructions([]Instruction{
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
				Actions: []AlterTableAction{
					&PostgresReplicaIdentityAction{Mode: "FULL"},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("AlterReplicaIdentity", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE events (id INT);`)
		driver.ExecOnTarget(`
			CREATE TABLE events (id INT);
			ALTER TABLE events REPLICA IDENTITY NOTHING;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "events",
				Actions: []AlterTableAction{
					&PostgresReplicaIdentityAction{Mode: "NOTHING"},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ReplicaIdentityUsingAnIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE events (id INT NOT NULL, code TEXT NOT NULL);`)
		driver.ExecOnTarget(`
			CREATE TABLE events (id INT NOT NULL, code TEXT NOT NULL);
			CREATE UNIQUE INDEX events_code_key ON events (code);
			ALTER TABLE events REPLICA IDENTITY USING INDEX events_code_key;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateIndexInstruction{
				Definition: "CREATE UNIQUE INDEX events_code_key ON events USING btree (code)",
			},
			&PostgresAlterTableInstruction{
				Name: "events",
				Actions: []AlterTableAction{
					&PostgresReplicaIdentityAction{
						Mode:      "USING INDEX",
						IndexName: "events_code_key",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ReplicaIdentityBeforeAnIndexRemoval", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE events (id INT NOT NULL, code TEXT NOT NULL);
			CREATE UNIQUE INDEX events_code_key ON events (code);
			ALTER TABLE events REPLICA IDENTITY USING INDEX events_code_key;
		`)
		driver.ExecOnTarget(`CREATE TABLE events (id INT NOT NULL, code TEXT NOT NULL);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "events",
				Actions: []AlterTableAction{
					&PostgresReplicaIdentityAction{Mode: "DEFAULT"},
				},
			},
			&SQLDropIndexInstruction{Name: "events_code_key"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithStorageParameters", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE tuned (id INT) WITH (fillfactor = 70);`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("AlterStorageParameters", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE tuned (id INT) WITH (fillfactor = 70);`)
		driver.ExecOnTarget(`CREATE TABLE tuned (id INT) WITH (fillfactor = 90);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "tuned",
				Actions: []AlterTableAction{
					&PostgresSetStorageParametersAction{
						Parameters: []string{"fillfactor=90"},
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ResetStorageParameters", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE tuned (id INT) WITH (fillfactor = 70);`)
		driver.ExecOnTarget(`CREATE TABLE tuned (id INT);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "tuned",
				Actions: []AlterTableAction{
					&PostgresResetStorageParametersAction{
						Names: []string{"fillfactor"},
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateViewWithACheckOption", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE base (a INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE base (a INT);
			CREATE VIEW positive AS SELECT a FROM base WHERE a > 0 WITH CASCADED CHECK OPTION;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateViewInstruction{
				Name:        "positive",
				Query:       " SELECT a\n   FROM base\n  WHERE (a > 0);",
				CheckOption: "CASCADED",
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropViewInstruction{Name: "positive"},
			&PostgresCreateViewInstruction{
				Name:        "positive",
				Query:       " SELECT a\n   FROM base\n  WHERE (a > 0);",
				CheckOption: "LOCAL",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateRule", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE base (a INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE base (a INT);
			CREATE RULE no_delete AS ON DELETE TO base DO INSTEAD NOTHING;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateRuleInstruction{
				Definition: "CREATE RULE no_delete AS\n    ON DELETE TO base DO INSTEAD NOTHING;",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropRule", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE base (a INT);
			CREATE RULE no_delete AS ON DELETE TO base DO INSTEAD NOTHING;
		`)
		driver.ExecOnTarget(`CREATE TABLE base (a INT);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropRuleInstruction{
				Name:      "no_delete",
				TableName: "base",
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropRuleInstruction{
				Name:      "guard",
				TableName: "base",
			},
			&PostgresCreateRuleInstruction{
				Definition: "CREATE RULE guard AS\n    ON DELETE TO base DO INSTEAD NOTHING;",
			},
		})

		driver.ExecOnSource(diff)
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

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateStatistics", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE t (a INT, b INT);`)

		driver.ExecOnTarget(`
			CREATE TABLE t (a INT, b INT);
			CREATE STATISTICS st_ab ON a, b FROM t;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateStatisticsInstruction{
				Definition: "CREATE STATISTICS st_ab ON a, b FROM t",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropStatistics", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE t (a INT, b INT);
			CREATE STATISTICS st_ab ON a, b FROM t;
		`)
		driver.ExecOnTarget(`CREATE TABLE t (a INT, b INT);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropStatisticsInstruction{Name: "st_ab"},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropStatisticsInstruction{Name: "st_ab"},
			&PostgresCreateStatisticsInstruction{
				Definition: "CREATE STATISTICS st_ab ON a, c FROM t",
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresGrantInstruction{
				Privileges: []string{"INSERT", "SELECT"},
				ObjectType: "TABLE",
				ObjectName: "users",
				Grantee:    "dbdiff_reader",
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresRevokeInstruction{
				Privileges: []string{"INSERT"},
				ObjectType: "TABLE",
				ObjectName: "users",
				Grantee:    "dbdiff_reader",
			},
		})

		driver.ExecOnSource(diff)
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

		diff := driver.RequireInstructions([]Instruction{
			&SQLDropViewInstruction{Name: "numbers"},
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("NoRevokeForADroppedView", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnSource(`
			CREATE VIEW numbers AS SELECT 1 AS value;
			GRANT SELECT ON numbers TO dbdiff_reader;
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLDropViewInstruction{Name: "numbers"},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []AlterTableAction{
					&PostgresInheritAction{ParentName: "new_parents"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []AlterTableAction{
					&PostgresNoInheritAction{ParentName: "old_parents"},
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("AlterColumnNotNull", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT NOT NULL);`)
		driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "name"},
				},
			},
		})
	})

	t.Run("AlterColumnDefault", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT DEFAULT 'anon');`)
		driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
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

		diff := driver.RequireInstructions([]Instruction{
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
				Actions: []AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "MAIN",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "MAIN",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ResetColumnStorage", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE MAIN;
		`)
		driver.ExecOnTarget(`CREATE TABLE docs (body TEXT);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "DEFAULT",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "body",
						DataType:   "text",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "MAIN",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithAStatisticsTarget", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 500;
		`)

		diff := driver.RequireInstructions([]Instruction{
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
				Actions: []AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName: "body",
						Source:     500,
					},
				},
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName: "body",
						Source:     500,
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ResetColumnStatisticsTarget", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 500;
		`)
		driver.ExecOnTarget(`CREATE TABLE docs (body TEXT);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName: "body",
						Source:     -1,
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("AddColumnWithAStorageAndAStatisticsTarget", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE docs (id INT);`)
		driver.ExecOnTarget(`
			CREATE TABLE docs (id INT, body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE EXTERNAL;
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 250;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []AlterTableAction{
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
				Actions: []AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "EXTERNAL",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName: "body",
						Source:     250,
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ConstraintsPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT PRIMARY KEY);`)

		driver.ExecOnTarget(`DROP TABLE users; CREATE TABLE users (id INT, CONSTRAINT pk_users PRIMARY KEY (id));`)
		driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "id"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
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
		driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
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
		driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
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

	t.Run("DropColumnWithPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, code INT, CONSTRAINT pk_users PRIMARY KEY (code));`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "pk_users"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: "code"},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropColumnWithUniqueConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, email TEXT, CONSTRAINT uq_email UNIQUE (email));`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "uq_email"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "uq_email"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropColumnOfCompositeConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, email TEXT, CONSTRAINT uq_users UNIQUE (id, email));`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, CONSTRAINT uq_users UNIQUE (id));`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "uq_users"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
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
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("Indexes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (name TEXT); CREATE INDEX idx_name ON users(name);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_name ON users USING btree (name)"},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION touch()\n RETURNS trigger\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN NEW; END; $function$",
			},
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_a_email ON users USING btree (email)"},
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_b_name ON users USING btree (name)"},
			&PostgresCreateTriggerInstruction{Definition: "CREATE TRIGGER trg_a_insert BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION touch()"},
			&PostgresCreateTriggerInstruction{Definition: "CREATE TRIGGER trg_b_update BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION touch()"},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropIndexInstruction{Name: "idx_email"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropColumnKeepsAnotherIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, email TEXT, name TEXT); CREATE INDEX idx_email ON users(email);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT); CREATE INDEX idx_name ON users(name);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_name ON users USING btree (name)"},
			&SQLDropIndexInstruction{Name: "idx_email"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropColumnAndModifyAnotherIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, email TEXT, name TEXT); CREATE INDEX idx_email ON users(email); CREATE INDEX idx_name ON users(name);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT); CREATE UNIQUE INDEX idx_name ON users(name);`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropIndexInstruction{Name: "idx_name"},
			&PostgresCreateIndexInstruction{Definition: "CREATE UNIQUE INDEX idx_name ON users USING btree (name)"},
			&SQLDropIndexInstruction{Name: "idx_email"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp()",
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp()",
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresTriggerEnableAction{
						Mode:        "DISABLE",
						TriggerName: "set_timestamp",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresTriggerEnableAction{
						Mode:        "ENABLE ALWAYS",
						TriggerName: "set_timestamp",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropTriggerInstruction{
				Name:      "set_timestamp",
				TableName: "users",
			},
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER set_timestamp BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp()",
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresTriggerEnableAction{
						Mode:        "ENABLE REPLICA",
						TriggerName: "set_timestamp",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
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
		driver.RequireInstructions([]Instruction{
			&PostgresCreateViewInstruction{
				Name:  "user_ids",
				Query: " SELECT id\n   FROM users;",
			},
		})
	})

	t.Run("CreateSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE SEQUENCE counter;`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("DropSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropSequenceInstruction{Name: "counter"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("AlterSequenceCache", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)
		driver.ExecOnTarget(`CREATE SEQUENCE counter CACHE 20;`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Cache: sql.NullInt64{
					Int64: 20,
					Valid: true,
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("AlterSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)
		driver.ExecOnTarget(`CREATE SEQUENCE counter INCREMENT BY 2 MAXVALUE 100 CYCLE;`)
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("SequenceRestartOnHigherMinimum", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)
		driver.ExecOnSource(`SELECT nextval('counter');`)

		driver.ExecOnTarget(`CREATE SEQUENCE counter MINVALUE 100 START WITH 100;`)
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("SequenceRestartOnLowerMaximum", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)
		driver.ExecOnSource(`SELECT nextval('counter') FROM generate_series(1, 10);`)

		driver.ExecOnTarget(`CREATE SEQUENCE counter MAXVALUE 5;`)
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("SequenceNoRestartWithinRange", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)
		driver.ExecOnSource(`SELECT nextval('counter');`)

		driver.ExecOnTarget(`CREATE SEQUENCE counter INCREMENT BY 2 MAXVALUE 100 CYCLE;`)
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("SequenceOfSerialColumnIsIgnored", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE users (id SERIAL);`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateEnumType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Labels: []string{"sad", "ok"},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("AddEnumValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)
		driver.ExecOnTarget(`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTypeAddValueInstruction{
				Name:  "mood",
				Value: "happy",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("RemoveEnumValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)
		driver.ExecOnTarget(`CREATE TYPE mood AS ENUM ('sad');`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropTypeInstruction{Name: "mood"},
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Labels: []string{"sad"},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropEnumType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE mood AS ENUM ('sad');`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropTypeInstruction{Name: "mood"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE DOMAIN positive_int AS integer NOT NULL DEFAULT 1 CHECK (VALUE > 0);`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("AlterDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE DOMAIN positive_int AS integer DEFAULT 1;`)
		driver.ExecOnTarget(`CREATE DOMAIN positive_int AS integer NOT NULL DEFAULT 2 CHECK (VALUE > 0);`)
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("RecreateDomainOnNewBaseType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE DOMAIN short_text AS integer;`)
		driver.ExecOnTarget(`CREATE DOMAIN short_text AS varchar(10);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropDomainInstruction{Name: "short_text"},
			&PostgresCreateDomainInstruction{
				Name:     "short_text",
				BaseType: "character varying(10)",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropDomainInstruction{Name: "positive_int"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TYPE address AS (street TEXT, city VARCHAR(10));`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("ModifyCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE address AS (street TEXT);`)
		driver.ExecOnTarget(`CREATE TYPE address AS (street TEXT, city TEXT);`)
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("DropCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE address AS (street TEXT);`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropTypeInstruction{Name: "address"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnTarget(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("ModifyAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnSource(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '10');`)

		driver.ExecOnTarget(setup)
		driver.ExecOnTarget(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("DropAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnSource(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)

		driver.ExecOnTarget(setup)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropAggregateInstruction{
				Name:      "total",
				Arguments: "integer",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropAggregateBeforeFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`)
		driver.ExecOnSource(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropAggregateInstruction{
				Name:      "total",
				Arguments: "integer",
			},
			&PostgresDropFunctionInstruction{
				Name:      "int_add",
				Arguments: "integer, integer",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropOperatorBeforeFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`)
		driver.ExecOnSource(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnTarget(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("DropOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnSource(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)

		driver.ExecOnTarget(setup)
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
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

		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(a integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$\n\t\t\tBEGIN\n\t\t\t\tRETURN a + 1;\n\t\t\tEND;\n\t\t\t$function$",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ReplaceFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 2; END; $$ LANGUAGE plpgsql;`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(a integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN a + 2; END; $function$",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ReplaceFunctionBodyOnly", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 3; END; $$ LANGUAGE plpgsql;`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(a integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN a + 3; END; $function$",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ReplaceFunctionReturnType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION calculate(a integer) RETURNS integer AS $$ BEGIN RETURN a; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION calculate(a integer) RETURNS text AS $$ BEGIN RETURN a::text; END; $$ LANGUAGE plpgsql;`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropFunctionInstruction{
				Name:      "calculate",
				Arguments: "a integer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION calculate(a integer)\n RETURNS text\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN a::text; END; $function$",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("RenameAFunctionParameter", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION increment(b integer) RETURNS integer AS $$ BEGIN RETURN b + 1; END; $$ LANGUAGE plpgsql;`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropFunctionInstruction{
				Name:      "increment",
				Arguments: "a integer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(b integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN b + 1; END; $function$",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("DropFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropFunctionInstruction{
				Name:      "increment",
				Arguments: "a integer",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("Procedures", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE PROCEDURE record_run() LANGUAGE sql AS $$ SELECT 1 $$;`)
		driver.ExecOnSource(`CREATE PROCEDURE outdated() LANGUAGE sql AS $$ SELECT 1 $$;`)
		driver.ExecOnSource(`CREATE PROCEDURE untouched() LANGUAGE sql AS $$ SELECT 1 $$;`)
		driver.ExecOnTarget(`CREATE PROCEDURE record_run() LANGUAGE sql AS $$ SELECT 2 $$;`)
		driver.ExecOnTarget(`CREATE PROCEDURE untouched() LANGUAGE sql AS $$ SELECT 1 $$;`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateProcedureInstruction{
				Definition: "CREATE OR REPLACE PROCEDURE record_run()\n LANGUAGE sql\nAS $procedure$ SELECT 2 $procedure$",
			},
			&PostgresDropProcedureInstruction{
				Name:      "outdated",
				Arguments: "",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateExtension", func(t *testing.T) {
		driver := NewTestPostgresDriverWithTwoDatabases(t)

		driver.ExecOnTarget(`CREATE EXTENSION pg_trgm;`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateExtensionInstruction{Name: "pg_trgm"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropExtension", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE EXTENSION pg_trgm;`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropExtensionInstruction{Name: "pg_trgm"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropExtensionThatOwnsAView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE EXTENSION pg_buffercache;`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropExtensionInstruction{Name: "pg_buffercache"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropTableBeforeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TYPE mood AS ENUM ('sad');
			CREATE TABLE events (id INT, feeling mood);
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLDropTableInstruction{Name: "events"},
			&PostgresDropTypeInstruction{Name: "mood"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropViewBeforeTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			CREATE VIEW user_ids AS SELECT id FROM users;
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLDropViewInstruction{Name: "user_ids"},
			&SQLDropTableInstruction{Name: "users"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropViewBeforeColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT, label TEXT);
			CREATE VIEW user_labels AS SELECT label FROM users;
		`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropViewInstruction{Name: "user_labels"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: "label"},
				},
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropViewInstruction{Name: "user_labels"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateViewsInDependencyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE VIEW view_a AS SELECT id FROM view_b;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateViewInstruction{
				Name:  "view_b",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCreateViewInstruction{
				Name:  "view_a",
				Query: " SELECT id\n   FROM view_b;",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropViewsInDependencyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE VIEW view_a AS SELECT id FROM view_b;
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLDropViewInstruction{Name: "view_a"},
			&SQLDropViewInstruction{Name: "view_b"},
			&SQLDropTableInstruction{Name: "users"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CompareRows", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (id INT PRIMARY KEY, name TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (4, 'Dave');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Robert'), (3, 'Carol');`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"3", "'Carol'"},
			},
			&SQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*SQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Robert'",
					},
				},
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
			&SQLDeleteInstruction{
				TableName: "users",
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "4",
						},
					},
				},
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Robert"},
			{"id": int64(3), "name": "Carol"},
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
		diff := driver.RequireInstructions([]Instruction{
			&PostgresInsertOverridingInstruction{
				TableName:   "items",
				ColumnNames: []string{"id", "price"},
				Expressions: []string{"3", "30"},
			},
			&SQLUpdateInstruction{
				TableName: "items",
				SetClauses: []*SQLSetClause{
					{
						ColumnName: "price",
						Expression: "15",
					},
				},
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&SQLDeleteInstruction{
				TableName: "items",
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
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
		diff := driver.RequireInstructions([]Instruction{
			&SQLInsertInstruction{
				TableName:   "measurements",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"2", "7"},
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&SQLInsertInstruction{
				TableName:   "documents",
				ColumnNames: []string{"id", "body", "raw"},
				Expressions: []string{"2", `'{"b": 3}'`, "NULL"},
			},
			&SQLUpdateInstruction{
				TableName: "documents",
				SetClauses: []*SQLSetClause{
					{
						ColumnName: "body",
						Expression: `'{"a": 2}'`,
					},
					{
						ColumnName: "raw",
						Expression: `'\x02'`,
					},
				},
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
		})

		driver.ExecOnSource(diff)
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
		diff := driver.RequireInstructions([]Instruction{
			&SQLDeleteInstruction{
				TableName: "children",
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "10",
						},
					},
				},
			},
			&SQLDeleteInstruction{
				TableName: "parents",
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CompareRowsOfATableWithoutAPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE logs (message TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO logs (message) VALUES ('stop');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO logs (message) VALUES ('start');`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLCommentInstruction{
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
		diff := driver.RequireInstructions([]Instruction{
			&SQLInsertInstruction{
				TableName:   "notes",
				ColumnNames: []string{"id", "body"},
				Expressions: []string{"3", "NULL"},
			},
			&SQLUpdateInstruction{
				TableName: "notes",
				SetClauses: []*SQLSetClause{
					{
						ColumnName: "body",
						Expression: "'it''s a note'",
					},
				},
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&SQLUpdateInstruction{
				TableName: "notes",
				SetClauses: []*SQLSetClause{
					{
						ColumnName: "body",
						Expression: "NULL",
					},
				},
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("notes", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "body": "it's a note"},
			{"id": int64(2), "body": nil},
			{"id": int64(3), "body": nil},
		}, rows)
	})

	t.Run("CompareRowsWithSpecialFloatValues", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE readings (id integer PRIMARY KEY, value double precision);`

		driver.ExecOnSource(schema)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO readings (id, value) VALUES (1, 'NaN'), (2, 'Infinity'), (3, '-Infinity'), (4, 1.5);`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"1", "'NaN'"},
			},
			&SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"2", "'Infinity'"},
			},
			&SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"3", "'-Infinity'"},
			},
			&SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"4", "1.5"},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CompareRowsOfATableWithTheKeyOnAnotherColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		driver.ExecOnSource(`CREATE TABLE items (a integer PRIMARY KEY, b integer);`)
		driver.ExecOnSource(`INSERT INTO items (a, b) VALUES (1, 2);`)

		driver.ExecOnTarget(`CREATE TABLE items (a integer, b integer PRIMARY KEY);`)
		driver.ExecOnTarget(`INSERT INTO items (a, b) VALUES (1, 2);`)
		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "b"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "items_pkey"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []AlterTableAction{
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
				Actions: []AlterTableAction{
					&PostgresDropNotNullAction{ColumnName: "a"},
				},
			},
			&SQLCommentInstruction{
				Text: `The table "items" holds another primary key in the source, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CompareRowsOfATableWithAnotherPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		// A new NOT NULL column needs an empty table, so the source holds no row here.
		driver.ExecOnSource(`CREATE TABLE items (label TEXT);`)

		driver.ExecOnTarget(`CREATE TABLE items (code INT PRIMARY KEY, label TEXT);`)
		driver.ExecOnTarget(`INSERT INTO items (code, label) VALUES (1, 'first');`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []AlterTableAction{
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
				Actions: []AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "items_pkey",
							Type: "p",
							Def:  "PRIMARY KEY (code)",
						},
					},
				},
			},
			&SQLCommentInstruction{
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
		diff := driver.RequireInstructions(nil)

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("TableNameThatNeedsQuotes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE "order ""list""" (id INT NOT NULL);`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("ExplicitSchema", func(t *testing.T) {
		harness := NewTestPostgresDriver(t)

		harness.ExecOnTarget(`CREATE TABLE users (id INT NOT NULL);`)

		// The two connection strings hold no search path, so the config selects the schema.
		driver, err := NewPostgresDriver(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: postgresTestConnectionString,
			SourceConnectionString: postgresTestConnectionString,
			TargetSchema:           harness.targetSchema,
			SourceSchema:           harness.sourceSchema,
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, driver.Close())
		})

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)

		diff := RenderInstructions(instructions)
		require.Equal(t, `CREATE TABLE "users" (
	"id" integer NOT NULL
);`, diff)

		harness.ExecOnSource(diff)
	})

	t.Run("UnknownSchema", func(t *testing.T) {
		driver, err := NewPostgresDriver(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: postgresTestConnectionString,
			SourceConnectionString: postgresTestConnectionString,
			TargetSchema:           "schema_that_does_not_exist",
			SourceSchema:           "schema_that_does_not_exist",
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, driver.Close())
		})

		_, err = driver.Diff(t.Context())
		require.EqualError(t, err, `the target database has no schema with the name "schema_that_does_not_exist"`)
	})

	t.Run("SQLFileTarget", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		migrationsDirectory := t.TempDir()

		WriteSQLFile(t, migrationsDirectory, "001_create_users.up.sql", `
			CREATE TABLE users (id INT NOT NULL);
		`)
		WriteSQLFile(t, migrationsDirectory, "002_add_email.up.sql", `
			ALTER TABLE users ADD COLUMN email TEXT;
		`)
		WriteSQLFile(t, migrationsDirectory, "002_add_email.down.sql", `
			ALTER TABLE users DROP COLUMN email;
		`)

		sourcePath := WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE users (id INT NOT NULL);
		`)

		driver, err := NewPostgresDriver(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: migrationsDirectory,
			SourceConnectionString: sourcePath,
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, driver.Close())
		})

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)

		require.Equal(t, []Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "email",
							Type: "text",
						},
					},
				},
			},
		}, instructions)

		_, err = driver.SourceDatabaseConnection.ExecContext(t.Context(), RenderInstructions(instructions))
		require.NoError(t, err)
	})

	t.Run("EmptyDirectoryTarget", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		sourcePath := WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE users (id INT NOT NULL);
		`)

		driver, err := NewPostgresDriver(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: t.TempDir(),
			SourceConnectionString: sourcePath,
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, driver.Close())
		})

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)

		require.Equal(t, []Instruction{
			&SQLDropTableInstruction{
				Name: "users",
			},
		}, instructions)
	})

	t.Run("SQLSourceWithConcurrentIndex", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		migrationsDirectory := t.TempDir()

		WriteSQLFile(t, migrationsDirectory, "001_create_users.sql", `-- dbdiff:no-transaction
			CREATE TABLE users (id INT NOT NULL, email TEXT, username TEXT);
			CREATE UNIQUE INDEX CONCURRENTLY "uq_users_email" ON users (email);
		`)
		WriteSQLFile(t, migrationsDirectory, "002_set_username_not_null.sql", `-- dbdiff:no-transaction
			DROP INDEX CONCURRENTLY "uq_users_email";
			ALTER TABLE users ALTER COLUMN username SET NOT NULL;
			CREATE INDEX CONCURRENTLY "ix_users_email" ON users (email);
		`)

		sourcePath := WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE users (id INT NOT NULL, email TEXT, username TEXT NOT NULL);
		`)

		driver, err := NewPostgresDriver(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: migrationsDirectory,
			SourceConnectionString: sourcePath,
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, driver.Close())
		})

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)

		require.Equal(t, []Instruction{
			&PostgresCreateIndexInstruction{
				Definition: `CREATE INDEX ix_users_email ON users USING btree (email)`,
			},
		}, instructions)

		_, err = driver.SourceDatabaseConnection.ExecContext(t.Context(), RenderInstructions(instructions))
		require.NoError(t, err)
	})

	t.Run("SQLSourceWithRenamedNotNullColumn", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		targetPath := WriteSQLFile(t, t.TempDir(), "target.sql", `
			CREATE TABLE mfa (id INT, login_mfa_id INT NOT NULL);
		`)

		sourcePath := WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE mfa (id INT, login_id INT NOT NULL);
			ALTER TABLE mfa RENAME COLUMN login_id TO login_mfa_id;
		`)

		driver, err := NewPostgresDriver(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: targetPath,
			SourceConnectionString: sourcePath,
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, driver.Close())
		})

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)
		require.Empty(t, instructions)
	})

	t.Run("SQLSourceWithNotNullChange", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		targetPath := WriteSQLFile(t, t.TempDir(), "target.sql", `
			CREATE TABLE mfa (id INT NOT NULL, label TEXT);
		`)

		sourcePath := WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE mfa (id INT, label TEXT NOT NULL);
		`)

		driver, err := NewPostgresDriver(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: targetPath,
			SourceConnectionString: sourcePath,
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, driver.Close())
		})

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)

		require.Equal(t, []Instruction{
			&PostgresAlterTableInstruction{
				Name: "mfa",
				Actions: []AlterTableAction{
					&PostgresSetNotNullAction{
						ColumnName: "id",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "mfa",
				Actions: []AlterTableAction{
					&PostgresDropNotNullAction{
						ColumnName: "label",
					},
				},
			},
		}, instructions)

		_, err = driver.SourceDatabaseConnection.ExecContext(t.Context(), RenderInstructions(instructions))
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

	t.Run("ScratchVersionOfConfig", func(t *testing.T) {
		sqlPath := WriteSQLFile(t, t.TempDir(), "schema.sql", `CREATE TABLE users (id INT);`)

		liveVersion := DetectPostgresScratchVersion(t.Context(), postgresTestConnectionString)
		require.NotEmpty(t, liveVersion)

		version := postgresScratchVersionOfConfig(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: sqlPath,
			SourceConnectionString: postgresTestConnectionString,
		})
		require.Equal(t, liveVersion, version)

		version = postgresScratchVersionOfConfig(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: postgresTestConnectionString,
			SourceConnectionString: sqlPath,
		})
		require.Equal(t, liveVersion, version)

		version = postgresScratchVersionOfConfig(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: sqlPath,
			SourceConnectionString: sqlPath,
		})
		require.Equal(t, embeddedpostgres.PostgresVersion(""), version)

		version = postgresScratchVersionOfConfig(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: sqlPath,
			SourceConnectionString: sqlPath,
			ScratchServerVersion:   "17.11.0",
		})
		require.Equal(t, embeddedpostgres.PostgresVersion("17.11.0"), version)

		version = postgresScratchVersionOfConfig(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: postgresTestConnectionString,
			SourceConnectionString: sqlPath,
			ScratchServerVersion:   "17.11.0",
		})
		require.Equal(t, liveVersion, version)
	})

	t.Run("SQLFileTargetAgainstDatabase", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		harness := NewTestPostgresDriver(t)
		harness.ExecOnSource(`CREATE TABLE users (id INT NOT NULL);`)

		targetPath := WriteSQLFile(t, t.TempDir(), "schema.sql", `
			CREATE TABLE users (id INT NOT NULL, name TEXT);
		`)

		driver, err := NewPostgresDriver(t.Context(), &PostgresDriverConfig{
			TargetConnectionString: targetPath,
			SourceConnectionString: postgresTestConnectionString,
			SourceSchema:           harness.sourceSchema,
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, driver.Close())
		})

		require.Equal(t, DetectPostgresScratchVersion(t.Context(), postgresTestConnectionString), driver.ScratchVersion)

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)

		require.Equal(t, []Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "name",
							Type: "text",
						},
					},
				},
			},
		}, instructions)

		harness.ExecOnSource(RenderInstructions(instructions))
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
}
