package drivers

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

const postgresTestConnectionString = "postgres://user:password@localhost:5432/dbdiff?sslmode=disable"

type TestingPostgresDriver struct {
	*PostgresDriver
	tb           testing.TB
	conn         *sql.DB
	sourceSchema string
	targetSchema string
}

func NewTestPostgresDriver(tb testing.TB) *TestingPostgresDriver {
	tb.Helper()

	connectionString := postgresTestConnectionString
	conn, err := sql.Open("pgx", connectionString)
	require.NoError(tb, err)

	err = conn.PingContext(tb.Context())
	require.NoError(tb, err)

	id := time.Now().UnixNano()
	sourceSchema := fmt.Sprintf("source_%d", id)
	targetSchema := fmt.Sprintf("target_%d", id)

	_, err = conn.ExecContext(tb.Context(), fmt.Sprintf("CREATE SCHEMA %s", sourceSchema))
	require.NoError(tb, err)
	_, err = conn.ExecContext(tb.Context(), fmt.Sprintf("CREATE SCHEMA %s", targetSchema))
	require.NoError(tb, err)

	// The connection stays open for this cleanup. A closed connection drops no schema.
	tb.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", sourceSchema))
		require.NoError(tb, err)

		_, err = conn.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", targetSchema))
		require.NoError(tb, err)

		require.NoError(tb, conn.Close())
	})

	sourceConnectionString := fmt.Sprintf("%s&search_path=%s", connectionString, sourceSchema)
	targetConnectionString := fmt.Sprintf("%s&search_path=%s", connectionString, targetSchema)

	driver, err := NewPostgresDriver(&PostgresDriverConfig{
		SourceConnectionString: sourceConnectionString,
		TargetConnectionString: targetConnectionString,
	})
	require.NoError(tb, err)

	tb.Cleanup(func() {
		require.NoError(tb, driver.Close())
	})

	return &TestingPostgresDriver{
		PostgresDriver: driver,
		tb:             tb,
		conn:           conn,
		sourceSchema:   sourceSchema,
		targetSchema:   targetSchema,
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

	// A CREATE DATABASE statement needs a connection outside a transaction, so the admin
	// connection runs it against the dbdiff database.
	id := time.Now().UnixNano()
	sourceDatabase := fmt.Sprintf("dbdiff_source_%d", id)
	targetDatabase := fmt.Sprintf("dbdiff_target_%d", id)

	_, err = adminConn.ExecContext(tb.Context(), fmt.Sprintf("CREATE DATABASE %s", sourceDatabase))
	require.NoError(tb, err)
	_, err = adminConn.ExecContext(tb.Context(), fmt.Sprintf("CREATE DATABASE %s", targetDatabase))
	require.NoError(tb, err)

	// The admin connection stays open for this cleanup.
	tb.Cleanup(func() {
		_, err := adminConn.ExecContext(context.Background(), fmt.Sprintf("DROP DATABASE %s", sourceDatabase))
		require.NoError(tb, err)

		_, err = adminConn.ExecContext(context.Background(), fmt.Sprintf("DROP DATABASE %s", targetDatabase))
		require.NoError(tb, err)

		require.NoError(tb, adminConn.Close())
	})

	sourceConnectionString := fmt.Sprintf("postgres://user:password@localhost:5432/%s?sslmode=disable", sourceDatabase)
	targetConnectionString := fmt.Sprintf("postgres://user:password@localhost:5432/%s?sslmode=disable", targetDatabase)

	driver, err := NewPostgresDriver(&PostgresDriverConfig{
		SourceConnectionString: sourceConnectionString,
		TargetConnectionString: targetConnectionString,
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
		sourceSchema:   "public",
		targetSchema:   "public",
	}
}

func (d *TestingPostgresDriver) ExecOnSource(sqlStatements string) {
	d.tb.Helper()
	_, err := d.SourceDatabaseConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingPostgresDriver) ExecOnTarget(sqlStatements string) {
	d.tb.Helper()
	_, err := d.TargetDatabaseConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

// RequireInstructions compares the instructions of the diff. The SQL text of each kind
// belongs to instruction_test.go, so this method compares no text. It returns the rendered
// diff, so the caller applies it to the target.
func (d *TestingPostgresDriver) RequireInstructions(expected []Instruction) string {
	d.tb.Helper()

	instructions, err := d.Diff(context.Background())
	require.NoError(d.tb, err)
	require.Equal(d.tb, expected, instructions)

	return RenderInstructions(instructions)
}

func (d *TestingPostgresDriver) FetchAllFromTarget(table string, additionalRules string) []map[string]any {
	d.tb.Helper()

	rows, err := d.TargetDatabaseConnection.Query(fmt.Sprintf("SELECT * FROM %s %s;", quoteIdentifier(table), additionalRules))
	require.NoError(d.tb, err)

	defer rows.Close()

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
	t.Run("CreateTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE simple (id INT, name TEXT);`)

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

		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)

		driver.RequireInstructions([]Instruction{
			&SQLDropTableInstruction{Name: "users"},
		})
	})

	t.Run("AddColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)

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

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT);`)

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

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name VARCHAR(50));`)

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

		driver.ExecOnSource(`CREATE TABLE users (id INT, score BIGINT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, score INT);`)
		driver.ExecOnTarget(`INSERT INTO users (id, score) VALUES (1, 42);`)

		// PostgreSQL casts an integer to a bigint, so the statement needs no USING clause.
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

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "score": int64(42)},
		}, rows)
	})

	t.Run("AlterColumnTypeWithoutAutomaticCast", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, score INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, score TEXT);`)
		driver.ExecOnTarget(`INSERT INTO users (id, score) VALUES (1, '42');`)

		// PostgreSQL casts no text to an integer, so the statement needs a USING clause.
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

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "score": int64(42)},
		}, rows)
	})

	t.Run("CreateTableWithArrayColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE tags (id INT, labels TEXT[]);`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateTableWithEnumColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TYPE mood AS ENUM ('sad', 'ok');
			CREATE TABLE users (id INT, mood mood);
		`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Values: []string{"sad", "ok"},
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

		driver.ExecOnTarget(diff)
	})

	t.Run("AlterColumnTypeToArray", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, tags BIGINT[]);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, tags INT[]);`)
		driver.ExecOnTarget(`INSERT INTO users (id, tags) VALUES (1, ARRAY[5, 6]);`)

		// information_schema.columns gives the text ARRAY for both types. format_type gives
		// the exact type name, so the statement below is valid SQL.
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

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "tags": "{5,6}"},
		}, rows)
	})

	t.Run("AlterColumnNotNull", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT NOT NULL);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT);`)

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

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT DEFAULT 'anon');`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT);`)

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

	t.Run("ConstraintsPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT PRIMARY KEY);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)

		driver.ExecOnSource(`DROP TABLE users; CREATE TABLE users (id INT, CONSTRAINT pk_users PRIMARY KEY (id));`)

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

		driver.ExecOnSource(`CREATE TABLE users (email TEXT, CONSTRAINT uq_email UNIQUE (email));`)
		driver.ExecOnTarget(`CREATE TABLE users (email TEXT);`)

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
			CREATE TABLE users (role_id INT, CONSTRAINT fk_role FOREIGN KEY (role_id) REFERENCES roles(id));
		`)
		driver.ExecOnTarget(`
			CREATE TABLE roles (id INT PRIMARY KEY);
			CREATE TABLE users (role_id INT);
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

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, code INT, CONSTRAINT pk_users PRIMARY KEY (code));`)

		// PostgreSQL drops the constraint with the column, so its drop comes first.
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

		driver.ExecOnTarget(diff)
	})

	t.Run("DropColumnWithUniqueConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, email TEXT, CONSTRAINT uq_email UNIQUE (email));`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("DropColumnKeepsOtherConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT NOT NULL, CONSTRAINT pk_users PRIMARY KEY (id));`)
		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INT NOT NULL,
				email TEXT,
				CONSTRAINT pk_users PRIMARY KEY (id),
				CONSTRAINT uq_email UNIQUE (email)
			);
		`)

		// The primary key covers no removed column, so the diff keeps it.
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

		driver.ExecOnTarget(diff)
	})

	t.Run("DropColumnOfCompositeConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, CONSTRAINT uq_users UNIQUE (id));`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, email TEXT, CONSTRAINT uq_users UNIQUE (id, email));`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("Indexes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (name TEXT); CREATE INDEX idx_name ON users(name);`)
		driver.ExecOnTarget(`CREATE TABLE users (name TEXT);`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_name ON users USING btree (name)"},
		})

		driver.ExecOnTarget(diff)
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

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, email TEXT); CREATE INDEX idx_email ON users(email);`)

		// A DROP COLUMN statement drops every index of that column, so the DROP INDEX
		// statement must print first.
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropIndexInstruction{Name: "idx_email"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("DropColumnKeepsAnotherIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT); CREATE INDEX idx_name ON users(name);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, email TEXT, name TEXT); CREATE INDEX idx_email ON users(email);`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("DropColumnAndModifyAnotherIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT); CREATE UNIQUE INDEX idx_name ON users(name);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, email TEXT, name TEXT); CREATE INDEX idx_email ON users(email); CREATE INDEX idx_name ON users(name);`)

		// The two statements of the modified index must stay adjacent.
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

		driver.ExecOnTarget(diff)
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
		driver.ExecOnTarget(setup)

		driver.ExecOnSource(`
			CREATE TABLE users (updated_at TIMESTAMP);
			CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
		`)
		driver.ExecOnTarget(`CREATE TABLE users (updated_at TIMESTAMP);`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp()",
			},
		})

		driver.ExecOnTarget(diff)
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

		driver.ExecOnSource(`CREATE TABLE users (id INT); CREATE VIEW user_ids AS SELECT id FROM users;`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)

		driver.RequireInstructions([]Instruction{
			&PostgresCreateViewInstruction{
				Name:  "user_ids",
				Query: " SELECT id\n   FROM users;",
			},
		})
	})

	t.Run("CreateSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateSequenceInstruction{
				Name:      "counter",
				DataType:  "bigint",
				Increment: 1,
				Min:       1,
				Max:       9223372036854775807,
				Start:     1,
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("DropSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE SEQUENCE counter;`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropSequenceInstruction{Name: "counter"},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("AlterSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter INCREMENT BY 2 MAXVALUE 100 CYCLE;`)
		driver.ExecOnTarget(`CREATE SEQUENCE counter;`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("SequenceRestartOnHigherMinimum", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter MINVALUE 100 START WITH 100;`)
		driver.ExecOnTarget(`CREATE SEQUENCE counter;`)

		// The current value stays below the new minimum, so MINVALUE needs a RESTART.
		driver.ExecOnTarget(`SELECT nextval('counter');`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("SequenceRestartOnLowerMaximum", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter MAXVALUE 5;`)
		driver.ExecOnTarget(`CREATE SEQUENCE counter;`)

		// The current value climbs above the new maximum, so MAXVALUE needs a RESTART.
		driver.ExecOnTarget(`SELECT nextval('counter') FROM generate_series(1, 10);`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("SequenceNoRestartWithinRange", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter INCREMENT BY 2 MAXVALUE 100 CYCLE;`)
		driver.ExecOnTarget(`CREATE SEQUENCE counter;`)

		// The current value stays inside the new range, so the diff holds no RESTART.
		driver.ExecOnTarget(`SELECT nextval('counter');`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("SequenceOfSerialColumnIsIgnored", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id SERIAL);`)

		// The table creates its own sequence. The diff holds the table only.
		driver.RequireInstructions([]Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
						Default: sql.NullString{
							String: "nextval('users_id_seq'::regclass)",
							Valid:  true,
						},
					},
				},
			},
		})
	})

	t.Run("CreateEnumType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Values: []string{"sad", "ok"},
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("AddEnumValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');`)
		driver.ExecOnTarget(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresAlterTypeAddValueInstruction{
				Name:  "mood",
				Value: "happy",
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("RemoveEnumValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE mood AS ENUM ('sad');`)
		driver.ExecOnTarget(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)

		// PostgreSQL removes no value from an enum. The type needs a recreation.
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropTypeInstruction{Name: "mood"},
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Values: []string{"sad"},
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("DropEnumType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TYPE mood AS ENUM ('sad');`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropTypeInstruction{Name: "mood"},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE DOMAIN positive_int AS integer NOT NULL DEFAULT 1 CHECK (VALUE > 0);`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("AlterDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE DOMAIN positive_int AS integer NOT NULL DEFAULT 2 CHECK (VALUE > 0);`)
		driver.ExecOnTarget(`CREATE DOMAIN positive_int AS integer DEFAULT 1;`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("RecreateDomainOnNewBaseType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE DOMAIN short_text AS varchar(10);`)
		driver.ExecOnTarget(`CREATE DOMAIN short_text AS integer;`)

		// PostgreSQL changes no base type of a domain, so the diff recreates the domain.
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropDomainInstruction{Name: "short_text"},
			&PostgresCreateDomainInstruction{
				Name:     "short_text",
				BaseType: "character varying(10)",
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("DropDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropDomainInstruction{Name: "positive_int"},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE address AS (street TEXT, city VARCHAR(10));`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("ModifyCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE address AS (street TEXT, city TEXT);`)
		driver.ExecOnTarget(`CREATE TYPE address AS (street TEXT);`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("DropCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TYPE address AS (street TEXT);`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropTypeInstruction{Name: "address"},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnSource(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("ModifyAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnSource(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)
		driver.ExecOnTarget(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '10');`)

		// PostgreSQL changes no option of an aggregate, so the diff recreates it.
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

		driver.ExecOnTarget(diff)
	})

	t.Run("DropAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnTarget(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropAggregateInstruction{
				Name:      "total",
				Arguments: "integer",
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("DropAggregateBeforeFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`)
		driver.ExecOnTarget(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("DropOperatorBeforeFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`)
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
			&PostgresDropFunctionInstruction{
				Name:      "int_add",
				Arguments: "integer, integer",
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnSource(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("ModifyOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;
			CREATE FUNCTION int_sub(integer, integer) RETURNS integer AS $$ SELECT $1 - $2; $$ LANGUAGE sql IMMUTABLE;
		`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnSource(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)
		driver.ExecOnTarget(`CREATE OPERATOR === (FUNCTION = int_sub, LEFTARG = integer, RIGHTARG = integer);`)

		// PostgreSQL changes no function of an operator, so the diff recreates it.
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

		driver.ExecOnTarget(diff)
	})

	t.Run("DropOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
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
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
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

		driver.ExecOnTarget(diff)
	})

	t.Run("ReplaceFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 2; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(a integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN a + 2; END; $function$",
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("ReplaceFunctionBodyOnly", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 3; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION increment(a integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN a + 3; END; $function$",
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("ReplaceFunctionReturnType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION calculate(a integer) RETURNS text AS $$ BEGIN RETURN a::text; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION calculate(a integer) RETURNS integer AS $$ BEGIN RETURN a; END; $$ LANGUAGE plpgsql;`)

		// PostgreSQL refuses CREATE OR REPLACE FUNCTION when the return type changes.
		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropFunctionInstruction{
				Name:      "calculate",
				Arguments: "a integer",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION calculate(a integer)\n RETURNS text\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN a::text; END; $function$",
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("DropFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropFunctionInstruction{
				Name:      "increment",
				Arguments: "a integer",
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateExtension", func(t *testing.T) {
		driver := NewTestPostgresDriverWithTwoDatabases(t)

		driver.ExecOnSource(`CREATE EXTENSION pg_trgm;`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresCreateExtensionInstruction{Name: "pg_trgm"},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("DropExtension", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE EXTENSION pg_trgm;`)

		diff := driver.RequireInstructions([]Instruction{
			&PostgresDropExtensionInstruction{Name: "pg_trgm"},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("DropTableBeforeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TYPE mood AS ENUM ('sad');
			CREATE TABLE events (id INT, feeling mood);
		`)

		// The table uses the type, so the table goes away first.
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropTableInstruction{Name: "events"},
			&PostgresDropTypeInstruction{Name: "mood"},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("DropViewBeforeTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE VIEW user_ids AS SELECT id FROM users;
		`)

		// The view uses the table, so the view goes away first.
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropViewInstruction{Name: "user_ids"},
			&SQLDropTableInstruction{Name: "users"},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("DropViewBeforeColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`
			CREATE TABLE users (id INT, label TEXT);
			CREATE VIEW user_labels AS SELECT label FROM users;
		`)

		// The view reads the column, so the view goes away before the column.
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropViewInstruction{Name: "user_labels"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []AlterTableAction{
					&SQLDropColumnAction{ColumnName: "label"},
				},
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("RecreateViewOnColumnTypeChange", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT, label VARCHAR);
			CREATE VIEW user_labels AS SELECT label FROM users;
		`)
		driver.ExecOnTarget(`
			CREATE TABLE users (id INT, label TEXT);
			CREATE VIEW user_labels AS SELECT label FROM users;
		`)

		// The definition stays equal, but the view reads a column that changes its type.
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

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateViewsInDependencyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE VIEW view_a AS SELECT id FROM view_b;
		`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)

		// view_a reads view_b, so the diff creates view_b first, against the name order.
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

		driver.ExecOnTarget(diff)
	})

	t.Run("DropViewsInDependencyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE VIEW view_a AS SELECT id FROM view_b;
		`)

		// PostgreSQL refuses to drop view_b while view_a still reads it.
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropViewInstruction{Name: "view_a"},
			&SQLDropViewInstruction{Name: "view_b"},
			&SQLDropTableInstruction{Name: "users"},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("CompareRows", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (id INT PRIMARY KEY, name TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Robert'), (3, 'Carol');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (4, 'Dave');`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Values:      []string{"3", "'Carol'"},
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

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Robert"},
			{"id": int64(3), "name": "Carol"},
		}, rows)
	})

	t.Run("CompareRowsOfATableWithoutAPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE logs (message TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO logs (message) VALUES ('start');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO logs (message) VALUES ('stop');`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLCommentInstruction{
				Text: `The table "logs" holds no primary key, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("logs", "")

		require.Equal(t, []map[string]any{
			{"message": "stop"},
		}, rows)
	})

	t.Run("CompareRowsWithAQuoteAndWithNull", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE notes (id INT PRIMARY KEY, body TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO notes (id, body) VALUES (1, 'it''s a note'), (2, NULL), (3, NULL);`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO notes (id, body) VALUES (1, 'plain'), (2, 'not empty');`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLInsertInstruction{
				TableName:   "notes",
				ColumnNames: []string{"id", "body"},
				Values:      []string{"3", "NULL"},
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

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("notes", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "body": "it's a note"},
			{"id": int64(2), "body": nil},
			{"id": int64(3), "body": nil},
		}, rows)
	})

	t.Run("CompareRowsOfATableWithAnotherPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		driver.ExecOnSource(`CREATE TABLE items (code INT PRIMARY KEY, label TEXT);`)
		driver.ExecOnSource(`INSERT INTO items (code, label) VALUES (1, 'first');`)

		// A new NOT NULL column needs an empty table, so the target holds no row here.
		driver.ExecOnTarget(`CREATE TABLE items (label TEXT);`)

		// The target holds no column with the name of the key.
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
				Text: `The table "items" holds another primary key in the target, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("items", "ORDER BY code")

		require.Equal(t, []map[string]any(nil), rows)
	})

	t.Run("CompareNoRowWithoutTheDataFlag", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `CREATE TABLE users (id INT PRIMARY KEY, name TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO users (id, name) VALUES (1, 'Alice');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO users (id, name) VALUES (2, 'Bob');`)

		diff := driver.RequireInstructions(nil)

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("TableNameThatNeedsQuotes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE "order ""list""" (id INT NOT NULL);`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("ExplicitSchema", func(t *testing.T) {
		harness := NewTestPostgresDriver(t)

		harness.ExecOnSource(`CREATE TABLE users (id INT NOT NULL);`)

		// The two connection strings hold no search path, so the config selects the schema.
		driver, err := NewPostgresDriver(&PostgresDriverConfig{
			SourceConnectionString: postgresTestConnectionString,
			TargetConnectionString: postgresTestConnectionString,
			SourceSchema:           harness.sourceSchema,
			TargetSchema:           harness.targetSchema,
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

		harness.ExecOnTarget(diff)
	})

	t.Run("UnknownSchema", func(t *testing.T) {
		driver, err := NewPostgresDriver(&PostgresDriverConfig{
			SourceConnectionString: postgresTestConnectionString,
			TargetConnectionString: postgresTestConnectionString,
			SourceSchema:           "schema_that_does_not_exist",
			TargetSchema:           "schema_that_does_not_exist",
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, driver.Close())
		})

		_, err = driver.Diff(t.Context())
		require.EqualError(t, err, `the source database has no schema with the name "schema_that_does_not_exist"`)
	})
}
