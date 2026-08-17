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

func (d *TestingPostgresDriver) RequireDiff(expectedDiff string) string {
	d.tb.Helper()

	instructions, err := d.Diff(context.Background())
	require.NoError(d.tb, err)

	diff := RenderInstructions(instructions)
	require.Equal(d.tb, expectedDiff, diff)

	return diff
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

		expected := `CREATE TABLE "simple" (
	"id" integer,
	"name" text
);`
		driver.RequireDiff(expected)
	})

	t.Run("DropTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)

		driver.RequireDiff(`DROP TABLE "users";`)
	})

	t.Run("AddColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)

		driver.RequireDiff(`ALTER TABLE "users" ADD COLUMN "name" text;`)
	})

	t.Run("DropColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT);`)

		driver.RequireDiff(`ALTER TABLE "users" DROP COLUMN "name";`)
	})

	t.Run("AlterColumnType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name VARCHAR(50));`)

		driver.RequireDiff(`ALTER TABLE "users" ALTER COLUMN "name" TYPE text;`)
	})

	t.Run("AlterColumnTypeWithAutomaticCast", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, score BIGINT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, score INT);`)
		driver.ExecOnTarget(`INSERT INTO users (id, score) VALUES (1, 42);`)

		// PostgreSQL casts an integer to a bigint, so the statement needs no USING clause.
		diff := driver.RequireDiff(`ALTER TABLE "users" ALTER COLUMN "score" TYPE bigint;`)

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
		diff := driver.RequireDiff(`ALTER TABLE "users" ALTER COLUMN "score" TYPE integer USING "score"::integer;`)

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "score": int64(42)},
		}, rows)
	})

	t.Run("CreateTableWithArrayColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE tags (id INT, labels TEXT[]);`)

		diff := driver.RequireDiff(`CREATE TABLE "tags" (
	"id" integer,
	"labels" text[]
);`)

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateTableWithEnumColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`
			CREATE TYPE mood AS ENUM ('sad', 'ok');
			CREATE TABLE users (id INT, mood mood);
		`)

		diff := driver.RequireDiff(`CREATE TYPE "mood" AS ENUM ('sad', 'ok');
CREATE TABLE "users" (
	"id" integer,
	"mood" mood
);`)

		driver.ExecOnTarget(diff)
	})

	t.Run("AlterColumnTypeToArray", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, tags BIGINT[]);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, tags INT[]);`)
		driver.ExecOnTarget(`INSERT INTO users (id, tags) VALUES (1, ARRAY[5, 6]);`)

		// information_schema.columns gives the text ARRAY for both types. format_type gives
		// the exact type name, so the statement below is valid SQL.
		diff := driver.RequireDiff(`ALTER TABLE "users" ALTER COLUMN "tags" TYPE bigint[] USING "tags"::bigint[];`)

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

		driver.RequireDiff(`ALTER TABLE "users" ALTER COLUMN "name" SET NOT NULL;`)
	})

	t.Run("AlterColumnDefault", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT DEFAULT 'anon');`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, name TEXT);`)

		driver.RequireDiff(`ALTER TABLE "users" ALTER COLUMN "name" SET DEFAULT 'anon'::text;`)
	})

	t.Run("ConstraintsPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT PRIMARY KEY);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)

		driver.ExecOnSource(`DROP TABLE users; CREATE TABLE users (id INT, CONSTRAINT pk_users PRIMARY KEY (id));`)

		driver.RequireDiff("ALTER TABLE \"users\" ALTER COLUMN \"id\" SET NOT NULL;\nALTER TABLE \"users\" ADD CONSTRAINT \"pk_users\" PRIMARY KEY (id);")
	})

	t.Run("ConstraintsUnique", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (email TEXT, CONSTRAINT uq_email UNIQUE (email));`)
		driver.ExecOnTarget(`CREATE TABLE users (email TEXT);`)

		driver.RequireDiff(`ALTER TABLE "users" ADD CONSTRAINT "uq_email" UNIQUE (email);`)
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

		driver.RequireDiff(`ALTER TABLE "users" ADD CONSTRAINT "fk_role" FOREIGN KEY (role_id) REFERENCES roles(id);`)
	})

	t.Run("DropColumnWithPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, code INT, CONSTRAINT pk_users PRIMARY KEY (code));`)

		// PostgreSQL drops the constraint with the column, so its drop comes first.
		diff := driver.RequireDiff(`ALTER TABLE "users" DROP CONSTRAINT "pk_users";
ALTER TABLE "users" DROP COLUMN "code";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropColumnWithUniqueConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, email TEXT, CONSTRAINT uq_email UNIQUE (email));`)

		diff := driver.RequireDiff(`ALTER TABLE "users" DROP CONSTRAINT "uq_email";
ALTER TABLE "users" DROP COLUMN "email";`)

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
		diff := driver.RequireDiff(`ALTER TABLE "users" DROP CONSTRAINT "uq_email";
ALTER TABLE "users" DROP COLUMN "email";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropColumnOfCompositeConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, CONSTRAINT uq_users UNIQUE (id));`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, email TEXT, CONSTRAINT uq_users UNIQUE (id, email));`)

		diff := driver.RequireDiff(`ALTER TABLE "users" DROP CONSTRAINT "uq_users";
ALTER TABLE "users" ADD CONSTRAINT "uq_users" UNIQUE (id);
ALTER TABLE "users" DROP COLUMN "email";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("Indexes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (name TEXT); CREATE INDEX idx_name ON users(name);`)
		driver.ExecOnTarget(`CREATE TABLE users (name TEXT);`)

		diff := driver.RequireDiff(`CREATE INDEX idx_name ON users USING btree (name);`)

		driver.ExecOnTarget(diff)
	})

	t.Run("EqualIndexes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `CREATE TABLE users (name TEXT); CREATE INDEX idx_name ON users(name);`
		driver.ExecOnSource(schema)
		driver.ExecOnTarget(schema)

		driver.RequireDiff("")
	})

	t.Run("DropColumnDropsItsIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, email TEXT); CREATE INDEX idx_email ON users(email);`)

		// A DROP COLUMN statement drops every index of that column, so the DROP INDEX
		// statement must print first.
		diff := driver.RequireDiff(`DROP INDEX "idx_email";
ALTER TABLE "users" DROP COLUMN "email";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropColumnKeepsAnotherIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT); CREATE INDEX idx_name ON users(name);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, email TEXT, name TEXT); CREATE INDEX idx_email ON users(email);`)

		diff := driver.RequireDiff(`CREATE INDEX idx_name ON users USING btree (name);
DROP INDEX "idx_email";
ALTER TABLE "users" DROP COLUMN "email";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropColumnAndModifyAnotherIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT, name TEXT); CREATE UNIQUE INDEX idx_name ON users(name);`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT, email TEXT, name TEXT); CREATE INDEX idx_email ON users(email); CREATE INDEX idx_name ON users(name);`)

		// The two statements of the modified index must stay adjacent.
		diff := driver.RequireDiff(`DROP INDEX "idx_name";
CREATE UNIQUE INDEX idx_name ON users USING btree (name);
DROP INDEX "idx_email";
ALTER TABLE "users" DROP COLUMN "email";`)

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

		diff := driver.RequireDiff(`CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();`)

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

		driver.RequireDiff("")
	})

	t.Run("Views", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT); CREATE VIEW user_ids AS SELECT id FROM users;`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)

		driver.RequireDiff(`CREATE VIEW "user_ids" AS  SELECT id
   FROM users;`)
	})

	t.Run("CreateSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter;`)

		diff := driver.RequireDiff(`CREATE SEQUENCE "counter" AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 NO CYCLE;`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE SEQUENCE counter;`)

		diff := driver.RequireDiff(`DROP SEQUENCE "counter";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("AlterSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter INCREMENT BY 2 MAXVALUE 100 CYCLE;`)
		driver.ExecOnTarget(`CREATE SEQUENCE counter;`)

		diff := driver.RequireDiff(`ALTER SEQUENCE "counter" INCREMENT BY 2 MAXVALUE 100 CYCLE;`)

		driver.ExecOnTarget(diff)
	})

	t.Run("SequenceRestartOnHigherMinimum", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter MINVALUE 100 START WITH 100;`)
		driver.ExecOnTarget(`CREATE SEQUENCE counter;`)

		// The current value stays below the new minimum, so MINVALUE needs a RESTART.
		driver.ExecOnTarget(`SELECT nextval('counter');`)

		diff := driver.RequireDiff(`ALTER SEQUENCE "counter" MINVALUE 100 START WITH 100 RESTART WITH 100;`)

		driver.ExecOnTarget(diff)
	})

	t.Run("SequenceRestartOnLowerMaximum", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter MAXVALUE 5;`)
		driver.ExecOnTarget(`CREATE SEQUENCE counter;`)

		// The current value climbs above the new maximum, so MAXVALUE needs a RESTART.
		driver.ExecOnTarget(`SELECT nextval('counter') FROM generate_series(1, 10);`)

		diff := driver.RequireDiff(`ALTER SEQUENCE "counter" MAXVALUE 5 RESTART WITH 1;`)

		driver.ExecOnTarget(diff)
	})

	t.Run("SequenceNoRestartWithinRange", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE SEQUENCE counter INCREMENT BY 2 MAXVALUE 100 CYCLE;`)
		driver.ExecOnTarget(`CREATE SEQUENCE counter;`)

		// The current value stays inside the new range, so the diff holds no RESTART.
		driver.ExecOnTarget(`SELECT nextval('counter');`)

		diff := driver.RequireDiff(`ALTER SEQUENCE "counter" INCREMENT BY 2 MAXVALUE 100 CYCLE;`)

		driver.ExecOnTarget(diff)
	})

	t.Run("SequenceOfSerialColumnIsIgnored", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id SERIAL);`)

		// The table creates its own sequence. The diff holds the table only.
		driver.RequireDiff(`CREATE TABLE "users" (
	"id" integer NOT NULL DEFAULT nextval('users_id_seq'::regclass)
);`)
	})

	t.Run("CreateEnumType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)

		diff := driver.RequireDiff(`CREATE TYPE "mood" AS ENUM ('sad', 'ok');`)

		driver.ExecOnTarget(diff)
	})

	t.Run("AddEnumValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');`)
		driver.ExecOnTarget(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)

		diff := driver.RequireDiff(`ALTER TYPE "mood" ADD VALUE 'happy';`)

		driver.ExecOnTarget(diff)
	})

	t.Run("RemoveEnumValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE mood AS ENUM ('sad');`)
		driver.ExecOnTarget(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)

		// PostgreSQL removes no value from an enum. The type needs a recreation.
		diff := driver.RequireDiff(`DROP TYPE "mood";
CREATE TYPE "mood" AS ENUM ('sad');`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropEnumType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TYPE mood AS ENUM ('sad');`)

		diff := driver.RequireDiff(`DROP TYPE "mood";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE DOMAIN positive_int AS integer NOT NULL DEFAULT 1 CHECK (VALUE > 0);`)

		diff := driver.RequireDiff(`CREATE DOMAIN "positive_int" AS integer DEFAULT 1 NOT NULL CONSTRAINT "positive_int_check" CHECK ((VALUE > 0));`)

		driver.ExecOnTarget(diff)
	})

	t.Run("AlterDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE DOMAIN positive_int AS integer NOT NULL DEFAULT 2 CHECK (VALUE > 0);`)
		driver.ExecOnTarget(`CREATE DOMAIN positive_int AS integer DEFAULT 1;`)

		diff := driver.RequireDiff(`ALTER DOMAIN "positive_int" SET DEFAULT 2;
ALTER DOMAIN "positive_int" SET NOT NULL;
ALTER DOMAIN "positive_int" ADD CONSTRAINT "positive_int_check" CHECK ((VALUE > 0));`)

		driver.ExecOnTarget(diff)
	})

	t.Run("RecreateDomainOnNewBaseType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE DOMAIN short_text AS varchar(10);`)
		driver.ExecOnTarget(`CREATE DOMAIN short_text AS integer;`)

		// PostgreSQL changes no base type of a domain, so the diff recreates the domain.
		diff := driver.RequireDiff(`DROP DOMAIN "short_text";
CREATE DOMAIN "short_text" AS character varying(10);`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);`)

		diff := driver.RequireDiff(`DROP DOMAIN "positive_int";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE address AS (street TEXT, city VARCHAR(10));`)

		diff := driver.RequireDiff(`CREATE TYPE "address" AS (
	"street" text,
	"city" character varying(10)
);`)

		driver.ExecOnTarget(diff)
	})

	t.Run("ModifyCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TYPE address AS (street TEXT, city TEXT);`)
		driver.ExecOnTarget(`CREATE TYPE address AS (street TEXT);`)

		diff := driver.RequireDiff(`DROP TYPE "address";
CREATE TYPE "address" AS (
	"street" text,
	"city" text
);`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE TYPE address AS (street TEXT);`)

		diff := driver.RequireDiff(`DROP TYPE "address";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnSource(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)

		diff := driver.RequireDiff(`CREATE AGGREGATE "total"(integer) (SFUNC = "int_add", STYPE = integer, INITCOND = '0');`)

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
		diff := driver.RequireDiff(`DROP AGGREGATE "total"(integer);
CREATE AGGREGATE "total"(integer) (SFUNC = "int_add", STYPE = integer, INITCOND = '0');`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropAggregate", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnTarget(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)

		diff := driver.RequireDiff(`DROP AGGREGATE "total"(integer);`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropAggregateBeforeFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`)
		driver.ExecOnTarget(`CREATE AGGREGATE total(integer) (SFUNC = int_add, STYPE = integer, INITCOND = '0');`)

		diff := driver.RequireDiff(`DROP AGGREGATE "total"(integer);
DROP FUNCTION "int_add"(integer, integer);`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropOperatorBeforeFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`)
		driver.ExecOnTarget(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)

		diff := driver.RequireDiff(`DROP OPERATOR === (integer, integer);
DROP FUNCTION "int_add"(integer, integer);`)

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnSource(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)

		diff := driver.RequireDiff(`CREATE OPERATOR === (FUNCTION = "int_add", LEFTARG = integer, RIGHTARG = integer);`)

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
		diff := driver.RequireDiff(`DROP OPERATOR === (integer, integer);
CREATE OPERATOR === (FUNCTION = "int_add", LEFTARG = integer, RIGHTARG = integer);`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropOperator", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `CREATE FUNCTION int_add(integer, integer) RETURNS integer AS $$ SELECT $1 + $2; $$ LANGUAGE sql IMMUTABLE;`
		driver.ExecOnSource(setup)
		driver.ExecOnTarget(setup)

		driver.ExecOnTarget(`CREATE OPERATOR === (FUNCTION = int_add, LEFTARG = integer, RIGHTARG = integer);`)

		diff := driver.RequireDiff(`DROP OPERATOR === (integer, integer);`)

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

		diff := driver.RequireDiff(`CREATE OR REPLACE FUNCTION increment(a integer)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
			BEGIN
				RETURN a + 1;
			END;
			$function$;`)

		driver.ExecOnTarget(diff)
	})

	t.Run("ReplaceFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 2; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)

		diff := driver.RequireDiff(`CREATE OR REPLACE FUNCTION increment(a integer)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$ BEGIN RETURN a + 2; END; $function$;`)

		driver.ExecOnTarget(diff)
	})

	t.Run("ReplaceFunctionBodyOnly", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 3; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)

		diff := driver.RequireDiff(`CREATE OR REPLACE FUNCTION increment(a integer)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$ BEGIN RETURN a + 3; END; $function$;`)

		driver.ExecOnTarget(diff)
	})

	t.Run("ReplaceFunctionReturnType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE FUNCTION calculate(a integer) RETURNS text AS $$ BEGIN RETURN a::text; END; $$ LANGUAGE plpgsql;`)
		driver.ExecOnTarget(`CREATE FUNCTION calculate(a integer) RETURNS integer AS $$ BEGIN RETURN a; END; $$ LANGUAGE plpgsql;`)

		// PostgreSQL refuses CREATE OR REPLACE FUNCTION when the return type changes.
		diff := driver.RequireDiff(`DROP FUNCTION "calculate"(a integer);
CREATE OR REPLACE FUNCTION calculate(a integer)
 RETURNS text
 LANGUAGE plpgsql
AS $function$ BEGIN RETURN a::text; END; $function$;`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropFunction", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE FUNCTION increment(a integer) RETURNS integer AS $$ BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql;`)

		diff := driver.RequireDiff(`DROP FUNCTION "increment"(a integer);`)

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateExtension", func(t *testing.T) {
		driver := NewTestPostgresDriverWithTwoDatabases(t)

		driver.ExecOnSource(`CREATE EXTENSION pg_trgm;`)

		diff := driver.RequireDiff(`CREATE EXTENSION "pg_trgm";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropExtension", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`CREATE EXTENSION pg_trgm;`)

		diff := driver.RequireDiff(`DROP EXTENSION "pg_trgm";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropTableBeforeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TYPE mood AS ENUM ('sad');
			CREATE TABLE events (id INT, feeling mood);
		`)

		// The table uses the type, so the table goes away first.
		diff := driver.RequireDiff(`DROP TABLE "events";
DROP TYPE "mood";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropViewBeforeTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INT);
			CREATE VIEW user_ids AS SELECT id FROM users;
		`)

		// The view uses the table, so the view goes away first.
		diff := driver.RequireDiff(`DROP VIEW "user_ids";
DROP TABLE "users";`)

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
		diff := driver.RequireDiff(`DROP VIEW "user_labels";
ALTER TABLE "users" DROP COLUMN "label";`)

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
		diff := driver.RequireDiff(`DROP VIEW "user_labels";
ALTER TABLE "users" ALTER COLUMN "label" TYPE character varying;
CREATE VIEW "user_labels" AS  SELECT label
   FROM users;`)

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
		diff := driver.RequireDiff(`CREATE VIEW "view_b" AS  SELECT id
   FROM users;
CREATE VIEW "view_a" AS  SELECT id
   FROM view_b;`)

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
		diff := driver.RequireDiff(`DROP VIEW "view_a";
DROP VIEW "view_b";
DROP TABLE "users";`)

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

		expected := `INSERT INTO "users" ("id", "name") VALUES (3, 'Carol');
UPDATE "users" SET "name" = 'Robert' WHERE "id" = 2;
DELETE FROM "users" WHERE "id" = 4;`

		diff := driver.RequireDiff(expected)

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

		diff := driver.RequireDiff(`-- The table "logs" holds no primary key, so dbdiff compares no row of it.`)

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

		expected := `INSERT INTO "notes" ("id", "body") VALUES (3, NULL);
UPDATE "notes" SET "body" = 'it''s a note' WHERE "id" = 1;
UPDATE "notes" SET "body" = NULL WHERE "id" = 2;`

		diff := driver.RequireDiff(expected)

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
		expected := `ALTER TABLE "items" ADD COLUMN "code" integer NOT NULL;
ALTER TABLE "items" ADD CONSTRAINT "items_pkey" PRIMARY KEY (code);
-- The table "items" holds another primary key in the target, so dbdiff compares no row of it.`

		diff := driver.RequireDiff(expected)

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

		diff := driver.RequireDiff("")

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("TableNameThatNeedsQuotes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE "order ""list""" (id INT NOT NULL);`)

		diff := driver.RequireDiff(`CREATE TABLE "order ""list""" (
	"id" integer NOT NULL
);`)

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
