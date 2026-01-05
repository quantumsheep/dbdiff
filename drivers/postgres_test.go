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

type TestingPostgresDriver struct {
	*PostgresDriver
	tb           testing.TB
	conn         *sql.DB
	sourceSchema string
	targetSchema string
}

func NewTestPostgresDriver(tb testing.TB) *TestingPostgresDriver {
	tb.Helper()

	dsn := "postgres://user:password@localhost:5432/dbdiff?sslmode=disable"
	conn, err := sql.Open("pgx", dsn)
	require.NoError(tb, err)

	// Create unique schemas
	id := time.Now().UnixNano()
	sourceSchema := fmt.Sprintf("source_%d", id)
	targetSchema := fmt.Sprintf("target_%d", id)

	_, err = conn.ExecContext(tb.Context(), fmt.Sprintf("CREATE SCHEMA %s", sourceSchema))
	require.NoError(tb, err)
	_, err = conn.ExecContext(tb.Context(), fmt.Sprintf("CREATE SCHEMA %s", targetSchema))
	require.NoError(tb, err)

	tb.Cleanup(func() {
		conn.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", sourceSchema))
		conn.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", targetSchema))
		conn.Close()
	})

	sourceDSN := fmt.Sprintf("%s&search_path=%s", dsn, sourceSchema)
	targetDSN := fmt.Sprintf("%s&search_path=%s", dsn, targetSchema)

	driver, err := NewPostgresDriver(&PostgresDriverConfig{
		SourceConnectionString: sourceDSN,
		TargetConnectionString: targetDSN,
	})
	require.NoError(tb, err)

	tb.Cleanup(func() {
		driver.Close()
	})

	return &TestingPostgresDriver{
		PostgresDriver: driver,
		tb:             tb,
		conn:           conn,
		sourceSchema:   sourceSchema,
		targetSchema:   targetSchema,
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

	diff, err := d.Diff(context.Background())
	require.NoError(d.tb, err)
	require.Equal(d.tb, expectedDiff, diff)

	return diff
}

func TestPostgresDriver(t *testing.T) {
	t.Run("CreateTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		// Let's use simple types first.
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

	t.Run("Indexes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (name TEXT); CREATE INDEX idx_name ON users(name);`)
		driver.ExecOnTarget(`CREATE TABLE users (name TEXT);`)

		driver.RequireDiff(`CREATE INDEX idx_name ON ` + driver.sourceSchema + `.users USING btree (name);`)
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

		driver.RequireDiff(fmt.Sprintf(`CREATE TRIGGER set_timestamp BEFORE UPDATE ON %s.users FOR EACH ROW EXECUTE FUNCTION update_timestamp();`, driver.sourceSchema))
	})

	t.Run("Views", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INT); CREATE VIEW user_ids AS SELECT id FROM users;`)
		driver.ExecOnTarget(`CREATE TABLE users (id INT);`)

		driver.RequireDiff(`CREATE VIEW "user_ids" AS  SELECT id
   FROM users;`)
	})
}
