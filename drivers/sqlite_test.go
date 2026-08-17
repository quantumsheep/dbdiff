package drivers

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

type TestingSQLiteDriver struct {
	*SQLiteDriver

	tb testing.TB
}

func (d *TestingSQLiteDriver) Close() error {
	d.tb.Helper()
	return d.SQLiteDriver.Close()
}

func (d *TestingSQLiteDriver) ExecOnSource(sqlStatements string) {
	d.tb.Helper()

	_, err := d.SourceDatabaseConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingSQLiteDriver) ExecOnTarget(sqlStatements string) {
	d.tb.Helper()

	_, err := d.TargetDatabaseConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingSQLiteDriver) RequireDiff(expectedDiff string) string {
	d.tb.Helper()

	diff, err := d.Diff(d.tb.Context())
	require.NoError(d.tb, err)
	require.Equal(d.tb, expectedDiff, diff)

	return diff
}

func (d *TestingSQLiteDriver) FetchAllFromTarget(table string, additionalRules string) []map[string]any {
	d.tb.Helper()

	columns, err := d.GetTableColumns(d.tb.Context(), d.TargetDatabaseConnection, table)
	require.NoError(d.tb, err)

	rows, err := d.TargetDatabaseConnection.Query(fmt.Sprintf("SELECT * FROM %q %s;", table, additionalRules))
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

		for i, col := range columns {
			row[col.Name] = columnValues[i]
		}

		results = append(results, row)
	}

	require.NoError(d.tb, rows.Err())

	return results
}

func NewTestSQLiteDriver(tb testing.TB) *TestingSQLiteDriver {
	tb.Helper()

	sourceDatabasePath := filepath.Join(tb.TempDir(), "source.sqlite")
	targetDatabasePath := filepath.Join(tb.TempDir(), "target.sqlite")

	driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
		SourceDatabasePath: sourceDatabasePath,
		TargetDatabasePath: targetDatabasePath,
	})
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, driver.Close())
	})

	return &TestingSQLiteDriver{
		SQLiteDriver: driver,
		tb:           tb,
	}
}

// errRowIteration is the error that failingRows returns after it exhausts its rows. A
// read method must return this error through rows.Err.
var errRowIteration = errors.New("row iteration failed")

// failingRows is a driver.Rows value that yields a fixed set of rows, then fails. It
// simulates a connection that breaks in the middle of a read.
type failingRows struct {
	columns  []string
	rows     [][]driver.Value
	position int
}

func (r *failingRows) Columns() []string {
	return r.columns
}

func (r *failingRows) Close() error {
	return nil
}

func (r *failingRows) Next(dest []driver.Value) error {
	if r.position >= len(r.rows) {
		return errRowIteration
	}

	copy(dest, r.rows[r.position])
	r.position++

	return nil
}

// failingStmt is a driver.Stmt value. Its Query method returns a failingRows value for
// every query, no matter the query text.
type failingStmt struct {
	columns []string
	rows    [][]driver.Value
}

func (s *failingStmt) Close() error {
	return nil
}

func (s *failingStmt) NumInput() int {
	return -1
}

func (s *failingStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("failingStmt does not support Exec")
}

func (s *failingStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &failingRows{columns: s.columns, rows: s.rows}, nil
}

// failingConn is a driver.Conn value. It prepares a failingStmt value for every query.
type failingConn struct {
	columns []string
	rows    [][]driver.Value
}

func (c *failingConn) Prepare(query string) (driver.Stmt, error) {
	return &failingStmt{columns: c.columns, rows: c.rows}, nil
}

func (c *failingConn) Close() error {
	return nil
}

func (c *failingConn) Begin() (driver.Tx, error) {
	return nil, errors.New("failingConn does not support Begin")
}

// failingDriver is a driver.Driver value. It opens a connection that fails in the
// middle of a read, after it yields the given rows.
type failingDriver struct {
	columns []string
	rows    [][]driver.Value
}

func (d *failingDriver) Open(name string) (driver.Conn, error) {
	return &failingConn{columns: d.columns, rows: d.rows}, nil
}

// init registers failingDriver one time. A second call to sql.Register with the same
// name panics, and go test can load this package one time only per process.
func init() {
	sql.Register("sqlite3_failing_rows", &failingDriver{
		columns: []string{"cid", "name", "type", "notnull", "dflt_value", "pk"},
		rows: [][]driver.Value{
			{int64(0), "id", "INTEGER", int64(0), nil, int64(1)},
		},
	})
}

func TestSQLiteDriver(t *testing.T) {
	t.Run("NoChanges", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.RequireDiff(``)
	})

	t.Run("CreateTables", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		diff := driver.RequireDiff(`CREATE TABLE "users" (
	"id" INTEGER PRIMARY KEY,
	"name" TEXT NOT NULL
);`)

		driver.ExecOnTarget(diff)
	})

	t.Run("AddColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		diff := driver.RequireDiff(`ALTER TABLE "users" ADD COLUMN "email" TEXT;`)

		// Check that data is preserved after applying the diff
		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "email": nil},
			{"id": int64(2), "name": "Bob", "email": nil},
		}, rows)
	})

	t.Run("RemoveColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);

			INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com');
		`)

		diff := driver.RequireDiff(`ALTER TABLE "users" DROP COLUMN "email";`)

		// Check that data is preserved after applying the diff
		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("RenameColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				full_name TEXT NOT NULL
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		diff := driver.RequireDiff(`ALTER TABLE "users" RENAME COLUMN "name" TO "full_name";`)

		// Check that data is preserved after applying the diff
		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "full_name": "Alice"},
			{"id": int64(2), "full_name": "Bob"},
		}, rows)
	})

	t.Run("RenameColumnWithSeveralCandidates", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				first_name TEXT,
				last_name TEXT
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name_a TEXT,
				name_b TEXT
			);
		`)

		// Two target columns have the same attributes. A rename is a guess, so the
		// diff removes the old columns and adds the new ones.
		diff := driver.RequireDiff(`ALTER TABLE "users" DROP COLUMN "name_a";
ALTER TABLE "users" DROP COLUMN "name_b";
ALTER TABLE "users" ADD COLUMN "first_name" TEXT;
ALTER TABLE "users" ADD COLUMN "last_name" TEXT;`)

		driver.ExecOnTarget(diff)
	})

	t.Run("RenameColumnTakesOneCandidateOnly", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				first_name TEXT,
				last_name TEXT
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name_a TEXT
			);

			INSERT INTO users (id, name_a) VALUES (1, 'Alice');
		`)

		diff := driver.RequireDiff(`ALTER TABLE "users" RENAME COLUMN "name_a" TO "first_name";
ALTER TABLE "users" ADD COLUMN "last_name" TEXT;`)

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "first_name": "Alice", "last_name": nil},
		}, rows)
	})

	t.Run("ModifyColumnType", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				age INTEGER
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				age TEXT
			);

			INSERT INTO users (id, name, age) VALUES (1, 'Alice', '30'), (2, 'Bob', '25');
		`)

		diff := driver.RequireDiff(`CREATE TABLE "_users_temp" (
	"id" INTEGER PRIMARY KEY,
	"name" TEXT NOT NULL,
	"age" INTEGER
);
INSERT INTO "_users_temp" ("id", "name", "age") SELECT "id", "name", "age" FROM "users";
DROP TABLE "users";
ALTER TABLE "_users_temp" RENAME TO "users";`)

		// Check that data is preserved after applying the diff
		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "age": int64(30)},
			{"id": int64(2), "name": "Bob", "age": int64(25)},
		}, rows)
	})

	t.Run("ModifyColumnSetNotNull", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		diff := driver.RequireDiff(`CREATE TABLE "_users_temp" (
	"id" INTEGER PRIMARY KEY,
	"name" TEXT NOT NULL
);
INSERT INTO "_users_temp" ("id", "name") SELECT "id", "name" FROM "users";
DROP TABLE "users";
ALTER TABLE "_users_temp" RENAME TO "users";`)

		// Check that data is preserved after applying the diff
		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("ModifyColumnDropNotNull", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		diff := driver.RequireDiff(`CREATE TABLE "_users_temp" (
	"id" INTEGER PRIMARY KEY,
	"name" TEXT
);
INSERT INTO "_users_temp" ("id", "name") SELECT "id", "name" FROM "users";
DROP TABLE "users";
ALTER TABLE "_users_temp" RENAME TO "users";`)

		// Check that data is preserved after applying the diff
		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("DropTables", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.RequireDiff(`DROP TABLE "users";`)
	})

	t.Run("TableNameThatNeedsQuotes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE "order ""list""" (
				id INTEGER PRIMARY KEY,
				name TEXT
			);
			CREATE INDEX "idx name" ON "order ""list""" (name);
		`)

		diff := driver.RequireDiff(`CREATE TABLE "order ""list""" (
	"id" INTEGER PRIMARY KEY,
	"name" TEXT
);
CREATE INDEX "idx name" ON "order ""list""" ("name");`)

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateIndexes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		diff := driver.RequireDiff(`CREATE UNIQUE INDEX "idx_users_name" ON "users" ("name");`)

		driver.ExecOnTarget(diff)
	})

	t.Run("DropIndexes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);
		`)

		diff := driver.RequireDiff(`DROP INDEX "idx_users_name";`)

		driver.ExecOnTarget(diff)
	})

	t.Run("ModifyIndexes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name, email);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);

			INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com');
		`)

		diff := driver.RequireDiff(`DROP INDEX "idx_users_name";
CREATE UNIQUE INDEX "idx_users_name" ON "users" ("name", "email");`)

		driver.ExecOnTarget(diff)
	})

	t.Run("CreatePartialIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
		`)

		diff := driver.RequireDiff(`CREATE INDEX "idx_users_active" ON "users" ("name") WHERE active = 1;`)

		driver.ExecOnTarget(diff)
	})

	t.Run("ModifyPartialIndexCondition", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 0;
		`)

		diff := driver.RequireDiff(`DROP INDEX "idx_users_active";
CREATE INDEX "idx_users_active" ON "users" ("name") WHERE active = 1;`)

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateExpressionIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (lower(name), id);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		diff := driver.RequireDiff(`CREATE UNIQUE INDEX "idx_users_name" ON "users" (lower(name), "id");`)

		driver.ExecOnTarget(diff)
	})

	t.Run("ModifyExpressionIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE INDEX idx_users_name ON users (lower(name));
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE INDEX idx_users_name ON users (upper(name));
		`)

		diff := driver.RequireDiff(`DROP INDEX "idx_users_name";
CREATE INDEX "idx_users_name" ON "users" (lower(name));`)

		driver.ExecOnTarget(diff)
	})

	t.Run("RecreateTableWithPartialIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;

			INSERT INTO users (id, name, active) VALUES (1, 'Alice', 1), (2, 'Bob', 0);
		`)

		// The recreation of the table drops the index. The last statement builds it again.
		diff := driver.RequireDiff(`CREATE TABLE "_users_temp" (
	"id" INTEGER PRIMARY KEY,
	"name" TEXT NOT NULL,
	"active" INTEGER NOT NULL
);
INSERT INTO "_users_temp" ("id", "name", "active") SELECT "id", "name", "active" FROM "users";
DROP TABLE "users";
ALTER TABLE "_users_temp" RENAME TO "users";
CREATE INDEX "idx_users_active" ON "users" ("name") WHERE active = 1;`)

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "active": int64(1)},
			{"id": int64(2), "name": "Bob", "active": int64(0)},
		}, rows)
	})

	t.Run("ImplicitUniqueIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				name TEXT
			);
			CREATE INDEX idx_users_name ON users (name);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				name TEXT
			);
		`)

		// The UNIQUE constraint builds an index. That index stays out of the diff.
		diff := driver.RequireDiff(`CREATE INDEX "idx_users_name" ON "users" ("name");`)

		driver.ExecOnTarget(diff)
	})

	t.Run("RecreateTableWithUniqueColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				age INTEGER
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				age TEXT
			);

			INSERT INTO users (id, email, age) VALUES (1, 'alice@example.com', '30'), (2, 'bob@example.com', '25');
		`)

		// The UNIQUE constraint builds an index of the origin "u". The recreation of the
		// table keeps the constraint in the definition of the column. It must not print a
		// CREATE INDEX statement for that index, because SQLite refuses a CREATE INDEX
		// statement with the name of such an index.
		diff := driver.RequireDiff(`CREATE TABLE "_users_temp" (
	"id" INTEGER PRIMARY KEY,
	"email" TEXT UNIQUE,
	"age" INTEGER
);
INSERT INTO "_users_temp" ("id", "email", "age") SELECT "id", "email", "age" FROM "users";
DROP TABLE "users";
ALTER TABLE "_users_temp" RENAME TO "users";`)

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "email": "alice@example.com", "age": int64(30)},
			{"id": int64(2), "email": "bob@example.com", "age": int64(25)},
		}, rows)

		// The new table refuses a second row with the email of the first row.
		_, err := driver.TargetDatabaseConnection.Exec(`INSERT INTO users (id, email, age) VALUES (3, 'alice@example.com', 40);`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: users.email")
	})

	t.Run("CreateTableWithUniqueConstraint", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team, name)
			);
		`)

		// A UNIQUE constraint of two or more columns is a table constraint. It keeps the
		// order of the columns of the constraint.
		diff := driver.RequireDiff(`CREATE TABLE "members" (
	"id" INTEGER PRIMARY KEY,
	"team" TEXT NOT NULL,
	"name" TEXT NOT NULL,
	UNIQUE ("team", "name")
);`)

		driver.ExecOnTarget(diff)
		driver.ExecOnTarget(`INSERT INTO members (id, team, name) VALUES (1, 'red', 'Alice');`)

		_, err := driver.TargetDatabaseConnection.Exec(`INSERT INTO members (id, team, name) VALUES (2, 'red', 'Alice');`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: members.team, members.name")
	})

	t.Run("AddUniqueConstraint", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team, name)
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL
			);

			INSERT INTO members (id, team, name) VALUES (1, 'red', 'Alice'), (2, 'blue', 'Bob');
		`)

		// SQLite adds no table constraint to a table, so the new constraint needs a
		// recreation of the table.
		diff := driver.RequireDiff(`CREATE TABLE "_members_temp" (
	"id" INTEGER PRIMARY KEY,
	"team" TEXT NOT NULL,
	"name" TEXT NOT NULL,
	UNIQUE ("team", "name")
);
INSERT INTO "_members_temp" ("id", "team", "name") SELECT "id", "team", "name" FROM "members";
DROP TABLE "members";
ALTER TABLE "_members_temp" RENAME TO "members";`)

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("members", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "team": "red", "name": "Alice"},
			{"id": int64(2), "team": "blue", "name": "Bob"},
		}, rows)
	})

	t.Run("CreateTableWithCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT,
				PRIMARY KEY (member, team)
			);
		`)

		// A primary key of two or more columns is a table constraint. It keeps the order of
		// the key, and that order differs from the order of the columns of the table.
		diff := driver.RequireDiff(`CREATE TABLE "memberships" (
	"team" TEXT NOT NULL,
	"member" TEXT NOT NULL,
	"role" TEXT,
	PRIMARY KEY ("member", "team")
);`)

		driver.ExecOnTarget(diff)
		driver.ExecOnTarget(`INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'lead');`)

		_, err := driver.TargetDatabaseConnection.Exec(`INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'guest');`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: memberships.member, memberships.team")
	})

	t.Run("CreateTableWithIntegerPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE counters (
				id INTEGER PRIMARY KEY,
				total INTEGER
			);
		`)

		// An INTEGER PRIMARY KEY is the alias of the rowid of SQLite. It keeps the form of a
		// column constraint, because the form of a table constraint changes the type of the
		// key.
		diff := driver.RequireDiff(`CREATE TABLE "counters" (
	"id" INTEGER PRIMARY KEY,
	"total" INTEGER
);`)

		driver.ExecOnTarget(diff)
		driver.ExecOnTarget(`INSERT INTO counters (total) VALUES (5);`)

		rows := driver.FetchAllFromTarget("counters", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "total": int64(5)},
		}, rows)
	})

	t.Run("RecreateTableWithCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				level INTEGER,
				PRIMARY KEY (team, member)
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				level TEXT,
				PRIMARY KEY (team, member)
			);

			INSERT INTO memberships (team, member, level) VALUES ('red', 'Alice', '3'), ('blue', 'Bob', '1');
		`)

		// The type of the column "level" changes, so the driver recreates the table. The new
		// table keeps the whole primary key.
		diff := driver.RequireDiff(`CREATE TABLE "_memberships_temp" (
	"team" TEXT NOT NULL,
	"member" TEXT NOT NULL,
	"level" INTEGER,
	PRIMARY KEY ("team", "member")
);
INSERT INTO "_memberships_temp" ("team", "member", "level") SELECT "team", "member", "level" FROM "memberships";
DROP TABLE "memberships";
ALTER TABLE "_memberships_temp" RENAME TO "memberships";`)

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("memberships", "ORDER BY team, member")

		require.Equal(t, []map[string]any{
			{"team": "blue", "member": "Bob", "level": int64(1)},
			{"team": "red", "member": "Alice", "level": int64(3)},
		}, rows)
	})

	t.Run("ModifyCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT NOT NULL,
				PRIMARY KEY (team, member)
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT NOT NULL,
				PRIMARY KEY (team, role)
			);

			INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'lead');
		`)

		// The primary key of the target holds another column, so the driver recreates the
		// table.
		diff := driver.RequireDiff(`CREATE TABLE "_memberships_temp" (
	"team" TEXT NOT NULL,
	"member" TEXT NOT NULL,
	"role" TEXT NOT NULL,
	PRIMARY KEY ("team", "member")
);
INSERT INTO "_memberships_temp" ("team", "member", "role") SELECT "team", "member", "role" FROM "memberships";
DROP TABLE "memberships";
ALTER TABLE "_memberships_temp" RENAME TO "memberships";`)

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("memberships", "ORDER BY team, member")

		require.Equal(t, []map[string]any{
			{"team": "red", "member": "Alice", "role": "lead"},
		}, rows)
	})

	t.Run("RecreateTableWithPrimaryKeyAndIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				email TEXT PRIMARY KEY,
				age INTEGER
			);
			CREATE INDEX idx_users_age ON users (age);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				email TEXT PRIMARY KEY,
				age TEXT
			);
			CREATE INDEX idx_users_age ON users (age);

			INSERT INTO users (email, age) VALUES ('alice@example.com', '30'), ('bob@example.com', '25');
		`)

		// The PRIMARY KEY builds an index of the origin "pk". The recreation of the table
		// must print the explicit index only, and no index of the PRIMARY KEY.
		diff := driver.RequireDiff(`CREATE TABLE "_users_temp" (
	"email" TEXT PRIMARY KEY,
	"age" INTEGER
);
INSERT INTO "_users_temp" ("email", "age") SELECT "email", "age" FROM "users";
DROP TABLE "users";
ALTER TABLE "_users_temp" RENAME TO "users";
CREATE INDEX "idx_users_age" ON "users" ("age");`)

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY email")

		require.Equal(t, []map[string]any{
			{"email": "alice@example.com", "age": int64(30)},
			{"email": "bob@example.com", "age": int64(25)},
		}, rows)
	})

	t.Run("Triggers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
			CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 2; END;
			CREATE TRIGGER users_delete AFTER DELETE ON users BEGIN SELECT 3; END;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 999; END;
			CREATE TRIGGER users_delete AFTER DELETE ON users BEGIN SELECT 3; END;
			CREATE TRIGGER users_audit AFTER INSERT ON users BEGIN SELECT 4; END;
		`)

		expected := `CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
DROP TRIGGER "users_update";
CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 2; END;
DROP TRIGGER "users_audit";`

		diff := driver.RequireDiff(expected)

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateTableWithTriggers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
		`)

		expected := `CREATE TABLE "users" (
	"id" INTEGER PRIMARY KEY,
	"name" TEXT
);
CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;`

		diff := driver.RequireDiff(expected)

		driver.ExecOnTarget(diff)
	})

	t.Run("Views", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT name FROM users;
			CREATE VIEW admins_view AS SELECT name FROM users WHERE name = 'admin';
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE VIEW old_view AS SELECT id FROM users;
		`)

		expected := `CREATE VIEW admins_view AS SELECT name FROM users WHERE name = 'admin';
DROP VIEW "users_view";
CREATE VIEW users_view AS SELECT name FROM users;
DROP VIEW "old_view";`

		diff := driver.RequireDiff(expected)

		driver.ExecOnTarget(diff)
	})

	t.Run("ForeignKeys", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TABLE posts (
				id INTEGER PRIMARY KEY,
				user_id INTEGER,
				title TEXT,
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TABLE posts (
				id INTEGER PRIMARY KEY,
				user_id INTEGER,
				title TEXT
			);

			INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'First Post'), (2, 1, 'Second Post');
		`)

		// Since adding a FK requires table recreation
		expected := `CREATE TABLE "_posts_temp" (
	"id" INTEGER PRIMARY KEY,
	"user_id" INTEGER,
	"title" TEXT,
	FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE
);
INSERT INTO "_posts_temp" ("id", "user_id", "title") SELECT "id", "user_id", "title" FROM "posts";
DROP TABLE "posts";
ALTER TABLE "_posts_temp" RENAME TO "posts";`

		diff := driver.RequireDiff(expected)

		// Check that data is preserved after applying the diff
		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("posts", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "user_id": int64(1), "title": "First Post"},
			{"id": int64(2), "user_id": int64(1), "title": "Second Post"},
		}, rows)
	})

	t.Run("CompareRows", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

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
		driver := NewTestSQLiteDriver(t)
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
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);`

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
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnSource(`CREATE TABLE items (code INTEGER PRIMARY KEY, label TEXT);`)
		driver.ExecOnSource(`INSERT INTO items (code, label) VALUES (1, 'first');`)

		driver.ExecOnTarget(`CREATE TABLE items (identifier INTEGER PRIMARY KEY, label TEXT);`)
		driver.ExecOnTarget(`INSERT INTO items (identifier, label) VALUES (1, 'old');`)

		// The target holds no column with the name of the key, so the driver reads no key
		// of a target row.
		expected := `ALTER TABLE "items" RENAME COLUMN "identifier" TO "code";
-- The table "items" holds another primary key in the target, so dbdiff compares no row of it.`

		diff := driver.RequireDiff(expected)

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("items", "ORDER BY code")

		require.Equal(t, []map[string]any{
			{"code": int64(1), "label": "old"},
		}, rows)
	})

	t.Run("CompareNoRowWithoutTheDataFlag", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

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

	t.Run("RowIterationError", func(t *testing.T) {
		db, err := sql.Open("sqlite3_failing_rows", "")
		require.NoError(t, err)

		defer db.Close()

		driver := &SQLiteDriver{}

		// failingDriver yields one row, then fails on the second call to Next. The
		// read method must return that failure through rows.Err.
		_, err = driver.GetTableColumns(t.Context(), db, "users")
		require.ErrorIs(t, err, errRowIteration)
	})
}
