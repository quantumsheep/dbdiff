package drivers

import (
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
}
