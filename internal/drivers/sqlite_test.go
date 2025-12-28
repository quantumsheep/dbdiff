package drivers

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func tempSQLiteDatabase(tb testing.TB, name string) (string, *sql.DB) {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), name)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		tb.Fatalf("failed to create temp sqlite database: %v", err)
	}

	tb.Cleanup(func() {
		require.NoError(tb, db.Close())
	})

	return path, db
}

func mustExecSQL(tb testing.TB, db *sql.DB, sqlStatements string) {
	tb.Helper()

	_, err := db.Exec(sqlStatements)
	require.NoError(tb, err)
}

func expectDiff(tb testing.TB, driver *SQLiteDriver, expectedDiff string) string {
	tb.Helper()

	diff, err := driver.Diff(tb.Context())
	require.NoError(tb, err)
	require.Equal(tb, expectedDiff, diff)

	return diff
}

func TestSQLiteDriver(t *testing.T) {
	t.Run("NoChanges", func(t *testing.T) {
		sourceDatabasePath, _ := tempSQLiteDatabase(t, "source.sqlite")
		targetDatabasePath, _ := tempSQLiteDatabase(t, "target.sqlite")

		driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabasePath,
			TargetDatabasePath: targetDatabasePath,
		})
		require.NoError(t, err)
		defer driver.Close()

		expectDiff(t, driver, ``)
	})

	t.Run("CreateTables", func(t *testing.T) {
		sourceDatabasePath, sourceDatabase := tempSQLiteDatabase(t, "source.sqlite")
		targetDatabasePath, _ := tempSQLiteDatabase(t, "target.sqlite")

		mustExecSQL(t, sourceDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabasePath,
			TargetDatabasePath: targetDatabasePath,
		})
		require.NoError(t, err)
		defer driver.Close()

		expectDiff(t, driver, `CREATE TABLE "users" (
	"id" INTEGER PRIMARY KEY,
	"name" TEXT NOT NULL
);`)
	})

	t.Run("AddColumn", func(t *testing.T) {
		sourceDatabasePath, sourceDatabase := tempSQLiteDatabase(t, "source.sqlite")
		targetDatabasePath, targetDatabase := tempSQLiteDatabase(t, "target.sqlite")

		mustExecSQL(t, sourceDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)

		mustExecSQL(t, targetDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabasePath,
			TargetDatabasePath: targetDatabasePath,
		})
		require.NoError(t, err)
		defer driver.Close()

		diff := expectDiff(t, driver, `ALTER TABLE "users" ADD COLUMN "email" TEXT;`)

		// Check that data is preserved after applying the diff
		mustExecSQL(t, targetDatabase, diff)

		rows, err := targetDatabase.Query(`SELECT id, name FROM users ORDER BY id;`)
		require.NoError(t, err)
		defer rows.Close()

		type Result struct {
			ID   int
			Name string
		}

		var results []Result
		for rows.Next() {
			var r Result
			err := rows.Scan(&r.ID, &r.Name)
			require.NoError(t, err)
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		require.Equal(t, []Result{
			{ID: 1, Name: "Alice"},
			{ID: 2, Name: "Bob"},
		}, results)
	})

	t.Run("RemoveColumn", func(t *testing.T) {
		sourceDatabasePath, sourceDatabase := tempSQLiteDatabase(t, "source.sqlite")
		targetDatabasePath, targetDatabase := tempSQLiteDatabase(t, "target.sqlite")

		mustExecSQL(t, sourceDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		mustExecSQL(t, targetDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)

		driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabasePath,
			TargetDatabasePath: targetDatabasePath,
		})
		require.NoError(t, err)
		defer driver.Close()

		expectDiff(t, driver, `ALTER TABLE "users" DROP COLUMN "email";`)
	})

	t.Run("RenameColumn", func(t *testing.T) {
		sourceDatabasePath, sourceDatabase := tempSQLiteDatabase(t, "source.sqlite")
		targetDatabasePath, targetDatabase := tempSQLiteDatabase(t, "target.sqlite")

		mustExecSQL(t, sourceDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				full_name TEXT NOT NULL
			);
		`)

		mustExecSQL(t, targetDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabasePath,
			TargetDatabasePath: targetDatabasePath,
		})
		require.NoError(t, err)
		defer driver.Close()

		diff := expectDiff(t, driver, `ALTER TABLE "users" RENAME COLUMN "name" TO "full_name";`)

		// Check that data is preserved after applying the diff
		mustExecSQL(t, targetDatabase, diff)

		rows, err := targetDatabase.Query(`SELECT id, full_name FROM users ORDER BY id;`)
		require.NoError(t, err)
		defer rows.Close()

		type Row struct {
			ID       int
			FullName string
		}

		var results []Row
		for rows.Next() {
			var r Row
			err := rows.Scan(&r.ID, &r.FullName)
			require.NoError(t, err)
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		require.Equal(t, []Row{
			{ID: 1, FullName: "Alice"},
			{ID: 2, FullName: "Bob"},
		}, results)
	})

	t.Run("ModifyColumnType", func(t *testing.T) {
		sourceDatabasePath, sourceDatabase := tempSQLiteDatabase(t, "source.sqlite")
		targetDatabasePath, targetDatabase := tempSQLiteDatabase(t, "target.sqlite")

		mustExecSQL(t, sourceDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				age INTEGER
			);
		`)

		mustExecSQL(t, targetDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				age TEXT
			);
		`)

		driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabasePath,
			TargetDatabasePath: targetDatabasePath,
		})
		require.NoError(t, err)
		defer driver.Close()

		expectDiff(t, driver, `ALTER TABLE "users" ADD COLUMN "age" INTEGER;
ALTER TABLE "users" DROP COLUMN "age";`)
	})

	t.Run("ModifyColumnSetNotNull", func(t *testing.T) {
		sourceDatabasePath, sourceDatabase := tempSQLiteDatabase(t, "source.sqlite")
		targetDatabasePath, targetDatabase := tempSQLiteDatabase(t, "target.sqlite")

		mustExecSQL(t, sourceDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		mustExecSQL(t, targetDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabasePath,
			TargetDatabasePath: targetDatabasePath,
		})
		require.NoError(t, err)
		defer driver.Close()

		diff := expectDiff(t, driver, `CREATE TABLE "_users_temp" (
	"id" INTEGER PRIMARY KEY,
	"name" TEXT NOT NULL
);
INSERT INTO "_users_temp" ("id", "name") SELECT "id", "name" FROM "users";
DROP TABLE "users";
ALTER TABLE "_users_temp" RENAME TO "users";`)

		// Check that data is preserved after applying the diff
		mustExecSQL(t, targetDatabase, diff)

		rows, err := targetDatabase.Query(`SELECT id, name FROM users ORDER BY id;`)
		require.NoError(t, err)
		defer rows.Close()

		type Row struct {
			ID   int
			Name string
		}

		var results []Row
		for rows.Next() {
			var r Row
			err := rows.Scan(&r.ID, &r.Name)
			require.NoError(t, err)
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		require.Equal(t, []Row{
			{ID: 1, Name: "Alice"},
			{ID: 2, Name: "Bob"},
		}, results)
	})

	t.Run("ModifyColumnDropNotNull", func(t *testing.T) {
		sourceDatabasePath, sourceDatabase := tempSQLiteDatabase(t, "source.sqlite")
		targetDatabasePath, targetDatabase := tempSQLiteDatabase(t, "target.sqlite")

		mustExecSQL(t, sourceDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT
			);
		`)

		mustExecSQL(t, targetDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabasePath,
			TargetDatabasePath: targetDatabasePath,
		})
		require.NoError(t, err)
		defer driver.Close()

		diff := expectDiff(t, driver, `CREATE TABLE "_users_temp" (
	"id" INTEGER PRIMARY KEY,
	"name" TEXT
);
INSERT INTO "_users_temp" ("id", "name") SELECT "id", "name" FROM "users";
DROP TABLE "users";
ALTER TABLE "_users_temp" RENAME TO "users";`)

		// Check that data is preserved after applying the diff
		mustExecSQL(t, targetDatabase, diff)

		rows, err := targetDatabase.Query(`SELECT id, name FROM users ORDER BY id;`)
		require.NoError(t, err)
		defer rows.Close()

		type Row struct {
			ID   int
			Name sql.NullString
		}

		var results []Row
		for rows.Next() {
			var r Row
			err := rows.Scan(&r.ID, &r.Name)
			require.NoError(t, err)
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		require.Equal(t, []Row{
			{ID: 1, Name: sql.NullString{String: "Alice", Valid: true}},
			{ID: 2, Name: sql.NullString{String: "Bob", Valid: true}},
		}, results)
	})

	t.Run("DropTables", func(t *testing.T) {
		sourceDatabasePath, _ := tempSQLiteDatabase(t, "source.sqlite")
		targetDatabasePath, targetDatabase := tempSQLiteDatabase(t, "target.sqlite")

		mustExecSQL(t, targetDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabasePath,
			TargetDatabasePath: targetDatabasePath,
		})
		require.NoError(t, err)
		defer driver.Close()

		expectDiff(t, driver, `DROP TABLE "users";`)
	})

	t.Run("CreateIndexes", func(t *testing.T) {
		sourceDatabasePath, sourceDatabase := tempSQLiteDatabase(t, "source.sqlite")
		targetDatabasePath, targetDatabase := tempSQLiteDatabase(t, "target.sqlite")

		mustExecSQL(t, sourceDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);
		`)

		mustExecSQL(t, targetDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabasePath,
			TargetDatabasePath: targetDatabasePath,
		})
		require.NoError(t, err)
		defer driver.Close()

		expectDiff(t, driver, `CREATE UNIQUE INDEX "idx_users_name" ON "users" ("name");`)
	})

	t.Run("DropIndexes", func(t *testing.T) {
		sourceDatabasePath, sourceDatabase := tempSQLiteDatabase(t, "source.sqlite")
		targetDatabasePath, targetDatabase := tempSQLiteDatabase(t, "target.sqlite")

		mustExecSQL(t, sourceDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		mustExecSQL(t, targetDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);
		`)

		driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabasePath,
			TargetDatabasePath: targetDatabasePath,
		})
		require.NoError(t, err)
		defer driver.Close()

		expectDiff(t, driver, `DROP INDEX "idx_users_name";`)
	})

	t.Run("ModifyIndexes", func(t *testing.T) {
		sourceDatabasePath, sourceDatabase := tempSQLiteDatabase(t, "source.sqlite")
		targetDatabasePath, targetDatabase := tempSQLiteDatabase(t, "target.sqlite")

		mustExecSQL(t, sourceDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name, email);
		`)

		mustExecSQL(t, targetDatabase, `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);

			INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com');
		`)

		driver, err := NewSQLiteDriver(&SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabasePath,
			TargetDatabasePath: targetDatabasePath,
		})
		require.NoError(t, err)
		defer driver.Close()

		expectDiff(t, driver, `DROP INDEX "idx_users_name";
CREATE UNIQUE INDEX "idx_users_name" ON "users" ("name", "email");`)
	})
}
