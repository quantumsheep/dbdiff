package driverssqlite

import (
	"path/filepath"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestSQLiteSQLSource(t *testing.T) {
	t.Run("SQLFileDesired", func(t *testing.T) {
		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "schema.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)

		currentPath := filepath.Join(t.TempDir(), "current.sqlite")

		driver := NewTestSQLiteDriverWithPaths(t, desiredPath, currentPath)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice');
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLiteAddColumnAction{
					Column: &SQLiteColumn{
						Name: "email",
						Type: "TEXT",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "email": nil},
		}, rows)
	})

	t.Run("MigrationsDirectoryAsDesired", func(t *testing.T) {
		migrationsDirectory := t.TempDir()

		sqltest.WriteSQLFile(t, migrationsDirectory, "001_create_users.up.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "002_add_email.up.sql", `
			ALTER TABLE users ADD COLUMN email TEXT;
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "002_add_email.down.sql", `
			ALTER TABLE users DROP COLUMN email;
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "010_add_index.up.sql", `
			CREATE INDEX users_email ON users (email);
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "notes.txt", `This file holds no SQL.`)

		currentPath := filepath.Join(t.TempDir(), "current.sqlite")

		driver := NewTestSQLiteDriverWithPaths(t, migrationsDirectory, currentPath)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				Name: "users",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name:    "name",
						Type:    "TEXT",
						NotNull: true,
					},
					{
						Name: "email",
						Type: "TEXT",
					},
				},
				ForeignKeys: []*SQLiteForeignKey{},
			},
			&SQLiteCreateIndexInstruction{
				Name:      "users_email",
				TableName: "users",
				Keys:      []string{`"email"`},
			},
		})

		driver.ExecOnCurrent(`INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');`)
		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "email": "alice@example.com"},
		}, rows)
	})

	t.Run("GooseMigrationsDirectorySource", func(t *testing.T) {
		migrationsDirectory := t.TempDir()

		sqltest.WriteSQLFile(t, migrationsDirectory, "00001_create_users.sql", `
			-- +goose Up
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			-- +goose Down
			DROP TABLE users;
		`)

		currentPath := filepath.Join(t.TempDir(), "current.sqlite")

		driver := NewTestSQLiteDriverWithPaths(t, migrationsDirectory, currentPath)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				Name: "users",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name:    "name",
						Type:    "TEXT",
						NotNull: true,
					},
				},
				ForeignKeys: []*SQLiteForeignKey{},
			},
		})
	})

	t.Run("SQLFileDesiredOnBothSides", func(t *testing.T) {
		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "desired.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)

		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			CREATE TABLE audit (
				id INTEGER PRIMARY KEY
			);
		`)

		driver := NewTestSQLiteDriverWithPaths(t, desiredPath, currentPath)

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLiteAddColumnAction{
					Column: &SQLiteColumn{
						Name: "email",
						Type: "TEXT",
					},
				},
			},
			&driversshared.SQLDropTableInstruction{
				Name: "audit",
			},
		})

		driver.ExecOnCurrent(diff)
	})

	t.Run("EmptyDirectoryDesired", func(t *testing.T) {
		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY);
		`)

		driver := NewTestSQLiteDriverWithPaths(t, t.TempDir(), currentPath)

		driver.RequireInstructions([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{
				Name: "users",
			},
		})
	})

	t.Run("InvalidSQLFileDesired", func(t *testing.T) {
		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "schema.sql", `CREATE TABLE users (;`)
		currentPath := filepath.Join(t.TempDir(), "current.sqlite")

		driver := NewSQLiteDriver(&SQLiteDriverConfig{})

		_, err := driver.Diff(t.Context(), driversshared.ParseDataSource(currentPath),
			driversshared.ParseDataSource(desiredPath), driversshared.DiffOptions{})
		require.ErrorContains(t, err, "schema.sql")
	})
}
