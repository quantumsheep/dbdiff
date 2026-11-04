package internalmigrations

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestMigrationIdentity(t *testing.T) {
	t.Run("VersionOfTime", func(t *testing.T) {
		moment := time.Date(2026, 8, 22, 14, 30, 0, 0, time.UTC)
		require.Equal(t, "20260822143000", VersionOfTime(moment))
	})

	t.Run("VersionUsesUTC", func(t *testing.T) {
		zone := time.FixedZone("test", 2*60*60)
		moment := time.Date(2026, 8, 22, 16, 30, 0, 0, zone)
		require.Equal(t, "20260822143000", VersionOfTime(moment))
	})

	t.Run("Slug", func(t *testing.T) {
		require.Equal(t, "add_created_at", Slug("Add created at"))
		require.Equal(t, "add_created_at", Slug("add-created-at"))
		require.Equal(t, "add_created_at", Slug("  Add   created  at!  "))
		require.Equal(t, "users2", Slug("users2"))
	})

	t.Run("ParseFileName", func(t *testing.T) {
		migration, err := ParseFileName("20260822143000_add_created_at.sql")
		require.NoError(t, err)
		require.Equal(t, "20260822143000", migration.Version)
		require.Equal(t, "add_created_at", migration.Name)
	})

	t.Run("ParseFileNameWithNoVersion", func(t *testing.T) {
		_, err := ParseFileName("add_created_at.sql")
		require.ErrorContains(t, err, "20260822143000_add_created_at.sql")
	})

	t.Run("ParseFileNameWithAShortVersion", func(t *testing.T) {
		_, err := ParseFileName("2026_add_created_at.sql")
		require.ErrorContains(t, err, "20260822143000_add_created_at.sql")
	})

	t.Run("Checksum", func(t *testing.T) {
		first := Checksum([]byte("SELECT 1;"))
		second := Checksum([]byte("SELECT 1;"))
		third := Checksum([]byte("SELECT 2;"))

		require.Equal(t, first, second)
		require.NotEqual(t, first, third)
		require.Len(t, first, 64)
	})

	t.Run("ReadDirectory", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "20260822143000_second.sql", "SELECT 2;")
		sqltest.WriteSQLFile(t, directory, "20260814101500_first.sql", "SELECT 1;")
		sqltest.WriteSQLFile(t, directory, "notes.txt", "ignored")

		migrations, err := ReadDirectory(directory)
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, "20260814101500", migrations[0].Version)
		require.Equal(t, "first", migrations[0].Name)
		require.Equal(t, "20260822143000", migrations[1].Version)
		require.Equal(t, Checksum([]byte("SELECT 1;")), migrations[0].Checksum)
	})

	t.Run("ReadDirectorySkipsADownFile", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "20260814101500_init.sql", "SELECT 1;")
		sqltest.WriteSQLFile(t, directory, "20260814101500_init.down.sql", "SELECT 2;")

		migrations, err := ReadDirectory(directory)
		require.NoError(t, err)
		require.Len(t, migrations, 1)
		require.Equal(t, "init", migrations[0].Name)
	})

	t.Run("ReadDirectoryThatIsAbsent", func(t *testing.T) {
		migrations, err := ReadDirectory(filepath.Join(t.TempDir(), "absent"))
		require.NoError(t, err)
		require.Empty(t, migrations)
	})

	t.Run("ReadDirectoryWithTwoFilesOfOneVersion", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "20260814101500_init.sql", "SELECT 1;")
		sqltest.WriteSQLFile(t, directory, "20260814101500_users.sql", "SELECT 2;")

		_, err := ReadDirectory(directory)
		require.ErrorContains(t, err, "hold one version")
	})

	t.Run("ReadDirectoryWithABadName", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "first.sql", "SELECT 1;")

		_, err := ReadDirectory(directory)
		require.ErrorContains(t, err, "first.sql")
	})
}

func TestConfig(t *testing.T) {
	writeConfig := func(t *testing.T, content string) string {
		t.Helper()

		path := filepath.Join(t.TempDir(), "dbdiff.yaml")

		err := os.WriteFile(path, []byte(content), 0o600)
		require.NoError(t, err)

		return path
	}

	t.Run("ReadEveryKey", func(t *testing.T) {
		path := writeConfig(t, "driver: postgres\nmigrations: ./migrations\ndatabase: postgres://localhost/app\n"+
			"desired: schema.sql\nschema: public\nserver-version: 17.11.0\nignore-table:\n  - example\n")

		config, err := ReadConfig(path, false)
		require.NoError(t, err)
		require.Equal(t, &Config{
			Driver:        driversshared.PostgresDriverName,
			Migrations:    "./migrations",
			Database:      "postgres://localhost/app",
			Desired:       "schema.sql",
			Schema:        "public",
			ServerVersion: "17.11.0",
			IgnoreTables:  []string{"example"},
		}, config)
	})

	t.Run("ExpandsAVariable", func(t *testing.T) {
		t.Setenv("DBDIFF_TEST_DATABASE_URL", "postgres://user:pa$word@localhost/app")
		t.Setenv("DBDIFF_TEST_TABLE", "example")

		path := writeConfig(t, "database: ${DBDIFF_TEST_DATABASE_URL}\nschema: $DBDIFF_TEST_TABLE\n"+
			"ignore-table:\n  - ${DBDIFF_TEST_TABLE}_audit\n")

		config, err := ReadConfig(path, false)
		require.NoError(t, err)
		require.Equal(t, &Config{
			Database:     "postgres://user:pa$word@localhost/app",
			Schema:       "$DBDIFF_TEST_TABLE",
			IgnoreTables: []string{"example_audit"},
		}, config)
	})

	t.Run("AVariableThatIsNotSet", func(t *testing.T) {
		path := writeConfig(t, "migrations: ./migrations\ndatabase: ${DBDIFF_TEST_VARIABLE_THAT_IS_NOT_SET}\n")

		_, err := ReadConfig(path, false)
		require.EqualError(t, err, fmt.Sprintf(`the variable "DBDIFF_TEST_VARIABLE_THAT_IS_NOT_SET" of the key "database" of %s is not set`, path))
	})

	t.Run("AVariableThatIsNotSetInTheList", func(t *testing.T) {
		path := writeConfig(t, "ignore-table:\n  - ${DBDIFF_TEST_VARIABLE_THAT_IS_NOT_SET}\n")

		_, err := ReadConfig(path, false)
		require.EqualError(t, err, fmt.Sprintf(`the variable "DBDIFF_TEST_VARIABLE_THAT_IS_NOT_SET" of the key "ignore-table" of %s is not set`, path))
	})

	t.Run("ARemovedKey", func(t *testing.T) {
		messages := map[string]string{
			"source: ./migrations\n":   `the key "source" of %s no longer exists. Use "migrations" for the migration directory`,
			"target: schema.sql\n":     `the key "target" of %s no longer exists. Use "database" for the database, or "desired" for the wanted schema of migrate generate`,
			"version: 17.11.0\n":       `the key "version" of %s no longer exists. Use "server-version" for the version of the temporary server`,
			"ignore:\n  tables: [a]\n": `the key "ignore" of %s no longer exists. Use "ignore-table" with a list of table names`,
		}

		for content, message := range messages {
			path := writeConfig(t, content)

			_, err := ReadConfig(path, false)
			require.EqualError(t, err, fmt.Sprintf(message, path))
		}
	})

	t.Run("ARemovedKeyOfAnotherFile", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ci.yaml")

		err := os.WriteFile(path, []byte("target: schema.sql\n"), 0o600)
		require.NoError(t, err)

		_, err = ReadConfig(path, false)
		require.EqualError(t, err, fmt.Sprintf(`the key "target" of %s no longer exists. `+
			`Use "database" for the database, or "desired" for the wanted schema of migrate generate`, path))
	})

	t.Run("AFileThatHoldsNoMapping", func(t *testing.T) {
		path := writeConfig(t, "- driver: sqlite3\n")

		_, err := ReadConfig(path, false)
		require.EqualError(t, err, fmt.Sprintf("the configuration file %s must hold a mapping of keys", path))
	})

	t.Run("AnEmptyFile", func(t *testing.T) {
		config, err := ReadConfig(writeConfig(t, ""), false)
		require.NoError(t, err)
		require.Equal(t, &Config{}, config)
	})

	t.Run("AFileThatHoldsOnlyAComment", func(t *testing.T) {
		config, err := ReadConfig(writeConfig(t, "# no key\n"), false)
		require.NoError(t, err)
		require.Equal(t, &Config{}, config)
	})

	t.Run("AnOptionalFileThatIsAbsent", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dbdiff.yaml")

		config, err := ReadConfig(path, true)
		require.NoError(t, err)
		require.Equal(t, &Config{}, config)
	})

	t.Run("ANamedFileThatIsAbsent", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dbdiff.yaml")

		_, err := ReadConfig(path, false)
		require.ErrorContains(t, err, "dbdiff.yaml")
	})

	t.Run("AFileThatHoldsBadYAML", func(t *testing.T) {
		directory := t.TempDir()

		path := filepath.Join(directory, "dbdiff.yaml")
		err := os.WriteFile(path, []byte("driver: [\n"), 0o600)
		require.NoError(t, err)

		_, err = ReadConfig(path, false)
		require.Error(t, err)
	})

	t.Run("AnUnknownKey", func(t *testing.T) {
		_, err := ReadConfig(writeConfig(t, "drivers: postgres\n"), false)
		require.EqualError(t, err, "yaml: unmarshal errors:\n  line 1: field drivers not found in type internalmigrations.Config")
	})
}

func TestSet(t *testing.T) {
	first := &Migration{
		Version:  "20260814101500",
		Name:     "init",
		Path:     "20260814101500_init.sql",
		Checksum: "aaa",
	}

	second := &Migration{
		Version:  "20260822143000",
		Name:     "add_created_at",
		Path:     "20260822143000_add_created_at.sql",
		Checksum: "bbb",
	}

	t.Run("Applied", func(t *testing.T) {
		set := NewSet([]*Migration{first}, []AppliedMigration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "aaa",
			},
		})

		require.Len(t, set.Entries, 1)
		require.Equal(t, StateApplied, set.Entries[0].State)
		require.Empty(t, set.Pending())
		require.Empty(t, set.Problems())
		require.NoError(t, set.ProblemError())
	})

	t.Run("Pending", func(t *testing.T) {
		set := NewSet([]*Migration{first, second}, []AppliedMigration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "aaa",
			},
		})

		require.Len(t, set.Pending(), 1)
		require.Equal(t, "20260822143000", set.Pending()[0].Migration.Version)
		require.NoError(t, set.ProblemError())
	})

	t.Run("Changed", func(t *testing.T) {
		set := NewSet([]*Migration{first}, []AppliedMigration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "other",
			},
		})

		require.Equal(t, StateChanged, set.Entries[0].State)
		require.ErrorContains(t, set.ProblemError(), "20260814101500_init")
		require.ErrorContains(t, set.RecordError(), "20260814101500_init")
	})

	t.Run("Dirty", func(t *testing.T) {
		set := NewSet([]*Migration{first}, []AppliedMigration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "aaa",
				Dirty:    true,
			},
		})

		require.Equal(t, StateDirty, set.Entries[0].State)
		require.ErrorContains(t, set.ProblemError(), "dirty")
		require.ErrorContains(t, set.RecordError(), "half applied")
	})

	t.Run("DirtyWithoutAFile", func(t *testing.T) {
		set := NewSet(nil, []AppliedMigration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "aaa",
				Dirty:    true,
			},
		})

		require.Equal(t, StateDirty, set.Entries[0].State)
		require.ErrorContains(t, set.RecordError(), "20260814101500_init")
	})

	t.Run("Missing", func(t *testing.T) {
		set := NewSet(nil, []AppliedMigration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "aaa",
			},
		})

		require.Equal(t, StateMissing, set.Entries[0].State)
		require.ErrorContains(t, set.ProblemError(), "20260814101500_init")
		require.ErrorContains(t, set.RecordError(), "20260814101500_init")
	})

	t.Run("OutOfOrder", func(t *testing.T) {
		set := NewSet([]*Migration{first, second}, []AppliedMigration{
			{
				Version:  "20260822143000",
				Name:     "add_created_at",
				Checksum: "bbb",
			},
		})

		require.Equal(t, StateOutOfOrder, set.Entries[0].State)
		require.Equal(t, StateApplied, set.Entries[1].State)
		require.ErrorContains(t, set.ProblemError(), "generate")
		require.NoError(t, set.RecordError())
	})

	t.Run("EntriesKeepTheVersionOrder", func(t *testing.T) {
		set := NewSet([]*Migration{second, first}, nil)

		require.Equal(t, "20260814101500", set.Entries[0].Migration.Version)
		require.Equal(t, "20260822143000", set.Entries[1].Migration.Version)
	})

	t.Run("Content", func(t *testing.T) {
		directory := t.TempDir()

		path := sqltest.WriteSQLFile(t, directory, "20260814101500_init.sql", "SELECT 1;\nSELECT 2;\n")

		entry := &Entry{
			Migration: &Migration{
				Path:     path,
				Checksum: Checksum([]byte("SELECT 1;\nSELECT 2;\n")),
			},
		}

		content, err := entry.Content()
		require.NoError(t, err)
		require.Equal(t, "SELECT 1;\nSELECT 2;\n", content)
	})

	t.Run("ContentOfAFileThatChanged", func(t *testing.T) {
		directory := t.TempDir()

		path := sqltest.WriteSQLFile(t, directory, "20260814101500_init.sql", "SELECT 1;\n")

		entries, err := ReadDirectory(directory)
		require.NoError(t, err)

		require.NoError(t, os.WriteFile(path, []byte("SELECT 2;\n"), 0o600))

		entry := &Entry{
			Migration: entries[0],
		}

		_, err = entry.Content()
		require.ErrorContains(t, err, "changed")
	})
}
