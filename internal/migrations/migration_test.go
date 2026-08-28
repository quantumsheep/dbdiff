package migrations

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/quantumsheep/dbdiff/internal/drivers"
	"github.com/stretchr/testify/require"
)

func TestMigrationIdentity(t *testing.T) {
	t.Run("VersionOfTime", func(t *testing.T) {
		moment := time.Date(2026, 8, 22, 14, 30, 0, 0, time.UTC)
		require.Equal(t, "20260822143000", MigrationVersionOfTime(moment))
	})

	t.Run("VersionUsesUTC", func(t *testing.T) {
		zone := time.FixedZone("test", 2*60*60)
		moment := time.Date(2026, 8, 22, 16, 30, 0, 0, zone)
		require.Equal(t, "20260822143000", MigrationVersionOfTime(moment))
	})

	t.Run("Slug", func(t *testing.T) {
		require.Equal(t, "add_created_at", MigrationSlug("Add created at"))
		require.Equal(t, "add_created_at", MigrationSlug("add-created-at"))
		require.Equal(t, "add_created_at", MigrationSlug("  Add   created  at!  "))
		require.Equal(t, "users2", MigrationSlug("users2"))
	})

	t.Run("ParseFileName", func(t *testing.T) {
		migration, err := ParseMigrationFileName("20260822143000_add_created_at.sql")
		require.NoError(t, err)
		require.Equal(t, "20260822143000", migration.Version)
		require.Equal(t, "add_created_at", migration.Name)
	})

	t.Run("ParseFileNameWithNoVersion", func(t *testing.T) {
		_, err := ParseMigrationFileName("add_created_at.sql")
		require.ErrorContains(t, err, "20260822143000_add_created_at.sql")
	})

	t.Run("ParseFileNameWithAShortVersion", func(t *testing.T) {
		_, err := ParseMigrationFileName("2026_add_created_at.sql")
		require.ErrorContains(t, err, "20260822143000_add_created_at.sql")
	})

	t.Run("Checksum", func(t *testing.T) {
		first := MigrationChecksum([]byte("SELECT 1;"))
		second := MigrationChecksum([]byte("SELECT 1;"))
		third := MigrationChecksum([]byte("SELECT 2;"))

		require.Equal(t, first, second)
		require.NotEqual(t, first, third)
		require.Len(t, first, 64)
	})

	t.Run("ReadDirectory", func(t *testing.T) {
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260822143000_second.sql", "SELECT 2;")
		WriteSQLFile(t, directory, "20260814101500_first.sql", "SELECT 1;")
		WriteSQLFile(t, directory, "notes.txt", "ignored")

		migrations, err := ReadMigrationDirectory(directory)
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, "20260814101500", migrations[0].Version)
		require.Equal(t, "first", migrations[0].Name)
		require.Equal(t, "20260822143000", migrations[1].Version)
		require.Equal(t, MigrationChecksum([]byte("SELECT 1;")), migrations[0].Checksum)
	})

	t.Run("ReadDirectorySkipsADownFile", func(t *testing.T) {
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql", "SELECT 1;")
		WriteSQLFile(t, directory, "20260814101500_init.down.sql", "SELECT 2;")

		migrations, err := ReadMigrationDirectory(directory)
		require.NoError(t, err)
		require.Len(t, migrations, 1)
		require.Equal(t, "init", migrations[0].Name)
	})

	t.Run("ReadDirectoryThatIsAbsent", func(t *testing.T) {
		migrations, err := ReadMigrationDirectory(filepath.Join(t.TempDir(), "absent"))
		require.NoError(t, err)
		require.Empty(t, migrations)
	})

	t.Run("ReadDirectoryWithTwoFilesOfOneVersion", func(t *testing.T) {
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql", "SELECT 1;")
		WriteSQLFile(t, directory, "20260814101500_users.sql", "SELECT 2;")

		_, err := ReadMigrationDirectory(directory)
		require.ErrorContains(t, err, "hold one version")
	})

	t.Run("ReadDirectoryWithABadName", func(t *testing.T) {
		directory := t.TempDir()

		WriteSQLFile(t, directory, "first.sql", "SELECT 1;")

		_, err := ReadMigrationDirectory(directory)
		require.ErrorContains(t, err, "first.sql")
	})
}

func TestMigrationConfig(t *testing.T) {
	t.Run("ReadEveryKey", func(t *testing.T) {
		directory := t.TempDir()

		path := filepath.Join(directory, "dbdiff.yaml")
		err := os.WriteFile(path, []byte("driver: postgres\ntarget: schema.sql\nsource: ./migrations\nschema: public\n"), 0o600)
		require.NoError(t, err)

		config, err := ReadMigrationConfig(path, false)
		require.NoError(t, err)
		require.Equal(t, drivers.PostgresDriverName, config.Driver)
		require.Equal(t, "schema.sql", config.Target)
		require.Equal(t, "./migrations", config.Source)
		require.Equal(t, "public", config.Schema)
	})

	t.Run("AnOptionalFileThatIsAbsent", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dbdiff.yaml")

		config, err := ReadMigrationConfig(path, true)
		require.NoError(t, err)
		require.Equal(t, &MigrationConfig{}, config)
	})

	t.Run("ANamedFileThatIsAbsent", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dbdiff.yaml")

		_, err := ReadMigrationConfig(path, false)
		require.ErrorContains(t, err, "dbdiff.yaml")
	})

	t.Run("AFileThatHoldsBadYAML", func(t *testing.T) {
		directory := t.TempDir()

		path := filepath.Join(directory, "dbdiff.yaml")
		err := os.WriteFile(path, []byte("driver: [\n"), 0o600)
		require.NoError(t, err)

		_, err = ReadMigrationConfig(path, false)
		require.Error(t, err)
	})

	t.Run("AnUnknownKey", func(t *testing.T) {
		directory := t.TempDir()

		path := filepath.Join(directory, "dbdiff.yaml")
		err := os.WriteFile(path, []byte("drivers: postgres\n"), 0o600)
		require.NoError(t, err)

		_, err = ReadMigrationConfig(path, false)
		require.ErrorContains(t, err, "drivers")
	})
}

func TestMigrationSet(t *testing.T) {
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
		set := NewMigrationSet([]*Migration{first}, []AppliedMigration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "aaa",
			},
		})

		require.Len(t, set.Entries, 1)
		require.Equal(t, MigrationApplied, set.Entries[0].State)
		require.Empty(t, set.Pending())
		require.Empty(t, set.Problems())
		require.NoError(t, set.ProblemError())
	})

	t.Run("Pending", func(t *testing.T) {
		set := NewMigrationSet([]*Migration{first, second}, []AppliedMigration{
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
		set := NewMigrationSet([]*Migration{first}, []AppliedMigration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "other",
			},
		})

		require.Equal(t, MigrationChanged, set.Entries[0].State)
		require.ErrorContains(t, set.ProblemError(), "20260814101500_init")
		require.ErrorContains(t, set.RecordError(), "20260814101500_init")
	})

	t.Run("Dirty", func(t *testing.T) {
		set := NewMigrationSet([]*Migration{first}, []AppliedMigration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "aaa",
				Dirty:    true,
			},
		})

		require.Equal(t, MigrationDirty, set.Entries[0].State)
		require.ErrorContains(t, set.ProblemError(), "dirty")
		require.ErrorContains(t, set.RecordError(), "half applied")
	})

	t.Run("DirtyWithoutAFile", func(t *testing.T) {
		set := NewMigrationSet(nil, []AppliedMigration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "aaa",
				Dirty:    true,
			},
		})

		require.Equal(t, MigrationDirty, set.Entries[0].State)
		require.ErrorContains(t, set.RecordError(), "20260814101500_init")
	})

	t.Run("Missing", func(t *testing.T) {
		set := NewMigrationSet(nil, []AppliedMigration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "aaa",
			},
		})

		require.Equal(t, MigrationMissing, set.Entries[0].State)
		require.ErrorContains(t, set.ProblemError(), "20260814101500_init")
		require.ErrorContains(t, set.RecordError(), "20260814101500_init")
	})

	t.Run("OutOfOrder", func(t *testing.T) {
		set := NewMigrationSet([]*Migration{first, second}, []AppliedMigration{
			{
				Version:  "20260822143000",
				Name:     "add_created_at",
				Checksum: "bbb",
			},
		})

		require.Equal(t, MigrationOutOfOrder, set.Entries[0].State)
		require.Equal(t, MigrationApplied, set.Entries[1].State)
		require.ErrorContains(t, set.ProblemError(), "generate")
		require.NoError(t, set.RecordError())
	})

	t.Run("EntriesKeepTheVersionOrder", func(t *testing.T) {
		set := NewMigrationSet([]*Migration{second, first}, nil)

		require.Equal(t, "20260814101500", set.Entries[0].Migration.Version)
		require.Equal(t, "20260822143000", set.Entries[1].Migration.Version)
	})

	t.Run("Content", func(t *testing.T) {
		directory := t.TempDir()

		path := WriteSQLFile(t, directory, "20260814101500_init.sql", "SELECT 1;\nSELECT 2;\n")

		entry := &MigrationEntry{
			Migration: &Migration{
				Path:     path,
				Checksum: MigrationChecksum([]byte("SELECT 1;\nSELECT 2;\n")),
			},
		}

		content, err := entry.Content()
		require.NoError(t, err)
		require.Equal(t, "SELECT 1;\nSELECT 2;\n", content)
	})

	t.Run("ContentOfAFileThatChanged", func(t *testing.T) {
		directory := t.TempDir()

		path := WriteSQLFile(t, directory, "20260814101500_init.sql", "SELECT 1;\n")

		entries, err := ReadMigrationDirectory(directory)
		require.NoError(t, err)

		require.NoError(t, os.WriteFile(path, []byte("SELECT 2;\n"), 0o600))

		entry := &MigrationEntry{
			Migration: entries[0],
		}

		_, err = entry.Content()
		require.ErrorContains(t, err, "changed")
	})
}
