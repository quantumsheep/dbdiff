package migrations

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/quantumsheep/dbdiff/drivers"
	"github.com/samber/lo"
)

const migrationVersionLayout = "20060102150405"

type Migration struct {
	Version  string
	Name     string
	Path     string
	Checksum string
}

func MigrationVersionOfTime(moment time.Time) string {
	return moment.UTC().Format(migrationVersionLayout)
}

func MigrationSlug(text string) string {
	var builder strings.Builder

	previousIsSeparator := false

	for _, letter := range strings.ToLower(text) {
		isWordLetter := letter == '_' ||
			(letter >= 'a' && letter <= 'z') ||
			(letter >= '0' && letter <= '9')

		if isWordLetter {
			builder.WriteRune(letter)
			previousIsSeparator = false

			continue
		}

		if !previousIsSeparator {
			builder.WriteRune('_')
			previousIsSeparator = true
		}
	}

	return strings.Trim(builder.String(), "_")
}

func ParseMigrationFileName(fileName string) (*Migration, error) {
	base := strings.TrimSuffix(fileName, filepath.Ext(fileName))

	version, name, found := strings.Cut(base, "_")
	if !found || name == "" || !isMigrationVersion(version) {
		return nil, fmt.Errorf("the file %q does not take the form 20260822143000_add_created_at.sql", fileName)
	}

	migration := &Migration{
		Version: version,
		Name:    name,
	}

	return migration, nil
}

func isMigrationVersion(version string) bool {
	if len(version) != len(migrationVersionLayout) {
		return false
	}

	for _, letter := range version {
		if letter < '0' || letter > '9' {
			return false
		}
	}

	return true
}

func MigrationChecksum(content []byte) string {
	sum := sha256.Sum256(content)

	return hex.EncodeToString(sum[:])
}

func ReadMigrationDirectory(directory string) ([]*Migration, error) {
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	var migrations []*Migration

	for _, entry := range entries {
		if entry.IsDir() || !drivers.HasSQLExtension(entry.Name()) || drivers.IsDownMigration(entry.Name()) {
			continue
		}

		migration, err := ParseMigrationFileName(entry.Name())
		if err != nil {
			return nil, err
		}

		migration.Path = filepath.Join(directory, entry.Name())

		content, err := os.ReadFile(migration.Path)
		if err != nil {
			return nil, err
		}

		migration.Checksum = MigrationChecksum(content)

		migrations = append(migrations, migration)
	}

	slices.SortFunc(migrations, func(first *Migration, second *Migration) int {
		return strings.Compare(first.Version, second.Version)
	})

	// The history table holds one row for each version, so a second file of one version
	// breaks the record.
	for position := 1; position < len(migrations); position++ {
		if migrations[position].Version == migrations[position-1].Version {
			return nil, fmt.Errorf("the files %s and %s hold one version",
				filepath.Base(migrations[position-1].Path), filepath.Base(migrations[position].Path))
		}
	}

	return migrations, nil
}

type MigrationState string

const (
	MigrationApplied    MigrationState = "applied"
	MigrationPending    MigrationState = "pending"
	MigrationChanged    MigrationState = "changed"
	MigrationMissing    MigrationState = "missing"
	MigrationOutOfOrder MigrationState = "out of order"
	MigrationDirty      MigrationState = "dirty"
)

type MigrationEntry struct {
	Migration *Migration
	State     MigrationState
	AppliedAt time.Time
}

func (e *MigrationEntry) FileName() string {
	return e.Migration.Version + "_" + e.Migration.Name
}

// LoadMigrationSet reads the checksum, and this method reads the file again at the moment
// of the apply. An edit between the two reads gives a history row that names text that no
// engine ran.
func (e *MigrationEntry) Content() (string, error) {
	content, err := os.ReadFile(e.Migration.Path)
	if err != nil {
		return "", err
	}

	checksum := MigrationChecksum(content)
	if checksum != e.Migration.Checksum {
		return "", fmt.Errorf("the file %s changed while dbdiff read it", e.FileName())
	}

	return string(content), nil
}

type MigrationSet struct {
	Entries []*MigrationEntry
}

func NewMigrationSet(files []*Migration, applied []AppliedMigration) *MigrationSet {
	appliedByVersion := make(map[string]AppliedMigration, len(applied))
	lastAppliedVersion := ""

	for _, row := range applied {
		appliedByVersion[row.Version] = row

		if row.Version > lastAppliedVersion {
			lastAppliedVersion = row.Version
		}
	}

	var entries []*MigrationEntry

	for _, file := range files {
		entry := &MigrationEntry{
			Migration: file,
			State:     MigrationPending,
		}

		row, found := appliedByVersion[file.Version]
		delete(appliedByVersion, file.Version)

		if found {
			entry.AppliedAt = row.AppliedAt
			entry.State = MigrationApplied

			if row.Checksum != file.Checksum {
				entry.State = MigrationChanged
			}

			if row.Dirty {
				entry.State = MigrationDirty
			}
		}

		if entry.State == MigrationPending && file.Version < lastAppliedVersion {
			entry.State = MigrationOutOfOrder
		}

		entries = append(entries, entry)
	}

	for _, row := range appliedByVersion {
		state := MigrationMissing
		if row.Dirty {
			state = MigrationDirty
		}

		entries = append(entries, &MigrationEntry{
			Migration: &Migration{
				Version:  row.Version,
				Name:     row.Name,
				Checksum: row.Checksum,
			},
			State:     state,
			AppliedAt: row.AppliedAt,
		})
	}

	slices.SortFunc(entries, func(first *MigrationEntry, second *MigrationEntry) int {
		return strings.Compare(first.Migration.Version, second.Migration.Version)
	})

	set := &MigrationSet{
		Entries: entries,
	}

	return set
}

func (s *MigrationSet) Pending() []*MigrationEntry {
	return lo.Filter(s.Entries, func(entry *MigrationEntry, _ int) bool {
		return entry.State == MigrationPending
	})
}

func (s *MigrationSet) Problems() []*MigrationEntry {
	return lo.Filter(s.Entries, func(entry *MigrationEntry, _ int) bool {
		return entry.State == MigrationChanged ||
			entry.State == MigrationMissing ||
			entry.State == MigrationOutOfOrder ||
			entry.State == MigrationDirty
	})
}

func (s *MigrationSet) RecordError() error {
	return migrationStateError(lo.Filter(s.Entries, func(entry *MigrationEntry, _ int) bool {
		return entry.State == MigrationChanged ||
			entry.State == MigrationMissing ||
			entry.State == MigrationDirty
	}))
}

func (s *MigrationSet) ProblemError() error {
	return migrationStateError(s.Problems())
}

func migrationStateError(entries []*MigrationEntry) error {
	if len(entries) == 0 {
		return nil
	}

	messages := lo.Map(entries, func(entry *MigrationEntry, _ int) string {
		return fmt.Sprintf("%s is %s", entry.FileName(), entry.State)
	})

	return fmt.Errorf("the migrations of the database need attention: %s. "+
		"A changed file or a missing file needs the file of the record. "+
		"An out of order file needs a delete and a new generate. "+
		"A dirty file is half applied. Repair the database, and delete the history row of the file",
		strings.Join(messages, ", "))
}
