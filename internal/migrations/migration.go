package internalmigrations

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

const migrationVersionLayout = "20060102150405"

type Migration struct {
	Version  string
	Name     string
	Path     string
	Checksum string
}

func VersionOfTime(moment time.Time) string {
	return moment.UTC().Format(migrationVersionLayout)
}

func Slug(text string) string {
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

func ParseFileName(fileName string) (*Migration, error) {
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

func Checksum(content []byte) string {
	sum := sha256.Sum256(content)

	return hex.EncodeToString(sum[:])
}

func ReadDirectory(directory string) ([]*Migration, error) {
	entries, err := os.ReadDir(directory)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	var migrations []*Migration

	for _, entry := range entries {
		if entry.IsDir() || !driversshared.HasSQLExtension(entry.Name()) || driversshared.IsDownMigration(entry.Name()) {
			continue
		}

		migration, err := ParseFileName(entry.Name())
		if err != nil {
			return nil, err
		}

		migration.Path = filepath.Join(directory, entry.Name())

		content, err := os.ReadFile(migration.Path)
		if err != nil {
			return nil, err
		}

		migration.Checksum = Checksum(content)

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

type State string

const (
	StateApplied    State = "applied"
	StatePending    State = "pending"
	StateChanged    State = "changed"
	StateMissing    State = "missing"
	StateOutOfOrder State = "out of order"
	StateDirty      State = "dirty"
)

type Entry struct {
	Migration *Migration
	State     State
	AppliedAt time.Time
}

func (e *Entry) Label() string {
	return e.Migration.Version + "_" + e.Migration.Name
}

// LoadSet reads the checksum, and this method reads the file again at the moment
// of the apply. An edit between the two reads gives a history row that names text that no
// engine ran.
func (e *Entry) Content() (string, error) {
	content, err := os.ReadFile(e.Migration.Path)
	if err != nil {
		return "", err
	}

	checksum := Checksum(content)
	if checksum != e.Migration.Checksum {
		return "", fmt.Errorf("the file %s changed while dbdiff read it", e.Label())
	}

	return string(content), nil
}

type Set struct {
	Entries []*Entry
}

func NewSet(files []*Migration, applied []AppliedMigration) *Set {
	appliedByVersion := make(map[string]AppliedMigration, len(applied))
	lastAppliedVersion := ""

	for _, row := range applied {
		appliedByVersion[row.Version] = row

		if row.Version > lastAppliedVersion {
			lastAppliedVersion = row.Version
		}
	}

	var entries []*Entry

	for _, file := range files {
		entry := &Entry{
			Migration: file,
			State:     StatePending,
		}

		row, found := appliedByVersion[file.Version]
		delete(appliedByVersion, file.Version)

		if found {
			entry.AppliedAt = row.AppliedAt
			entry.State = StateApplied

			if row.Checksum != file.Checksum {
				entry.State = StateChanged
			}

			if row.Dirty {
				entry.State = StateDirty
			}
		}

		if entry.State == StatePending && file.Version < lastAppliedVersion {
			entry.State = StateOutOfOrder
		}

		entries = append(entries, entry)
	}

	for _, row := range appliedByVersion {
		state := StateMissing
		if row.Dirty {
			state = StateDirty
		}

		entries = append(entries, &Entry{
			Migration: &Migration{
				Version:  row.Version,
				Name:     row.Name,
				Checksum: row.Checksum,
			},
			State:     state,
			AppliedAt: row.AppliedAt,
		})
	}

	slices.SortFunc(entries, func(first *Entry, second *Entry) int {
		return strings.Compare(first.Migration.Version, second.Migration.Version)
	})

	set := &Set{
		Entries: entries,
	}

	return set
}

func (s *Set) Pending() []*Entry {
	return lo.Filter(s.Entries, func(entry *Entry, _ int) bool {
		return entry.State == StatePending
	})
}

func (s *Set) Problems() []*Entry {
	return lo.Filter(s.Entries, func(entry *Entry, _ int) bool {
		return entry.State == StateChanged ||
			entry.State == StateMissing ||
			entry.State == StateOutOfOrder ||
			entry.State == StateDirty
	})
}

// An out of order file is no record error. Up and step refuse it in ProblemError, because it breaks the version order.
func (s *Set) RecordError() error {
	return stateError(lo.Filter(s.Entries, func(entry *Entry, _ int) bool {
		return entry.State == StateChanged ||
			entry.State == StateMissing ||
			entry.State == StateDirty
	}))
}

func (s *Set) ProblemError() error {
	return stateError(s.Problems())
}

func stateError(entries []*Entry) error {
	if len(entries) == 0 {
		return nil
	}

	messages := lo.Map(entries, func(entry *Entry, _ int) string {
		return fmt.Sprintf("%s is %s", entry.Label(), entry.State)
	})

	return fmt.Errorf("the migrations of the database need attention: %s. "+
		"The command migrate repair updates a changed row, and it deletes a missing row. "+
		"An out of order file needs a delete and a new generate. "+
		"A dirty file is half applied. Repair the database first, and migrate repair then deletes its row",
		strings.Join(messages, ", "))
}
