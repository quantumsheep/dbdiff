package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type PostgresDriverConfig struct {
	Schema               string
	ComparePrivileges    bool
	ScratchServerVersion string
	ScratchDatabase      string
	IgnoreTables         []string
}

type PostgresDriver struct {
	DesiredDatabaseConnection *sql.DB
	CurrentDatabaseConnection *sql.DB
	DesiredSchema             string
	CurrentSchema             string
	ComparePrivileges         bool
	IgnoreTables              []string
	ScratchServerVersion      string
	ScratchDatabase           string

	ScratchVersion embeddedpostgres.PostgresVersion

	scratchServer        *PostgresScratchServer
	scratchDatabaseNames []string

	recreatedObjectNames map[string]bool
}

func NewPostgresDriver(config *PostgresDriverConfig) *PostgresDriver {
	return &PostgresDriver{
		DesiredSchema:        config.Schema,
		CurrentSchema:        config.Schema,
		ComparePrivileges:    config.ComparePrivileges,
		IgnoreTables:         config.IgnoreTables,
		ScratchServerVersion: config.ScratchServerVersion,
		ScratchDatabase:      config.ScratchDatabase,
	}
}

func OpenPostgresConnection(connectionString string, schema string) (*sql.DB, error) {
	if schema == "" {
		return sql.Open("pgx", connectionString)
	}

	connectionConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}

	connectionConfig.RuntimeParams["search_path"] = driversshared.QuoteIdentifier(schema)

	return stdlib.OpenDB(*connectionConfig), nil
}

// PostgreSQL accepts a search path that names no schema, and each query then reads an
// empty schema. Without this check the diff drops every object of the current schema.
func (d *PostgresDriver) VerifySchema(ctx context.Context, db *sql.DB, schema string, role string) error {
	if schema == "" {
		return nil
	}

	row := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, schema)

	var found bool

	err := row.Scan(&found)
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("the %s database has no schema with the name %q", role, schema)
	}

	return nil
}

func (d *PostgresDriver) Diff(
	ctx context.Context,
	current driversshared.DataSource,
	desired driversshared.DataSource,
	options driversshared.DiffOptions,
) (driversshared.Instructions, error) {
	release, err := d.openSides(ctx, current, desired)
	if err != nil {
		return nil, err
	}

	defer release()

	d.recreatedObjectNames = nil

	err = d.VerifySchema(ctx, d.DesiredDatabaseConnection, d.DesiredSchema, "desired")
	if err != nil {
		return nil, err
	}

	err = d.VerifySchema(ctx, d.CurrentDatabaseConnection, d.CurrentSchema, "current")
	if err != nil {
		return nil, err
	}

	// Every section comes after the sections that its objects use. Keep this order.
	sections := []func(ctx context.Context) (*SectionDiff, error){
		d.DiffExtensions,
		d.DiffEnumTypes,
		d.DiffDomains,
		d.DiffCompositeTypes,
		d.DiffSequences,
		d.DiffFunctions,
		d.DiffCasts,
		d.DiffAggregates,
		d.DiffOperators,
		d.DiffTables,
		d.DiffSequenceOwners,
		d.DiffStatistics,
		d.DiffViewsAndMaterializedViews,
		d.DiffPrivileges,
	}

	sectionDiffs := make([]*SectionDiff, 0, len(sections))

	for _, section := range sections {
		sectionDiff, err := section(ctx)
		if err != nil {
			return nil, err
		}

		sectionDiffs = append(sectionDiffs, sectionDiff)
	}

	var instructions []driversshared.Instruction

	// PostgreSQL refuses a DROP statement while another object uses the object, so a removal
	// takes the reverse section order. An early removal runs before every addition.
	for _, sectionDiff := range slices.Backward(sectionDiffs) {
		instructions = append(instructions, sectionDiff.EarlyRemovals...)
	}

	for _, sectionDiff := range sectionDiffs {
		instructions = append(instructions, sectionDiff.Additions...)
	}

	for _, sectionDiff := range slices.Backward(sectionDiffs) {
		instructions = append(instructions, sectionDiff.Removals...)
	}

	if options.CompareData {
		dataInstructions, err := d.DiffData(ctx)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, dataInstructions...)
	}

	return instructions, nil
}

func (d *PostgresDriver) openSides(
	ctx context.Context,
	current driversshared.DataSource,
	desired driversshared.DataSource,
) (func(), error) {
	if d.ScratchDatabase == "" {
		d.ScratchVersion = d.scratchVersionOfSources(ctx, current, desired)
	}

	desiredDatabaseConnection, err := d.OpenSide(ctx, desired, d.DesiredSchema, "desired")
	if err != nil {
		_ = d.releaseScratchResources(ctx)
		return nil, err
	}

	d.DesiredDatabaseConnection = desiredDatabaseConnection

	currentDatabaseConnection, err := d.OpenSide(ctx, current, d.CurrentSchema, "current")
	if err != nil {
		_ = d.DesiredDatabaseConnection.Close()
		_ = d.releaseScratchResources(ctx)

		return nil, err
	}

	d.CurrentDatabaseConnection = currentDatabaseConnection

	return func() {
		_ = d.DesiredDatabaseConnection.Close()
		_ = d.CurrentDatabaseConnection.Close()
		d.DesiredDatabaseConnection = nil
		d.CurrentDatabaseConnection = nil
		_ = d.releaseScratchResources(ctx)
	}, nil
}

// An early removal covers the object that blocks an addition of another section.
type SectionDiff struct {
	EarlyRemovals []driversshared.Instruction
	Additions     []driversshared.Instruction
	Removals      []driversshared.Instruction
}

type sectionRules[T any] struct {
	Get    func(ctx context.Context, db *sql.DB) ([]T, error)
	Key    func(object T) string
	Create func(desired T) []driversshared.Instruction
	Change func(desired T, current T) []driversshared.Instruction
	Drop   func(current T) driversshared.Instruction
}

func diffSection[T any](ctx context.Context, driver *PostgresDriver, rules sectionRules[T]) (*SectionDiff, error) {
	additions, removals, err := driversshared.DiffSides(ctx, driver.DesiredDatabaseConnection, driver.CurrentDatabaseConnection, rules.Get, driversshared.DiffRules[T]{
		Key:    rules.Key,
		Create: rules.Create,
		Change: func(desired T, current T) ([]driversshared.Instruction, error) {
			return rules.Change(desired, current), nil
		},
		Drop: func(current T) []driversshared.Instruction {
			return []driversshared.Instruction{rules.Drop(current)}
		},
	})
	if err != nil {
		return nil, err
	}

	return &SectionDiff{
		Additions: additions,
		Removals:  removals,
	}, nil
}

// PostgreSQL refuses a change of a column while a view that reads it exists, so every DROP
// VIEW statement goes into the early removals.
// A recreated object loses its privileges and its owner, and DiffPrivileges reads this
// set to give them back.
func (d *PostgresDriver) markRecreated(name string) {
	if d.recreatedObjectNames == nil {
		d.recreatedObjectNames = map[string]bool{}
	}

	d.recreatedObjectNames[name] = true
}

// A depth-first walk puts each object after the objects that it depends on. Two
// independent objects keep their order in the input. A name that is not in the input
// does not count as a dependency.
func sortByDependency[T any](objects []T, nameOf func(T) string, dependencyNamesOf func(T) []string) []T {
	objectByName := make(map[string]T, len(objects))

	for _, object := range objects {
		objectByName[nameOf(object)] = object
	}

	sorted := make([]T, 0, len(objects))
	visited := make(map[string]bool, len(objects))

	var visit func(object T)

	visit = func(object T) {
		if visited[nameOf(object)] {
			return
		}

		visited[nameOf(object)] = true

		for _, dependencyName := range dependencyNamesOf(object) {
			dependency, found := objectByName[dependencyName]
			if found {
				visit(dependency)
			}
		}

		sorted = append(sorted, object)
	}

	for _, object := range objects {
		visit(object)
	}

	return sorted
}
