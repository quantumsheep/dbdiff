package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

const (
	postgresScratchUser        = "postgres"
	postgresScratchPassword    = "postgres"
	embeddedPostgresModulePath = "github.com/fergusstrange/embedded-postgres"
)

var postgresScratchVersions = map[int]embeddedpostgres.PostgresVersion{
	9:  embeddedpostgres.V9,
	10: embeddedpostgres.V10,
	11: embeddedpostgres.V11,
	12: embeddedpostgres.V12,
	13: embeddedpostgres.V13,
	14: embeddedpostgres.V14,
	15: embeddedpostgres.V15,
	16: embeddedpostgres.V16,
	17: embeddedpostgres.V17,
	18: embeddedpostgres.V18,
}

type PostgresScratchServer struct {
	server             *embeddedpostgres.EmbeddedPostgres
	port               uint32
	temporaryDirectory string
}

func StartPostgresScratchServer(version embeddedpostgres.PostgresVersion) (*PostgresScratchServer, error) {
	port, err := findFreePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find a free port for the temporary postgres server: %w", err)
	}

	temporaryDirectory, err := os.MkdirTemp("", "dbdiff-postgres-")
	if err != nil {
		return nil, err
	}

	// dbdiff writes the SQL statements to the standard output, and the default logger of
	// the library writes its log to that same stream.
	configuration := embeddedpostgres.DefaultConfig().
		Port(port).
		Username(postgresScratchUser).
		Password(postgresScratchPassword).
		RuntimePath(filepath.Join(temporaryDirectory, "runtime")).
		DataPath(filepath.Join(temporaryDirectory, "data")).
		Logger(io.Discard)

	if version != "" {
		configuration = configuration.Version(version)
	}

	binariesPath, err := postgresScratchBinariesPath(version)
	if err == nil {
		configuration = configuration.BinariesPath(binariesPath)
	}

	server := embeddedpostgres.NewDatabase(configuration)

	err = server.Start()
	if err != nil {
		_ = os.RemoveAll(temporaryDirectory)
		return nil, fmt.Errorf("failed to start the temporary postgres server: %w", err)
	}

	scratchServer := &PostgresScratchServer{
		server:             server,
		port:               port,
		temporaryDirectory: temporaryDirectory,
	}

	return scratchServer, nil
}

func (s *PostgresScratchServer) ConnectionString(database string) string {
	return fmt.Sprintf(
		"postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable",
		postgresScratchUser,
		postgresScratchPassword,
		s.port,
		database,
	)
}

func (s *PostgresScratchServer) CreateDatabase(ctx context.Context, name string) (string, error) {
	connection, err := sql.Open("pgx", s.ConnectionString("postgres"))
	if err != nil {
		return "", err
	}

	defer func() { _ = connection.Close() }()

	_, err = connection.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", driversshared.QuoteIdentifier(name)))
	if err != nil {
		return "", fmt.Errorf("failed to create the temporary database %q: %w", name, err)
	}

	return s.ConnectionString(name), nil
}

func (s *PostgresScratchServer) Stop() error {
	err := s.server.Stop()

	removeError := os.RemoveAll(s.temporaryDirectory)
	if err == nil {
		err = removeError
	}

	return err
}

// This directory keeps the extracted server between two runs. Its name holds the version,
// because a new library can hold a new default.
func postgresScratchBinariesPath(version embeddedpostgres.PostgresVersion) (string, error) {
	cacheDirectory, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}

	name := string(version)
	if name == "" {
		name = "default-" + embeddedPostgresModuleVersion()
	}

	return filepath.Join(cacheDirectory, "dbdiff", "postgres-"+name), nil
}

func embeddedPostgresModuleVersion() string {
	buildInformation, available := debug.ReadBuildInfo()
	if !available {
		return "unknown"
	}

	for _, dependency := range buildInformation.Deps {
		if dependency.Path == embeddedPostgresModulePath {
			return dependency.Version
		}
	}

	return "unknown"
}

func DetectPostgresScratchVersion(ctx context.Context, connectionString string) embeddedpostgres.PostgresVersion {
	connection, err := sql.Open("pgx", connectionString)
	if err != nil {
		return ""
	}

	defer func() { _ = connection.Close() }()

	row := connection.QueryRowContext(ctx, "SELECT current_setting('server_version_num')::int")

	var versionNumber int

	err = row.Scan(&versionNumber)
	if err != nil {
		return ""
	}

	return postgresScratchVersions[versionNumber/10000]
}

func postgresScratchVersionOfConfig(ctx context.Context, config *PostgresDriverConfig) embeddedpostgres.PostgresVersion {
	targetHoldsSQL := driversshared.IsSQLSource(config.TargetConnectionString)
	sourceHoldsSQL := driversshared.IsSQLSource(config.SourceConnectionString)

	if targetHoldsSQL && !sourceHoldsSQL {
		return DetectPostgresScratchVersion(ctx, config.SourceConnectionString)
	}

	if sourceHoldsSQL && !targetHoldsSQL {
		return DetectPostgresScratchVersion(ctx, config.TargetConnectionString)
	}

	if targetHoldsSQL && sourceHoldsSQL && config.ScratchServerVersion != "" {
		return embeddedpostgres.PostgresVersion(config.ScratchServerVersion)
	}

	return ""
}

func findFreePort() (uint32, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}

	defer func() { _ = listener.Close() }()

	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("the listener gave no TCP address")
	}

	return uint32(address.Port), nil
}

func (d *PostgresDriver) OpenSide(ctx context.Context, connectionString string, schema string, role string) (*sql.DB, error) {
	if !driversshared.IsSQLSource(connectionString) {
		return OpenPostgresConnection(connectionString, schema)
	}

	sqlSource, err := driversshared.NewSQLSource(connectionString)
	if err != nil {
		return nil, err
	}

	scratchServer, err := d.ScratchServer()
	if err != nil {
		return nil, err
	}

	databaseConnectionString, err := scratchServer.CreateDatabase(ctx, role)
	if err != nil {
		return nil, err
	}

	connection, err := OpenPostgresConnection(databaseConnectionString, schema)
	if err != nil {
		return nil, err
	}

	err = sqlSource.ApplyTo(ctx, connection)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}

	return connection, nil
}

func (d *PostgresDriver) ScratchServer() (*PostgresScratchServer, error) {
	if d.scratchServer != nil {
		return d.scratchServer, nil
	}

	scratchServer, err := StartPostgresScratchServer(d.ScratchVersion)
	if err != nil {
		return nil, err
	}

	d.scratchServer = scratchServer

	return scratchServer, nil
}

func (d *PostgresDriver) StopScratchServer() error {
	if d.scratchServer == nil {
		return nil
	}

	scratchServer := d.scratchServer
	d.scratchServer = nil

	return scratchServer.Stop()
}
