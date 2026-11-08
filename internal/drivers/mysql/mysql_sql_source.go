package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	embeddedmysql "github.com/quantumsheep/embedded-mysql"
)

// The replay of a SQL source and the apply of a migration send several statements in one
// call, and the driver refuses that without MultiStatements.
func ParseMySQLConnectionString(connectionString string) (*mysql.Config, error) {
	config, err := parseMySQLConnectionString(connectionString)
	if err != nil {
		return nil, err
	}

	config.MultiStatements = true

	return config, nil
}

func parseMySQLConnectionString(connectionString string) (*mysql.Config, error) {
	withURLPrefix := strings.HasPrefix(connectionString, "mysql://") ||
		strings.HasPrefix(connectionString, "mariadb://")

	if !withURLPrefix {
		return mysql.ParseDSN(connectionString)
	}

	parsed, err := url.Parse(connectionString)
	if err != nil {
		return nil, err
	}

	address := parsed.Host
	if parsed.Port() == "" {
		address += ":3306"
	}

	config, err := mysql.ParseDSN("tcp(" + address + ")/?" + parsed.RawQuery)
	if err != nil {
		return nil, err
	}

	config.User = parsed.User.Username()
	config.Passwd, _ = parsed.User.Password()
	config.DBName = strings.TrimPrefix(parsed.Path, "/")

	return config, nil
}

// SET FOREIGN_KEY_CHECKS changes one session, and a pool with several connections runs
// the next statement on another session. One connection keeps the setting.
func OpenMySQLConnection(connectionString string) (*sql.DB, error) {
	config, err := ParseMySQLConnectionString(connectionString)
	if err != nil {
		return nil, err
	}

	return openMySQLConnectionWithConfig(config)
}

func openMySQLConnectionWithConfig(config *mysql.Config) (*sql.DB, error) {
	connector, err := mysql.NewConnector(config)
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)

	return db, nil
}

// A scratch database materializes a SQL source on a temporary server.
type mysqlScratchDatabase struct {
	serverConfig *mysql.Config
	name         string
}

func (d *MySQLDriver) OpenSide(
	ctx context.Context,
	dataSource driversshared.DataSource,
	otherSource driversshared.DataSource,
	role string,
) (*sql.DB, error) {
	connectionString, isConnectionString := driversshared.ConnectionString(dataSource)
	if isConnectionString {
		return OpenMySQLConnection(connectionString)
	}

	path, _ := driversshared.SQLSourcePath(dataSource)

	sqlSource, err := driversshared.NewSQLSource(path)
	if err != nil {
		return nil, err
	}

	sqlSource.BackslashEscapes = true

	serverConfig, err := d.scratchServerConfig(ctx, otherSource)
	if err != nil {
		return nil, err
	}

	scratch := &mysqlScratchDatabase{
		serverConfig: serverConfig,
		name:         driversshared.ScratchDatabaseName(role),
	}

	// The server can create the database after the context stops, so the cleanup drops the
	// name before the creation is known.
	d.scratchDatabases = append(d.scratchDatabases, scratch)

	err = scratch.create(ctx)
	if err != nil {
		return nil, err
	}

	connection, err := scratch.open(scratch.name)
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

func (d *MySQLDriver) scratchServerConfig(
	ctx context.Context,
	otherSource driversshared.DataSource,
) (*mysql.Config, error) {
	if d.ScratchDatabase != "" {
		return ParseMySQLConnectionString(d.ScratchDatabase)
	}

	flavor := embeddedmysql.MySQL
	version := d.ScratchServerVersion

	otherConnectionString, isConnectionString := driversshared.ConnectionString(otherSource)
	if isConnectionString {
		otherVersion, mariadb, err := DetectMySQLScratchVersion(ctx, otherConnectionString)
		if err != nil {
			return nil, fmt.Errorf("failed to read the server version of the database: %w", err)
		}

		if mariadb {
			flavor = embeddedmysql.MariaDB
		}

		if version == "" {
			version = otherVersion
		}
	}

	server, err := d.ScratchServer(flavor, version)
	if err != nil {
		return nil, err
	}

	return ParseMySQLConnectionString(server.DSN())
}

// The flavor and the version apply at the first call only. One diff starts one server.
func (d *MySQLDriver) ScratchServer(flavor embeddedmysql.Flavor, version string) (*embeddedmysql.EmbeddedMySQL, error) {
	if d.scratchServer != nil {
		return d.scratchServer, nil
	}

	// The library sets the logger, the port, and the cache path by itself.
	server := embeddedmysql.NewDatabase(embeddedmysql.DefaultConfig().
		Flavor(flavor).
		Version(version))

	err := server.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start the temporary %s server: %w", flavor, err)
	}

	d.scratchServer = server

	return server, nil
}

func DetectMySQLScratchVersion(ctx context.Context, connectionString string) (string, bool, error) {
	connection, err := OpenMySQLConnection(connectionString)
	if err != nil {
		return "", false, err
	}

	defer func() { _ = connection.Close() }()

	var version string

	err = connection.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	if err != nil {
		return "", false, err
	}

	mariadb := isMariaDB(version)

	// A server writes a build suffix into the version, for example "8.0.43-0ubuntu0.22.04.1",
	// and the download reads the three numbers alone.
	numericVersion, _, _ := strings.Cut(version, "-")

	return numericVersion, mariadb, nil
}

func (d *MySQLDriver) StopScratchServer() error {
	if d.scratchServer == nil {
		return nil
	}

	server := d.scratchServer
	d.scratchServer = nil

	return server.Stop()
}

func (s *mysqlScratchDatabase) create(ctx context.Context) error {
	connection, err := s.open("")
	if err != nil {
		return err
	}

	defer func() { _ = connection.Close() }()

	_, err = connection.ExecContext(ctx, "CREATE DATABASE "+QuoteIdentifier(s.name)+";")
	if err != nil {
		return fmt.Errorf("failed to create the temporary database %q: %w", s.name, err)
	}

	return nil
}

func (s *mysqlScratchDatabase) drop(ctx context.Context) error {
	connection, err := s.open("")
	if err != nil {
		return err
	}

	defer func() { _ = connection.Close() }()

	_, err = connection.ExecContext(ctx, "DROP DATABASE IF EXISTS "+QuoteIdentifier(s.name)+";")
	if err != nil {
		return fmt.Errorf("failed to drop the temporary database %q: %w", s.name, err)
	}

	return nil
}

func (s *mysqlScratchDatabase) open(databaseName string) (*sql.DB, error) {
	config := s.serverConfig.Clone()
	config.DBName = databaseName

	connector, err := mysql.NewConnector(config)
	if err != nil {
		return nil, err
	}

	// A SET statement of a SQL file must reach the next statement, as in OpenMySQLConnection.
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)

	return db, nil
}
