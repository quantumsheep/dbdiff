package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

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
	config.ParseTime = true

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

	config := mysql.NewConfig()
	config.Net = "tcp"
	config.User = parsed.User.Username()
	config.Passwd, _ = parsed.User.Password()
	config.DBName = strings.TrimPrefix(parsed.Path, "/")

	address := parsed.Host
	if parsed.Port() == "" {
		address += ":3306"
	}

	config.Addr = address

	for key, values := range parsed.Query() {
		if config.Params == nil {
			config.Params = make(map[string]string)
		}

		config.Params[key] = values[0]
	}

	return config, nil
}

// SET FOREIGN_KEY_CHECKS changes one session, and a pool with several connections runs
// the next statement on another session. One connection keeps the setting.
func OpenMySQLConnection(connectionString string) (*sql.DB, error) {
	config, err := ParseMySQLConnectionString(connectionString)
	if err != nil {
		return nil, err
	}

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

func (d *MySQLDriver) OpenSide(ctx context.Context, source driversshared.DataSource,
	otherSource driversshared.DataSource, role string) (*sql.DB, error) {
	connectionSource, isConnectionString := source.(driversshared.ConnectionStringDataSource)
	if isConnectionString {
		return OpenMySQLConnection(connectionSource.ConnectionString)
	}

	path, _ := driversshared.SQLSourcePath(source)

	sqlSource, err := driversshared.NewSQLSource(path)
	if err != nil {
		return nil, err
	}

	serverConfig, err := d.scratchServerConfig(ctx, otherSource)
	if err != nil {
		return nil, err
	}

	scratch := &mysqlScratchDatabase{
		serverConfig: serverConfig,
		name:         fmt.Sprintf("dbdiff_%s_%d", role, time.Now().UnixNano()),
	}

	err = scratch.create(ctx)
	if err != nil {
		return nil, err
	}

	d.scratchDatabases = append(d.scratchDatabases, scratch)

	connection, err := scratch.open()
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

func (d *MySQLDriver) scratchServerConfig(ctx context.Context, otherSource driversshared.DataSource) (*mysql.Config, error) {
	flavor := embeddedmysql.MySQL
	version := d.ScratchServerVersion

	otherConnectionSource, isConnectionString := otherSource.(driversshared.ConnectionStringDataSource)
	if isConnectionString {
		otherVersion, mariadb := DetectMySQLScratchVersion(ctx, otherConnectionSource.ConnectionString)
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

func DetectMySQLScratchVersion(ctx context.Context, connectionString string) (string, bool) {
	connection, err := OpenMySQLConnection(connectionString)
	if err != nil {
		return "", false
	}

	defer func() { _ = connection.Close() }()

	var version string

	err = connection.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	if err != nil {
		return "", false
	}

	mariadb := strings.Contains(strings.ToLower(version), "mariadb")

	// A server writes a build suffix into the version, for example "8.0.43-0ubuntu0.22.04.1",
	// and the download reads the three numbers alone.
	numericVersion, _, _ := strings.Cut(version, "-")

	return numericVersion, mariadb
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
	connection, err := s.openServer()
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

func (s *mysqlScratchDatabase) drop() error {
	connection, err := s.openServer()
	if err != nil {
		return err
	}

	defer func() { _ = connection.Close() }()

	_, err = connection.Exec("DROP DATABASE " + QuoteIdentifier(s.name) + ";")
	if err != nil {
		return fmt.Errorf("failed to drop the temporary database %q: %w", s.name, err)
	}

	return nil
}

func (s *mysqlScratchDatabase) openServer() (*sql.DB, error) {
	config := s.serverConfig.Clone()
	config.DBName = ""

	connector, err := mysql.NewConnector(config)
	if err != nil {
		return nil, err
	}

	return sql.OpenDB(connector), nil
}

func (s *mysqlScratchDatabase) open() (*sql.DB, error) {
	config := s.serverConfig.Clone()
	config.DBName = s.name

	connector, err := mysql.NewConnector(config)
	if err != nil {
		return nil, err
	}

	return sql.OpenDB(connector), nil
}
