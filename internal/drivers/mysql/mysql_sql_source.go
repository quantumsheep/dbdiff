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

// A scratch database materializes a SQL source on the server of the other side, because
// no library gives a temporary MySQL server.
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

	if driversshared.IsSQLSource(otherSource) {
		return nil, fmt.Errorf("the mysql driver builds a SQL source on the server of the other side. Give a database as the other argument")
	}

	path, _ := driversshared.SQLSourcePath(source)

	sqlSource, err := driversshared.NewSQLSource(path)
	if err != nil {
		return nil, err
	}

	otherConnectionSource := otherSource.(driversshared.ConnectionStringDataSource)

	serverConfig, err := ParseMySQLConnectionString(otherConnectionSource.ConnectionString)
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
