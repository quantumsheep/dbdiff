package driversshared

import (
	"os"
	"strings"
)

// The methods use value receivers, so a pointer to a data source type also satisfies
// DataSource. The code must read a data source only through these methods.
type DataSource interface {
	sqlSourcePath() (string, bool)
	connectionString() (string, bool)
}

type FileDataSource struct {
	Path string
}

type FolderDataSource struct {
	Path string
}

type ConnectionStringDataSource struct {
	ConnectionString string
}

func (dataSource FileDataSource) sqlSourcePath() (string, bool) {
	return dataSource.Path, true
}

func (FileDataSource) connectionString() (string, bool) {
	return "", false
}

func (dataSource FolderDataSource) sqlSourcePath() (string, bool) {
	return dataSource.Path, true
}

func (FolderDataSource) connectionString() (string, bool) {
	return "", false
}

func (ConnectionStringDataSource) sqlSourcePath() (string, bool) {
	return "", false
}

func (dataSource ConnectionStringDataSource) connectionString() (string, bool) {
	return dataSource.ConnectionString, true
}

// A path with "://" never names SQL text, so the scheme check comes first.
func ParseDataSource(argument string) DataSource {
	if strings.Contains(argument, "://") {
		return ConnectionStringDataSource{
			ConnectionString: argument,
		}
	}

	if hasSQLExtension(argument) {
		return FileDataSource{
			Path: argument,
		}
	}

	information, err := os.Stat(argument)
	if err == nil && information.IsDir() {
		return FolderDataSource{
			Path: argument,
		}
	}

	return ConnectionStringDataSource{
		ConnectionString: argument,
	}
}

func SQLSourcePath(dataSource DataSource) (string, bool) {
	return dataSource.sqlSourcePath()
}

func ConnectionString(dataSource DataSource) (string, bool) {
	return dataSource.connectionString()
}
