package driversshared

import (
	"os"
	"strings"
)

type DataSource interface {
	dataSource()
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

func (FileDataSource) dataSource() {}

func (FolderDataSource) dataSource() {}

func (ConnectionStringDataSource) dataSource() {}

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

func IsSQLSource(dataSource DataSource) bool {
	_, ok := SQLSourcePath(dataSource)

	return ok
}

func SQLSourcePath(dataSource DataSource) (string, bool) {
	switch value := dataSource.(type) {
	case FileDataSource:
		return value.Path, true
	case FolderDataSource:
		return value.Path, true
	default:
		return "", false
	}
}
