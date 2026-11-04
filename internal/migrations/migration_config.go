package internalmigrations

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"regexp"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"gopkg.in/yaml.v3"
)

const DefaultConfigPath = "dbdiff.yaml"

// The yaml tags must stay equal to the long flag names of the CLI.
type Config struct {
	Driver          driversshared.DriverName `yaml:"driver"`
	Migrations      string                   `yaml:"migrations"`
	Database        string                   `yaml:"database"`
	Desired         string                   `yaml:"desired"`
	Schema          string                   `yaml:"schema"`
	ServerVersion   string                   `yaml:"server-version"`
	ScratchDatabase string                   `yaml:"scratch-database"`
	IgnoreTables    []string                 `yaml:"ignore-table"`
}

var removedConfigKeys = map[string]string{
	"source":  `Use "migrations" for the migration directory`,
	"target":  `Use "database" for the database, or "desired" for the wanted schema of migrate generate`,
	"version": `Use "server-version" for the version of the temporary server`,
	"ignore":  `Use "ignore-table" with a list of table names`,
}

// Only the braced form expands, so a "$" in a password stays literal.
var configVariablePattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

func ReadConfig(path string, optional bool) (*Config, error) {
	content, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) && optional {
		return &Config{}, nil
	}

	if err != nil {
		return nil, err
	}

	var document yaml.Node

	err = yaml.Unmarshal(content, &document)
	if err != nil {
		return nil, err
	}

	if len(document.Content) > 0 {
		mapping := document.Content[0]
		if mapping.Kind != yaml.MappingNode {
			return nil, fmt.Errorf("the configuration file %s must hold a mapping of keys", path)
		}

		for index := range len(mapping.Content) / 2 {
			key := mapping.Content[2*index].Value

			replacement, removed := removedConfigKeys[key]
			if removed {
				return nil, fmt.Errorf("the key %q of %s no longer exists. %s", key, path, replacement)
			}
		}
	}

	config := &Config{}

	decoder := yaml.NewDecoder(bytes.NewReader(content))
	decoder.KnownFields(true)

	err = decoder.Decode(config)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	err = expandConfigVariables(config, path)
	if err != nil {
		return nil, err
	}

	return config, nil
}

type configValue struct {
	key   string
	value *string
}

func expandConfigVariables(config *Config, path string) error {
	driver := string(config.Driver)

	values := []configValue{
		{
			key:   "driver",
			value: &driver,
		},
		{
			key:   "migrations",
			value: &config.Migrations,
		},
		{
			key:   "database",
			value: &config.Database,
		},
		{
			key:   "desired",
			value: &config.Desired,
		},
		{
			key:   "schema",
			value: &config.Schema,
		},
		{
			key:   "server-version",
			value: &config.ServerVersion,
		},
		{
			key:   "scratch-database",
			value: &config.ScratchDatabase,
		},
	}

	for index := range config.IgnoreTables {
		values = append(values, configValue{
			key:   "ignore-table",
			value: &config.IgnoreTables[index],
		})
	}

	for _, entry := range values {
		expanded, err := expandVariables(*entry.value, entry.key, path)
		if err != nil {
			return err
		}

		*entry.value = expanded
	}

	config.Driver = driversshared.DriverName(driver)

	return nil
}

func expandVariables(value string, key string, path string) (string, error) {
	var missingVariable string

	expanded := configVariablePattern.ReplaceAllStringFunc(value, func(match string) string {
		name := configVariablePattern.FindStringSubmatch(match)[1]

		variable, found := os.LookupEnv(name)
		if !found && missingVariable == "" {
			missingVariable = name
		}

		return variable
	})

	if missingVariable != "" {
		return "", fmt.Errorf("the variable %q of the key %q of %s is not set", missingVariable, key, path)
	}

	return expanded, nil
}
