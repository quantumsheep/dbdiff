package migrations

import (
	"bytes"
	"errors"
	"io"
	"os"

	"github.com/quantumsheep/dbdiff/internal/drivers"
	"gopkg.in/yaml.v3"
)

const DefaultMigrationConfigPath = "dbdiff.yaml"

type MigrationConfig struct {
	Driver  drivers.DriverName `yaml:"driver"`
	Source  string             `yaml:"source"`
	Target  string             `yaml:"target"`
	Schema  string             `yaml:"schema"`
	Version string             `yaml:"version"`
}

func ReadMigrationConfig(path string, optional bool) (*MigrationConfig, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) && optional {
		return &MigrationConfig{}, nil
	}

	if err != nil {
		return nil, err
	}

	config := &MigrationConfig{}

	decoder := yaml.NewDecoder(bytes.NewReader(content))
	decoder.KnownFields(true)

	err = decoder.Decode(config)
	if errors.Is(err, io.EOF) {
		return config, nil
	}

	if err != nil {
		return nil, err
	}

	return config, nil
}
