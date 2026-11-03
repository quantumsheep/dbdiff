package internalmigrations

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"gopkg.in/yaml.v3"
)

const DefaultConfigPath = "dbdiff.yaml"

type Config struct {
	Driver  driversshared.DriverName `yaml:"driver"`
	Source  string                   `yaml:"source"`
	Target  string                   `yaml:"target"`
	Schema  string                   `yaml:"schema"`
	Version string                   `yaml:"version"`
	Ignore  IgnoreConfig             `yaml:"ignore"`
}

type IgnoreConfig struct {
	Tables []string `yaml:"tables"`
}

func ReadConfig(path string, optional bool) (*Config, error) {
	content, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) && optional {
		return &Config{}, nil
	}

	if err != nil {
		return nil, err
	}

	config := &Config{}

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
