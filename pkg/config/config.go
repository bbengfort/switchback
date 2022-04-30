package config

import (
	"github.com/kelseyhightower/envconfig"
	"github.com/rs/zerolog"
)

type Config struct {
	Maintenance bool         `split_words:"true" default:"false"`
	LogLevel    LevelDecoder `split_words:"true" default:"info"`
	ConsoleLog  bool         `split_words:"true" default:"false"`
	BindAddr    string       `split_words:"true" default:":7773"`
	processed   bool
}

func New() (_ Config, err error) {
	var conf Config
	if err = envconfig.Process("switchback", &conf); err != nil {
		return Config{}, err
	}

	// Validate config-specific constraints
	if err = conf.Validate(); err != nil {
		return Config{}, err
	}

	conf.processed = true
	return conf, nil
}

func (c Config) GetLogLevel() zerolog.Level {
	return zerolog.Level(c.LogLevel)
}

func (c Config) IsZero() bool {
	return !c.processed
}

func (c Config) Validate() error {
	return nil
}
