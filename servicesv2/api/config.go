package api

import (
	"net/url"
	"os/user"
	fp "path/filepath"
)

type Config struct {
	BindAddr string `toml:"v2-bind-addr"`
	BoltFile string `toml:"v2-bolt-file"`
}

func NewConfig() Config {
	user, _ := user.Current()
	boltFile := fp.Join(user.HomeDir, ".influxdbv2", "influxd.bolt")
	return Config{
		BindAddr: "localhost:9999",
		BoltFile: boltFile,
	}
}

func (c *Config) Validate() error {
	// confirm that BindAddr is a valid URL
	if err := c.ValidateBindAddr(); err != nil {
		return err
	}

	return nil
}

func (c *Config) ValidateBindAddr() error {
	if _, err := url.ParseRequestURI(c.BindAddr); err != nil {
		return err
	}
	return nil
}
