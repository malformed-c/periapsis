package config_test

import (
	"testing"

	"github.com/malformed-c/periapsis/internal/config"

	"github.com/sanity-io/litter"
)

func TestLoad(t *testing.T) {
	rawConfig, err := config.Load("../../configs/perigeos.toml")
	if err != nil {
		t.Skip("perigeos.toml not found, skipping")
	}

	litter.Dump(rawConfig)
}

func TestProcess(t *testing.T) {
	rawConfig, err := config.Load("../../configs/perigeos.toml")
	if err != nil {
		t.Skip("perigeos.toml not found, skipping")
	}

	cfg, err := rawConfig.Process("/var/lib/apsis/perigeos")
	if err != nil {
		t.Fatal(err)
	}

	litter.Dump(cfg)
}
