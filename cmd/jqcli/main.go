package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/chrispump/go-pg-jobqueue/internal/db"
	"github.com/chrispump/go-pg-jobqueue/internal/scenario"
	"gopkg.in/yaml.v3"
)

type appConfig struct {
	Database db.Config `yaml:"database"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register scenarios first (needed for flag validation)
	registry := []scenario.Scenario{
		scenario.NewThroughputLockingScenario(),
		scenario.NewLatencyPollingVsListenScenario(),
		scenario.NewRetryStrategiesScenario(),
		scenario.NewDLQConsistencyScenario(),
	}

	scenarios := make(map[string]scenario.Scenario, len(registry))
	for _, sc := range registry {
		scenarios[sc.Name()] = sc
	}

	allowed := slices.Collect(maps.Keys(scenarios))
	allowedStr := strings.Join(allowed, "|")

	var (
		configPath   string
		scenarioName string
	)

	flag.StringVar(&configPath, "config", "config.yml", "path to config file")
	flag.Func(
		"scenario",
		"Scenario to run (default: run all).\nOne of: "+allowedStr+" or leave empty to run all",
		func(v string) error {
			if v != "" && v != "all" {
				if _, ok := scenarios[v]; !ok {
					return fmt.Errorf("unknown scenario %q (one of: %s)", v, allowedStr)
				}
			}

			scenarioName = v

			return nil
		},
	)
	flag.Parse()

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	dbconn, err := db.NewPostgresDatabase(ctx, db.BuildDSN(cfg.Database))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer dbconn.Close()

	// Migrate the database
	if err := dbconn.Migrate(); err != nil {
		log.Fatalf("Database migration failed: %v", err)
	}

	// Run all scenarios if none specified
	if scenarioName == "" || scenarioName == "all" {
		log.Println("═══════════════════════════════════════════════════════════════")
		log.Println("Running all scenarios sequentially.")
		log.Println("This will take a while. Time for a coffee!")
		log.Println("═══════════════════════════════════════════════════════════════")

		for i, s := range registry {
			log.Printf("Running scenario %d/%d: %s", i+1, len(registry), s.Name())
			log.Printf("Description: %s", s.Describe())

			if _, err := s.Run(ctx, dbconn); err != nil {
				log.Fatalf("Scenario %s failed: %v", s.Name(), err)
			}

			log.Printf("Scenario %s completed", s.Name())
		}

		log.Println("═══════════════════════════════════════════════════════════════")
		log.Println("All scenarios completed successfully")
		log.Println("═══════════════════════════════════════════════════════════════")
		os.Exit(0)
	}

	// Run single scenario
	if s, ok := scenarios[scenarioName]; ok {
		if _, err := s.Run(ctx, dbconn); err != nil {
			log.Fatalf("Scenario failed: %v", err)
		}

		os.Exit(0)
	} else {
		log.Fatalf("Unknown scenario: %s (available: %s)", scenarioName, allowedStr)
	}
}

func loadConfig(path string) (appConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return appConfig{}, fmt.Errorf("open config file: %w", err)
	}
	defer f.Close()

	var cfg appConfig

	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return appConfig{}, fmt.Errorf("decode config: %w", err)
	}

	return cfg, nil
}
