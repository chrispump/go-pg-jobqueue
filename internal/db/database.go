package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

const MaxConns = 100

// Config holds the database connection configuration.
type Config struct {
	Hosts    []string `yaml:"hosts"`
	Port     int      `yaml:"port"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	Name     string   `yaml:"name"`
	SSLMode  string   `yaml:"sslmode"`
}

// BuildDSN constructs a PostgreSQL DSN from the given Config.
// Multiple hosts are joined with commas for pgx multi-host failover.
func BuildDSN(c Config) string {
	port := strconv.Itoa(c.Port)
	hostports := make([]string, 0, len(c.Hosts))

	for _, h := range c.Hosts {
		hostports = append(hostports, net.JoinHostPort(h, port))
	}

	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s",
		c.User, c.Password, strings.Join(hostports, ","), c.Name, c.SSLMode)
}

type DB struct {
	Pool *pgxpool.Pool
}

// NewPostgresDatabase creates a new Postgres database connection pool.
func NewPostgresDatabase(ctx context.Context, dsn string) (*DB, error) {
	// configure pgx connection pool
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("unable to parse config: %w", err)
	}

	// set maximum connections
	config.MaxConns = int32(MaxConns)

	// create connection pool from config
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	// verify connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()

		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	return &DB{Pool: pool}, nil
}

// Close closes the database connection pool.
func (db *DB) Close() {
	db.Pool.Close()
}

// Migrate applies all pending up-migrations. Safe to call concurrently.
func (db *DB) Migrate() error {
	return db.runMigrations(false)
}

// ResetAndMigrate drops the schema and re-applies all migrations from scratch.
// Only use this in tests — not safe to call concurrently across packages.
func (db *DB) ResetAndMigrate() error {
	return db.runMigrations(true)
}

func (db *DB) TruncateJobsTable(ctx context.Context) error {
	_, err := db.Pool.Exec(ctx, "TRUNCATE TABLE jobs")
	if err != nil {
		return fmt.Errorf("failed to truncate jobs table: %w", err)
	}

	return nil
}

// StdlibDB returns a database/sql DB from the pgx connection pool.
func (db *DB) StdlibDB() *sql.DB {
	return stdlib.OpenDBFromPool(db.Pool)
}

func (db *DB) runMigrations(reset bool) error {
	// create stdlib DB from pgx pool for golang-migrate compatibility
	sqlDB := db.StdlibDB()
	defer sqlDB.Close()

	// create pgsql migrate driver
	driver, err := postgres.WithInstance(sqlDB, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("failed to create migrate driver: %w", err)
	}

	// create iofs source from embedded migrations
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	// create new pgsql migration from embedded migrations to the specified database
	m, err := migrate.NewWithInstance("iofs", src, "postgres", driver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}
	defer m.Close()

	// force clear dirty state in case of previous failed migration
	if v, dirty, verr := m.Version(); verr == nil && dirty {
		if err := m.Force(int(v)); err != nil {
			return fmt.Errorf("failed to force dirty migration version %d: %w", v, err)
		}
	}

	if reset {
		if err = m.Down(); err != nil && err != migrate.ErrNoChange {
			return fmt.Errorf("failed to roll back migrations: %w", err)
		}
	}

	// migrate up to latest version
	if err = m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	return nil
}
