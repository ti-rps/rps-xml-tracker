// Package migrate applies the embedded goose migrations to Postgres on startup,
// so deploying is just "docker compose up" — no manual goose step on the server.
package migrate

import (
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/stdlib" // pgx database/sql driver for goose
	"github.com/pressly/goose/v3"

	"github.com/EnzzoHosaki/rps-xml-tracker/migrations"
)

// Up opens a database/sql connection (pgx stdlib), runs all pending migrations,
// and closes it. Safe to call on every boot — goose tracks applied versions.
func Up(dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer db.Close()

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	if err := goose.Up(db, "."); err != nil {
		return fmt.Errorf("goose up: %w", err)
	}
	return nil
}

var _ = stdlib.GetDefaultDriver // keep the pgx stdlib import referenced
