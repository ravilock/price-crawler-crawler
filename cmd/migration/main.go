package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lib/pq"
)

const latestVersion = 2

func migrationFailure(output []byte, databaseURL string) error {
	diagnostic := strings.ReplaceAll(string(output), databaseURL, "[database URL redacted]")
	if parsed, err := url.Parse(databaseURL); err == nil && parsed.User != nil {
		if password, ok := parsed.User.Password(); ok && password != "" {
			for _, secret := range []string{password, url.QueryEscape(password), url.PathEscape(password)} {
				diagnostic = strings.ReplaceAll(diagnostic, secret, "[redacted]")
			}
		}
	}
	diagnostic = strings.TrimSpace(diagnostic)
	if len(diagnostic) > 1000 {
		diagnostic = diagnostic[:1000]
	}
	return fmt.Errorf("crawler migration failed; no rollback or version force was attempted: %s", diagnostic)
}

type schemaVersion struct {
	version int
	dirty   bool
	tracked bool
}

func readVersion(ctx context.Context, db *sql.DB) (schemaVersion, error) {
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('public.schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		return schemaVersion{}, errors.New("cannot inspect crawler migration metadata")
	}
	if !exists {
		return schemaVersion{}, nil
	}
	var count int
	var value schemaVersion
	if err := db.QueryRowContext(ctx, `SELECT count(*), COALESCE(max(version),0), COALESCE(bool_or(dirty),false) FROM public.schema_migrations`).Scan(&count, &value.version, &value.dirty); err != nil {
		return schemaVersion{}, errors.New("cannot read crawler migration version")
	}
	if count > 1 {
		return schemaVersion{}, errors.New("crawler migration metadata contains multiple versions; inspect before recovery")
	}
	value.tracked = count == 1
	return value, nil
}

func reportVersion(value schemaVersion) {
	if !value.tracked {
		fmt.Println("crawler version=none dirty=false")
		return
	}
	fmt.Printf("crawler version=%d dirty=%t\n", value.version, value.dirty)
}

func run(ctx context.Context, action string) error {
	if action != "up" && action != "version" {
		return errors.New("usage: migration up|version")
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return errors.New("crawler migration requires DATABASE_URL")
	}
	db, err := sql.Open("postgres", url)
	if err != nil {
		return errors.New("invalid crawler database configuration")
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		return errors.New("crawler database is unavailable or credentials were rejected")
	}
	version, err := readVersion(ctx, db)
	if err != nil {
		return err
	}
	if action == "version" {
		reportVersion(version)
		return nil
	}
	if version.dirty {
		reportVersion(version)
		return errors.New("dirty crawler migration blocks startup; inspect schema and follow explicit recovery instructions")
	}
	if version.tracked && version.version > latestVersion {
		return fmt.Errorf("crawler database version %d is newer than this release (%d)", version.version, latestVersion)
	}
	if !version.tracked {
		var legacy bool
		tables := []string{"source_domain_config", "crawl_request", "offer", "price_availability_observation", "message_outbox", "message_inbox"}
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname='public' AND tablename=ANY($1::text[]))`, pq.Array(tables)).Scan(&legacy); err != nil {
			return errors.New("cannot inspect unversioned crawler schema")
		}
		if legacy {
			return errors.New("unversioned owned schema detected; compare the retained schema and explicitly adopt its verified version using README instructions")
		}
	}
	// Redact connection details before reporting bounded CLI diagnostics.
	command := exec.CommandContext(ctx, "/usr/local/bin/migrate", "-path", "/migrations", "-database", url, "up")
	if output, err := command.CombinedOutput(); err != nil {
		if failed, readErr := readVersion(ctx, db); readErr == nil {
			reportVersion(failed)
		}
		return migrationFailure(output, url)
	}
	version, err = readVersion(ctx, db)
	if err != nil {
		return err
	}
	reportVersion(version)
	if !version.tracked || version.dirty || version.version != latestVersion {
		return errors.New("crawler migrations did not reach the expected clean release version")
	}
	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: migration up|version")
		os.Exit(1)
	}
	timeout := 120
	if raw := os.Getenv("MIGRATION_TIMEOUT_SECONDS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 3600 {
			fmt.Fprintln(os.Stderr, "MIGRATION_TIMEOUT_SECONDS must be between 1 and 3600")
			os.Exit(1)
		}
		timeout = value
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	if err := run(ctx, os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
