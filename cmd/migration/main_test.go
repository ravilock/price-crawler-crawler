package main

import (
	"os"
	"price-crawler/services/crawler/internal/migrationversion"
	"strconv"
	"strings"
	"testing"
)

func TestReleaseVersionMatchesLatestUpMigration(t *testing.T) {
	files, err := os.ReadDir("../../migrations")
	if err != nil {
		t.Fatal(err)
	}
	latest := 0
	for _, file := range files {
		name := file.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		prefix, _, ok := strings.Cut(name, "_")
		if !ok {
			t.Fatalf("migration file lacks numeric version: %s", name)
		}
		version, err := strconv.Atoi(prefix)
		if err != nil {
			t.Fatalf("invalid migration version in %s", name)
		}
		if version > latest {
			latest = version
		}
	}
	if migrationversion.Expected != latest {
		t.Fatalf("release version=%d, latest up migration=%d", migrationversion.Expected, latest)
	}
}

func TestMigrationFailureKeepsCauseWithoutCredentials(t *testing.T) {
	dsn := "postgres://crawler:p%40ss%2Bword@db:5432/crawler"
	output := []byte("migration version 3: syntax error; connection " + dsn + "; password p@ss+word encoded p%40ss%2Bword")
	message := migrationFailure(output, dsn).Error()
	for _, secret := range []string{dsn, "p@ss+word", "p%40ss%2Bword"} {
		if strings.Contains(message, secret) {
			t.Fatal("migration diagnostics disclosed configured credentials")
		}
	}
	if !strings.Contains(message, "syntax error") || !strings.Contains(message, "version 3") {
		t.Fatal("migration diagnostics lost the failure cause or version")
	}
}

func TestMigrationFailureBoundsDiagnosticsAfterRedaction(t *testing.T) {
	message := migrationFailure([]byte(strings.Repeat("x", 10000)), "postgres://db/crawler").Error()
	if len(message) > 1200 {
		t.Fatal("unbounded migration diagnostics")
	}
}
