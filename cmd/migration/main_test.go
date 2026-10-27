package main

import (
	"strings"
	"testing"
)

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
