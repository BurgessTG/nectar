package snapshot

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInspectFilesystemSnapshotPrefersHighestUTxOHDState(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "ledger", "100", "state"))
	mustWriteFile(t, filepath.Join(dir, "ledger", "100", "tables", "tvar"))
	mustWriteFile(t, filepath.Join(dir, "ledger", "200", "state"))
	mustWriteFile(t, filepath.Join(dir, "ledger", "200", "tables"))
	mustWriteFile(t, filepath.Join(dir, "ledger", "300.lstate"))

	report, err := InspectFilesystemSnapshot(dir)
	if err != nil {
		t.Fatalf("InspectFilesystemSnapshot returned error: %v", err)
	}

	if !report.DingoCompatible {
		t.Fatalf("expected Dingo-compatible snapshot")
	}
	if report.StateSlot != 200 {
		t.Fatalf("expected state slot 200, got %d", report.StateSlot)
	}
	if report.StateFormat != SnapshotFormatUTxOHD {
		t.Fatalf("expected UTxO-HD format, got %q", report.StateFormat)
	}
	if report.UTxOTableFormat != UTxOTableFormatTablesFile {
		t.Fatalf("expected current tables file, got %q", report.UTxOTableFormat)
	}
	if report.DirectImportSupported {
		t.Fatalf("direct filesystem import should not be marked supported yet")
	}
}

func TestInspectFilesystemSnapshotFindsDBSubdirLegacyState(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "db", "ledger", "90_snapshot"))
	mustWriteFile(t, filepath.Join(dir, "db", "ledger", "100.lstate"))

	report, err := InspectFilesystemSnapshot(dir)
	if err != nil {
		t.Fatalf("InspectFilesystemSnapshot returned error: %v", err)
	}

	if report.LedgerDir != filepath.Join(dir, "db", "ledger") {
		t.Fatalf("unexpected ledger dir %q", report.LedgerDir)
	}
	if report.StateSlot != 100 {
		t.Fatalf("expected state slot 100, got %d", report.StateSlot)
	}
	if report.StateFormat != SnapshotFormatLegacy {
		t.Fatalf("expected legacy format, got %q", report.StateFormat)
	}
	if !report.DingoCompatible {
		t.Fatalf("expected Dingo-compatible legacy snapshot")
	}
}

func TestInspectFilesystemSnapshotMarksTVarDirectImportSupported(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "ledger", "200", "state"))
	mustWriteFile(t, filepath.Join(dir, "ledger", "200", "tables", "tvar"))

	report, err := InspectFilesystemSnapshot(dir)
	if err != nil {
		t.Fatalf("InspectFilesystemSnapshot returned error: %v", err)
	}

	if !report.DirectImportSupported {
		t.Fatalf("expected tables/tvar import support")
	}
	if report.UTxOTableFormat != UTxOTableFormatTVar {
		t.Fatalf("expected tvar format, got %q", report.UTxOTableFormat)
	}
}

func TestInspectFilesystemSnapshotReportsMissingLedgerDir(t *testing.T) {
	report, err := InspectFilesystemSnapshot(t.TempDir())
	if err != nil {
		t.Fatalf("InspectFilesystemSnapshot returned error: %v", err)
	}
	if report.DingoCompatible {
		t.Fatalf("expected snapshot to be incompatible")
	}
	if len(report.Missing) == 0 {
		t.Fatalf("expected missing ledger report")
	}
}

func mustWriteFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
