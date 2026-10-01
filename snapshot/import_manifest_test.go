package snapshot

import "testing"

func TestBuildImportManifestMarksGraphsAndRewardsOutOfSnapshotPath(t *testing.T) {
	manifest := BuildImportManifest(ImportManifestOptions{
		Source:    "utxorpc",
		Endpoint:  "http://127.0.0.1:50051",
		Apply:     true,
		LiveReady: true,
	})

	assertTableStatus(t, manifest, "token_holders", CoverageApplied)
	assertTableStatus(t, manifest, "wallet_connections", CoverageRequiresChainSync)
	assertTableStatus(t, manifest, "token_wallet_connections", CoverageRequiresChainSync)
	assertTableStatus(t, manifest, "token_metadata", CoverageExternal)
	assertTableStatus(t, manifest, "rewards", CoverageExcluded)
}

func TestBuildImportManifestKeepsFilesystemDiscoveryOnly(t *testing.T) {
	report := &FilesystemSnapshotReport{
		DingoCompatible: true,
		LedgerDir:       "/snapshot/ledger",
		StatePath:       "/snapshot/ledger/200/state",
		StateSlot:       200,
		StateFormat:     SnapshotFormatUTxOHD,
		UTxOTablePath:   "/snapshot/ledger/200/tables",
		UTxOTableSlot:   200,
		UTxOTableFormat: UTxOTableFormatTablesFile,
	}

	manifest := BuildImportManifest(ImportManifestOptions{
		Source:           "filesystem",
		FilesystemReport: report,
	})

	if manifest.Filesystem == nil || !manifest.Filesystem.DingoCompatible {
		t.Fatalf("expected Dingo-compatible filesystem manifest")
	}
	assertTableStatus(t, manifest, "token_holders", CoverageDiscoveryOnly)
	assertTableStatus(t, manifest, "wallet_connections", CoverageRequiresChainSync)
	assertTableStatus(t, manifest, "rewards", CoverageExcluded)
}

func TestBuildImportManifestMarksParsedNDJSONAsCurrentUTxOSource(t *testing.T) {
	manifest := BuildImportManifest(ImportManifestOptions{
		Source:       "dingo-ndjson",
		SnapshotFile: "/tmp/current-utxo.ndjson",
		Apply:        true,
	})

	if manifest.Source != "ndjson" {
		t.Fatalf("source = %q, want ndjson", manifest.Source)
	}
	if manifest.SnapshotFile != "/tmp/current-utxo.ndjson" {
		t.Fatalf("snapshot file = %q", manifest.SnapshotFile)
	}
	assertTableStatus(t, manifest, "token_holders", CoverageApplied)
	assertTableStatus(t, manifest, "wallet_connections", CoverageRequiresChainSync)
	assertTableStatus(t, manifest, "rewards", CoverageExcluded)
}

func TestBuildImportManifestMarksTVarAsDirectCurrentUTxOSource(t *testing.T) {
	manifest := BuildImportManifest(ImportManifestOptions{
		Source:                          "filesystem-tvar",
		SnapshotFile:                    "/snapshot/ledger/200/tables/tvar",
		Apply:                           true,
		DirectFilesystemDecodeSupported: true,
	})

	if manifest.Source != "tvar" {
		t.Fatalf("source = %q, want tvar", manifest.Source)
	}
	if !manifest.DirectFilesystemDecodeSupported {
		t.Fatalf("expected direct filesystem decode support")
	}
	assertTableStatus(t, manifest, "token_holders", CoverageApplied)
	assertTableStatus(t, manifest, "token_wallet_connections", CoverageRequiresChainSync)
	assertTableStatus(t, manifest, "rewards", CoverageExcluded)
}

func assertTableStatus(t *testing.T, manifest ImportManifest, table string, status string) {
	t.Helper()
	for _, coverage := range manifest.Tables {
		if coverage.Table == table {
			if coverage.Status != status {
				t.Fatalf("table %s status = %s, want %s", table, coverage.Status, status)
			}
			return
		}
	}
	t.Fatalf("table %s not found in manifest", table)
}
