package snapshot

import "time"

const (
	CoverageReadyAfterFullApply = "ready_after_full_apply"
	CoverageApplied             = "applied"
	CoverageDiscoveryOnly       = "discovery_only"
	CoverageRequiresChainSync   = "requires_chainsync"
	CoverageExternal            = "external"
	CoverageExcluded            = "excluded"
)

type ImportManifestOptions struct {
	Source                          string
	DolosDir                        string
	SnapshotDir                     string
	SnapshotFile                    string
	Endpoint                        string
	Apply                           bool
	Partial                         bool
	LiveReady                       bool
	FilesystemReport                *FilesystemSnapshotReport
	Warnings                        []string
	DirectFilesystemDecodeSupported bool
}

type ImportManifest struct {
	GeneratedAt                     time.Time           `json:"generated_at"`
	Source                          string              `json:"source"`
	DolosDir                        string              `json:"dolos_dir,omitempty"`
	SnapshotDir                     string              `json:"snapshot_dir,omitempty"`
	SnapshotFile                    string              `json:"snapshot_file,omitempty"`
	Endpoint                        string              `json:"endpoint,omitempty"`
	LiveReady                       bool                `json:"live_ready"`
	Apply                           bool                `json:"apply"`
	Partial                         bool                `json:"partial"`
	FullUnfilteredRequired          bool                `json:"full_unfiltered_required"`
	DirectFilesystemDecodeSupported bool                `json:"direct_filesystem_decode_supported"`
	Filesystem                      *FilesystemManifest `json:"filesystem,omitempty"`
	Tables                          []TableCoverage     `json:"tables"`
	Warnings                        []string            `json:"warnings,omitempty"`
	NextLiveGates                   []string            `json:"next_live_gates,omitempty"`
}

type FilesystemManifest struct {
	DingoCompatible bool   `json:"dingo_compatible"`
	LedgerDir       string `json:"ledger_dir,omitempty"`
	StatePath       string `json:"state_path,omitempty"`
	StateSlot       uint64 `json:"state_slot,omitempty"`
	StateFormat     string `json:"state_format,omitempty"`
	UTxOTablePath   string `json:"utxo_table_path,omitempty"`
	UTxOTableSlot   uint64 `json:"utxo_table_slot,omitempty"`
	UTxOTableFormat string `json:"utxo_table_format,omitempty"`
}

type TableCoverage struct {
	Table       string   `json:"table"`
	Status      string   `json:"status"`
	PopulatedBy string   `json:"populated_by,omitempty"`
	Requires    []string `json:"requires,omitempty"`
	Notes       []string `json:"notes,omitempty"`
}

func BuildImportManifest(options ImportManifestOptions) ImportManifest {
	source := normalizeSource(options.Source)
	nextLiveGates := []string{}
	if options.LiveReady {
		nextLiveGates = append(nextLiveGates, "Dolos socket and UTxO RPC live readiness are confirmed")
	} else {
		nextLiveGates = append(nextLiveGates,
			"wait for Dolos snapshot bootstrap to finish",
			"start Dolos daemon after user go-ahead",
			"confirm bootstrap-dolos --require-socket",
		)
	}
	nextLiveGates = append(nextLiveGates,
		"run full unfiltered snapshot-import --apply --replace-utxo from parsed Dingo-style UTxO NDJSON or UTxO-HD tables/tvar",
		"use Dolos UTxO RPC only for filtered checks unless complete enumeration is explicitly proven",
		"run ChainSync for historical wallet graph tables",
	)

	manifest := ImportManifest{
		GeneratedAt:                     time.Now().UTC(),
		Source:                          source,
		DolosDir:                        options.DolosDir,
		SnapshotDir:                     options.SnapshotDir,
		SnapshotFile:                    options.SnapshotFile,
		Endpoint:                        options.Endpoint,
		LiveReady:                       options.LiveReady,
		Apply:                           options.Apply,
		Partial:                         options.Partial,
		FullUnfilteredRequired:          true,
		DirectFilesystemDecodeSupported: options.DirectFilesystemDecodeSupported,
		Warnings:                        append([]string{}, options.Warnings...),
		NextLiveGates:                   nextLiveGates,
	}

	if options.FilesystemReport != nil {
		report := options.FilesystemReport
		manifest.Filesystem = &FilesystemManifest{
			DingoCompatible: report.DingoCompatible,
			LedgerDir:       report.LedgerDir,
			StatePath:       report.StatePath,
			StateSlot:       report.StateSlot,
			StateFormat:     report.StateFormat,
			UTxOTablePath:   report.UTxOTablePath,
			UTxOTableSlot:   report.UTxOTableSlot,
			UTxOTableFormat: report.UTxOTableFormat,
		}
		manifest.Warnings = append(manifest.Warnings, report.Warnings...)
	}

	manifest.Tables = tableCoverageForSource(source, options.Apply, options.Partial)
	return manifest
}

func tableCoverageForSource(source string, apply bool, partial bool) []TableCoverage {
	if source == "filesystem" {
		return []TableCoverage{
			{
				Table:       "tx_outs",
				Status:      CoverageDiscoveryOnly,
				PopulatedBy: "Dingo-style ledger-state/UTxO-HD file discovery",
				Requires:    []string{"direct filesystem ledger-state decoder"},
			},
			{
				Table:       "ma_tx_outs",
				Status:      CoverageDiscoveryOnly,
				PopulatedBy: "Dingo-style ledger-state/UTxO-HD file discovery",
				Requires:    []string{"direct filesystem ledger-state decoder"},
			},
			{
				Table:       "multi_assets",
				Status:      CoverageDiscoveryOnly,
				PopulatedBy: "Dingo-style ledger-state/UTxO-HD file discovery",
				Requires:    []string{"direct filesystem ledger-state decoder"},
			},
			{
				Table:    "token_holders",
				Status:   CoverageDiscoveryOnly,
				Requires: []string{"tx_outs", "ma_tx_outs", "direct filesystem ledger-state decoder"},
			},
			graphCoverage("wallet_connections"),
			graphCoverage("token_wallet_connections"),
			metadataCoverage(),
			rewardCoverage(),
		}
	}

	populatedBy := "Dolos UTxO RPC SearchUtxos current UTxO set"
	requires := []string{"Dolos UTxO RPC live endpoint"}
	if source == "ndjson" {
		populatedBy = "parsed Dingo-style current UTxO NDJSON"
		requires = []string{"parsed Dingo-style current UTxO NDJSON file"}
	}
	if source == "tvar" {
		populatedBy = "Dingo-compatible UTxO-HD ledger/<slot>/tables/tvar"
		requires = []string{"UTxO-HD tables/tvar file"}
	}

	holderStatus := CoverageReadyAfterFullApply
	if apply && !partial {
		holderStatus = CoverageApplied
	}
	if partial {
		holderStatus = CoverageDiscoveryOnly
	}

	return []TableCoverage{
		{
			Table:       "tx_outs",
			Status:      holderStatus,
			PopulatedBy: populatedBy,
			Requires:    requires,
			Notes:       []string{"full unfiltered apply required for chart data"},
		},
		{
			Table:       "ma_tx_outs",
			Status:      holderStatus,
			PopulatedBy: populatedBy,
			Requires:    requires,
			Notes:       []string{"full unfiltered apply required for chart data"},
		},
		{
			Table:       "multi_assets",
			Status:      holderStatus,
			PopulatedBy: populatedBy,
			Requires:    requires,
		},
		{
			Table:       "token_holders",
			Status:      holderStatus,
			PopulatedBy: "RefreshTokenHoldersFromCurrentUTxO",
			Requires:    []string{"tx_outs", "ma_tx_outs", "full unfiltered apply"},
			Notes:       []string{"feeds holder distribution, allTokens, walletTokens, whale count, and top-10 concentration"},
		},
		graphCoverage("wallet_connections"),
		graphCoverage("token_wallet_connections"),
		metadataCoverage(),
		rewardCoverage(),
	}
}

func graphCoverage(table string) TableCoverage {
	return TableCoverage{
		Table:    table,
		Status:   CoverageRequiresChainSync,
		Requires: []string{"Dolos ouroboros socket", "historical ChainSync processing"},
		Notes:    []string{"current UTxO snapshots do not contain transaction movement history"},
	}
}

func metadataCoverage() TableCoverage {
	return TableCoverage{
		Table:       "token_metadata",
		Status:      CoverageExternal,
		PopulatedBy: "backend Koios/cache path",
		Notes:       []string{"snapshot import does not fabricate token metadata"},
	}
}

func rewardCoverage() TableCoverage {
	return TableCoverage{
		Table:    "rewards",
		Status:   CoverageExcluded,
		Requires: []string{"defensible reward source or implemented state-query/reward calculation"},
		Notes:    []string{"snapshot import does not write placeholder reward data"},
	}
}

func normalizeSource(source string) string {
	switch source {
	case "", "utxorpc":
		return "utxorpc"
	case "ndjson", "dingo-ndjson":
		return "ndjson"
	case "tvar", "filesystem-tvar", "dingo-tvar":
		return "tvar"
	case "filesystem":
		return "filesystem"
	default:
		return source
	}
}
