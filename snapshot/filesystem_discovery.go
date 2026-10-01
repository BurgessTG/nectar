package snapshot

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	SnapshotFormatLegacy = "legacy"
	SnapshotFormatUTxOHD = "utxo-hd"

	UTxOTableFormatTablesFile = "tables-file"
	UTxOTableFormatTVar       = "tvar"
)

type LedgerStateCandidate struct {
	Path            string
	Slot            uint64
	Format          string
	UTxOTablePath   string
	UTxOTableFormat string
}

type FilesystemSnapshotReport struct {
	Root                  string
	LedgerDir             string
	StatePath             string
	StateSlot             uint64
	StateFormat           string
	UTxOTablePath         string
	UTxOTableSlot         uint64
	UTxOTableFormat       string
	Candidates            []LedgerStateCandidate
	DingoCompatible       bool
	DirectImportSupported bool
	Missing               []string
	Warnings              []string
}

func InspectFilesystemSnapshot(root string) (*FilesystemSnapshotReport, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("snapshot directory is required")
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve snapshot directory: %w", err)
	}
	info, err := os.Stat(absRoot)
	if err != nil {
		return nil, fmt.Errorf("inspect snapshot directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("snapshot path is not a directory: %s", absRoot)
	}

	report := &FilesystemSnapshotReport{
		Root: absRoot,
	}

	ledgerDir, err := findFilesystemLedgerDir(absRoot)
	if err != nil {
		report.Missing = append(report.Missing, "ledger/ or db/ledger/")
		report.Warnings = append(report.Warnings, err.Error())
		return report, nil
	}
	report.LedgerDir = ledgerDir

	entries, err := os.ReadDir(ledgerDir)
	if err != nil {
		return nil, fmt.Errorf("read ledger directory: %w", err)
	}

	var utxoHDCandidates []LedgerStateCandidate
	var legacyCandidates []LedgerStateCandidate
	var tableCandidates []LedgerStateCandidate
	for _, entry := range entries {
		name := entry.Name()
		fullPath := filepath.Join(ledgerDir, name)
		if entry.IsDir() {
			slot, ok := parseLedgerSlot(name)
			if !ok {
				continue
			}
			tablePath, tableFormat := findFilesystemUTxOTable(fullPath)
			if tablePath != "" {
				tableCandidates = append(tableCandidates, LedgerStateCandidate{
					Path:            filepath.Join(fullPath, "state"),
					Slot:            slot,
					Format:          SnapshotFormatUTxOHD,
					UTxOTablePath:   tablePath,
					UTxOTableFormat: tableFormat,
				})
			}
			statePath := filepath.Join(fullPath, "state")
			if fileExists(statePath) {
				utxoHDCandidates = append(utxoHDCandidates, LedgerStateCandidate{
					Path:            statePath,
					Slot:            slot,
					Format:          SnapshotFormatUTxOHD,
					UTxOTablePath:   tablePath,
					UTxOTableFormat: tableFormat,
				})
			}
			continue
		}

		slot, ok := parseLegacyLedgerStateSlot(name)
		if !ok {
			continue
		}
		legacyCandidates = append(legacyCandidates, LedgerStateCandidate{
			Path:   fullPath,
			Slot:   slot,
			Format: SnapshotFormatLegacy,
		})
	}

	sortLedgerCandidates(utxoHDCandidates)
	sortLedgerCandidates(legacyCandidates)
	sortLedgerCandidates(tableCandidates)

	report.Candidates = append(report.Candidates, utxoHDCandidates...)
	report.Candidates = append(report.Candidates, legacyCandidates...)

	switch {
	case len(utxoHDCandidates) > 0:
		applySelectedLedgerState(report, utxoHDCandidates[0])
	case len(legacyCandidates) > 0:
		applySelectedLedgerState(report, legacyCandidates[0])
	default:
		report.Missing = append(report.Missing, "ledger state file")
		report.Warnings = append(report.Warnings, "no Dingo-compatible ledger state file found")
	}

	if report.UTxOTablePath == "" && len(tableCandidates) > 0 {
		report.UTxOTablePath = tableCandidates[0].UTxOTablePath
		report.UTxOTableSlot = tableCandidates[0].Slot
		report.UTxOTableFormat = tableCandidates[0].UTxOTableFormat
	}
	if report.StatePath != "" {
		report.DingoCompatible = true
	}
	if report.StatePath != "" && report.UTxOTablePath != "" && report.StateSlot != report.UTxOTableSlot {
		report.Warnings = append(report.Warnings, "highest ledger state slot differs from highest UTxO table slot")
	}
	if report.StatePath != "" && report.UTxOTablePath == "" && report.StateFormat == SnapshotFormatUTxOHD {
		report.Warnings = append(report.Warnings, "UTxO-HD state found without a separate UTxO table file")
	}
	if report.UTxOTablePath != "" && report.UTxOTableFormat == UTxOTableFormatTVar {
		report.DirectImportSupported = true
	} else if report.StatePath != "" {
		report.Warnings = append(report.Warnings, "direct filesystem apply is supported only for UTxO-HD tables/tvar; legacy ledger-state decode is not implemented yet")
	}

	return report, nil
}

func applySelectedLedgerState(report *FilesystemSnapshotReport, candidate LedgerStateCandidate) {
	report.StatePath = candidate.Path
	report.StateSlot = candidate.Slot
	report.StateFormat = candidate.Format
	report.UTxOTablePath = candidate.UTxOTablePath
	report.UTxOTableSlot = candidate.Slot
	report.UTxOTableFormat = candidate.UTxOTableFormat
}

func findFilesystemLedgerDir(root string) (string, error) {
	candidates := []string{
		filepath.Join(root, "ledger"),
		filepath.Join(root, "db", "ledger"),
	}
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err == nil && info.IsDir() {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("ledger directory not found in %s", root)
}

func findFilesystemUTxOTable(slotDir string) (string, string) {
	candidates := []struct {
		path   string
		format string
	}{
		{path: filepath.Join(slotDir, "tables"), format: UTxOTableFormatTablesFile},
		{path: filepath.Join(slotDir, "tables", "tvar"), format: UTxOTableFormatTVar},
	}
	for _, candidate := range candidates {
		info, err := os.Stat(candidate.path)
		if err == nil && !info.IsDir() {
			return candidate.path, candidate.format
		}
	}
	return "", ""
}

func sortLedgerCandidates(candidates []LedgerStateCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Slot == candidates[j].Slot {
			return candidates[i].Path < candidates[j].Path
		}
		return candidates[i].Slot > candidates[j].Slot
	})
}

func parseLegacyLedgerStateSlot(name string) (uint64, bool) {
	if strings.HasSuffix(name, ".checksum") ||
		strings.HasSuffix(name, ".lock") ||
		strings.HasSuffix(name, ".tmp") {
		return 0, false
	}
	name = strings.TrimSuffix(name, ".lstate")
	name = strings.TrimSuffix(name, "_snapshot")
	return parseLedgerSlot(name)
}

func parseLedgerSlot(name string) (uint64, bool) {
	if name == "" {
		return 0, false
	}
	slot, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return 0, false
	}
	return slot, true
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
