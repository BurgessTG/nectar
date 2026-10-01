package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"nectar/snapshot"
)

func TestSnapshotImportFilesystemDryScansTVarThroughCommand(t *testing.T) {
	root := t.TempDir()
	slotDir := filepath.Join(root, "ledger", "200")
	tablesDir := filepath.Join(slotDir, "tables")
	if err := os.MkdirAll(tablesDir, 0755); err != nil {
		t.Fatalf("mkdir snapshot layout: %v", err)
	}
	if err := os.WriteFile(filepath.Join(slotDir, "state"), []byte("state"), 0644); err != nil {
		t.Fatalf("write state marker: %v", err)
	}

	txHash := bytes.Repeat([]byte{0xaa}, 32)
	address := append([]byte{0x61}, bytes.Repeat([]byte{0x11}, 28)...)
	policy := bytes.Repeat([]byte{0xbb}, 28)
	tvar := buildCommandTestTVar(txHash, 1, buildCommandTestMempackTxOut(address, 1_000_000, policy, []byte("A"), 5))
	tvarPath := filepath.Join(tablesDir, "tvar")
	if err := os.WriteFile(tvarPath, tvar, 0644); err != nil {
		t.Fatalf("write tvar fixture: %v", err)
	}

	manifestPath := filepath.Join(root, "manifest.json")
	output := captureStdout(t, func() {
		handleSnapshotImportCommand([]string{
			"--source", "filesystem",
			"--snapshot-dir", root,
			"--limit", "1",
			"--write-manifest", manifestPath,
		})
	})

	for _, want := range []string{
		"Snapshot/current UTxO import",
		"utxos: 1",
		"ma_tx_outs: 1",
		"Dry run only. Filesystem source found UTxO-HD tables/tvar and is ready for direct apply.",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("command output missing %q:\n%s", want, output)
		}
	}

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest snapshot.ImportManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if manifest.Source != "tvar" {
		t.Fatalf("manifest source = %q, want tvar", manifest.Source)
	}
	if !manifest.DirectFilesystemDecodeSupported {
		t.Fatalf("manifest direct filesystem decode support = false, want true")
	}
	if manifest.Filesystem == nil {
		t.Fatalf("manifest filesystem section missing")
	}
	if manifest.Filesystem.UTxOTablePath != tvarPath {
		t.Fatalf("manifest tvar path = %q, want %q", manifest.Filesystem.UTxOTablePath, tvarPath)
	}
	if manifest.Filesystem.UTxOTableFormat != snapshot.UTxOTableFormatTVar {
		t.Fatalf("manifest tvar format = %q, want %q", manifest.Filesystem.UTxOTableFormat, snapshot.UTxOTableFormatTVar)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}
	os.Stdout = w
	defer func() {
		os.Stdout = oldStdout
	}()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close stdout pipe: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stdout pipe: %v", err)
	}
	return string(out)
}

func buildCommandTestTVar(txHash []byte, outputIndex uint32, txOut []byte) []byte {
	key := append([]byte{}, txHash...)
	key = append(key, byte(outputIndex), byte(outputIndex>>8))
	data := []byte{0x81, 0xa1}
	data = append(data, encodeCommandTestCBORBytes(key)...)
	data = append(data, encodeCommandTestCBORBytes(txOut)...)
	return data
}

func buildCommandTestMempackTxOut(address []byte, lovelace uint64, policy []byte, assetName []byte, amount uint64) []byte {
	flat := buildCommandTestFlatMultiAsset(policy, assetName, amount)
	data := []byte{0}
	data = append(data, encodeCommandTestMempackVarLen(uint64(len(address)))...)
	data = append(data, address...)
	data = append(data, 1)
	data = append(data, encodeCommandTestMempackVarLen(lovelace)...)
	data = append(data, encodeCommandTestMempackVarLen(1)...)
	data = append(data, encodeCommandTestMempackVarLen(uint64(len(flat)))...)
	data = append(data, flat...)
	return data
}

func buildCommandTestFlatMultiAsset(policy []byte, assetName []byte, amount uint64) []byte {
	flat := make([]byte, 8+2+2)
	binary.LittleEndian.PutUint64(flat[0:8], amount)
	binary.LittleEndian.PutUint16(flat[8:10], uint16(len(flat)))
	binary.LittleEndian.PutUint16(flat[10:12], uint16(len(flat)+28))
	flat = append(flat, policy...)
	flat = append(flat, assetName...)
	return flat
}

func encodeCommandTestCBORBytes(value []byte) []byte {
	switch {
	case len(value) < 24:
		return append([]byte{byte(0x40 | len(value))}, value...)
	case len(value) <= 255:
		return append([]byte{0x58, byte(len(value))}, value...)
	default:
		panic("test CBOR helper only supports short byte strings")
	}
}

func encodeCommandTestMempackVarLen(value uint64) []byte {
	if value == 0 {
		return []byte{0}
	}
	var groups []byte
	for value > 0 {
		groups = append([]byte{byte(value & 0x7f)}, groups...)
		value >>= 7
	}
	for i := 0; i < len(groups)-1; i++ {
		groups[i] |= 0x80
	}
	return groups
}
