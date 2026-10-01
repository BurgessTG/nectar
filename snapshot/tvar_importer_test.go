package snapshot

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseTVarUTxOsDecodesMempackMultiAssetOutput(t *testing.T) {
	txHash := bytes.Repeat([]byte{0xaa}, 32)
	address := append([]byte{0x61}, bytes.Repeat([]byte{0x11}, 28)...)
	policy := bytes.Repeat([]byte{0xbb}, 28)
	assetName := []byte("TEST")

	data := buildTestTVar(txHash, 7, buildTestMempackTxOut(address, 1_500_000, policy, assetName, 42))

	var parsed []tvarUTxO
	if err := parseTVarUTxOs(data, func(utxo tvarUTxO) error {
		parsed = append(parsed, utxo)
		return nil
	}); err != nil {
		t.Fatalf("parseTVarUTxOs returned error: %v", err)
	}

	if len(parsed) != 1 {
		t.Fatalf("parsed %d UTxOs, want 1", len(parsed))
	}
	got := parsed[0]
	if !bytes.Equal(got.TxHash, txHash) {
		t.Fatalf("tx hash = %x, want %x", got.TxHash, txHash)
	}
	if got.OutputIndex != 7 {
		t.Fatalf("output index = %d, want 7", got.OutputIndex)
	}
	if !bytes.Equal(got.Address, address) {
		t.Fatalf("address = %x, want %x", got.Address, address)
	}
	if got.Amount != 1_500_000 {
		t.Fatalf("amount = %d, want 1500000", got.Amount)
	}
	if len(got.Assets) != 1 {
		t.Fatalf("asset count = %d, want 1", len(got.Assets))
	}
	if !bytes.Equal(got.Assets[0].PolicyID, policy) || !bytes.Equal(got.Assets[0].Name, assetName) || got.Assets[0].Amount != 42 {
		t.Fatalf("asset = policy:%x name:%x amount:%d", got.Assets[0].PolicyID, got.Assets[0].Name, got.Assets[0].Amount)
	}
}

func TestConvertTVarUTxOProducesNectarRows(t *testing.T) {
	utxo := tvarUTxO{
		TxHash:      bytes.Repeat([]byte{0xaa}, 32),
		OutputIndex: 2,
		Address:     append([]byte{0x61}, bytes.Repeat([]byte{0x22}, 28)...),
		Amount:      2_000_000,
		Assets: []tvarAsset{{
			PolicyID: bytes.Repeat([]byte{0xbb}, 28),
			Name:     []byte("HNY"),
			Amount:   9,
		}},
	}

	txOut, assets, err := convertTVarUTxO(utxo)
	if err != nil {
		t.Fatalf("convertTVarUTxO returned error: %v", err)
	}
	if txOut.Index != 2 || txOut.Value != 2_000_000 {
		t.Fatalf("unexpected tx out: index=%d value=%d", txOut.Index, txOut.Value)
	}
	if !strings.HasPrefix(txOut.Address, "addr1") {
		t.Fatalf("address = %q, want mainnet addr", txOut.Address)
	}
	if len(txOut.PaymentCred) != 28 {
		t.Fatalf("payment credential length = %d, want 28", len(txOut.PaymentCred))
	}
	if len(assets) != 1 || assets[0].Amount != 9 {
		t.Fatalf("assets = %#v", assets)
	}
}

func TestTVarImporterDryRunReadsMappedFile(t *testing.T) {
	txHash := bytes.Repeat([]byte{0xaa}, 32)
	address := append([]byte{0x61}, bytes.Repeat([]byte{0x11}, 28)...)
	policy := bytes.Repeat([]byte{0xbb}, 28)
	data := buildTestTVar(txHash, 1, buildTestMempackTxOut(address, 1_000_000, policy, []byte("A"), 5))

	path := filepath.Join(t.TempDir(), "tvar")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write tvar fixture: %v", err)
	}

	stats, err := NewTVarImporter(nil, TVarOptions{Path: path, BatchSize: 1}).Import(context.Background())
	if err != nil {
		t.Fatalf("Import returned error: %v", err)
	}
	if stats.Applied {
		t.Fatalf("dry run should not be marked applied")
	}
	if stats.UTxOs != 1 || stats.TxOuts != 1 || stats.MaTxOuts != 1 || stats.MultiAssets != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
}

func buildTestTVar(txHash []byte, outputIndex uint32, txOut []byte) []byte {
	key := append([]byte{}, txHash...)
	key = append(key, byte(outputIndex), byte(outputIndex>>8))
	data := []byte{0x81, 0xa1}
	data = append(data, encodeTestCBORBytes(key)...)
	data = append(data, encodeTestCBORBytes(txOut)...)
	return data
}

func buildTestMempackTxOut(address []byte, lovelace uint64, policy []byte, assetName []byte, amount uint64) []byte {
	flat := buildTestFlatMultiAsset(policy, assetName, amount)
	data := []byte{0}
	data = append(data, encodeTestMempackVarLen(uint64(len(address)))...)
	data = append(data, address...)
	data = append(data, 1)
	data = append(data, encodeTestMempackVarLen(lovelace)...)
	data = append(data, encodeTestMempackVarLen(1)...)
	data = append(data, encodeTestMempackVarLen(uint64(len(flat)))...)
	data = append(data, flat...)
	return data
}

func buildTestFlatMultiAsset(policy []byte, assetName []byte, amount uint64) []byte {
	flat := make([]byte, 8+2+2)
	binary.LittleEndian.PutUint64(flat[0:8], amount)
	binary.LittleEndian.PutUint16(flat[8:10], uint16(len(flat)))
	binary.LittleEndian.PutUint16(flat[10:12], uint16(len(flat)+28))
	flat = append(flat, policy...)
	flat = append(flat, assetName...)
	return flat
}

func encodeTestCBORBytes(value []byte) []byte {
	switch {
	case len(value) < 24:
		return append([]byte{byte(0x40 | len(value))}, value...)
	case len(value) <= 255:
		return append([]byte{0x58, byte(len(value))}, value...)
	default:
		panic("test CBOR helper only supports short byte strings")
	}
}

func encodeTestMempackVarLen(value uint64) []byte {
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
