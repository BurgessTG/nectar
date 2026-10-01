package snapshot

import (
	"bytes"
	"strings"
	"testing"
)

func TestConvertNDJSONUTxOMapsDingoStyleFields(t *testing.T) {
	rawAddressHex := "61" + strings.Repeat("11", 28)
	record := NDJSONUTxO{
		TxID:          strings.Repeat("aa", 32),
		Index:         7,
		AddressRawHex: rawAddressHex,
		Value:         1_500_000,
		Assets: []NDJSONAsset{
			{
				PolicyId: strings.Repeat("bb", 28),
				AssetHex: "54455354",
				Quantity: 42,
			},
		},
		DatumHash: strings.Repeat("cc", 32),
	}

	txOut, assets, err := convertNDJSONUTxO(record)
	if err != nil {
		t.Fatalf("convertNDJSONUTxO returned error: %v", err)
	}
	if !bytes.Equal(txOut.TxHash, bytes.Repeat([]byte{0xaa}, 32)) {
		t.Fatalf("unexpected tx hash: %x", txOut.TxHash)
	}
	if txOut.Index != 7 {
		t.Fatalf("index = %d, want 7", txOut.Index)
	}
	if txOut.Value != 1_500_000 {
		t.Fatalf("value = %d, want 1500000", txOut.Value)
	}
	if len(txOut.AddressRaw) != 29 || !strings.HasPrefix(txOut.Address, "addr1") {
		t.Fatalf("address not decoded into Cardano address: raw=%x string=%q", txOut.AddressRaw, txOut.Address)
	}
	if len(txOut.PaymentCred) != 28 {
		t.Fatalf("payment credential length = %d, want 28", len(txOut.PaymentCred))
	}
	if len(assets) != 1 {
		t.Fatalf("asset count = %d, want 1", len(assets))
	}
	if !bytes.Equal(assets[0].Policy, bytes.Repeat([]byte{0xbb}, 28)) {
		t.Fatalf("unexpected asset policy: %x", assets[0].Policy)
	}
	if string(assets[0].Name) != "TEST" {
		t.Fatalf("asset name = %q, want TEST", string(assets[0].Name))
	}
	if assets[0].Quantity != 42 {
		t.Fatalf("asset quantity = %d, want 42", assets[0].Quantity)
	}
}

func TestConvertNDJSONUTxORejectsMalformedOptionalHashes(t *testing.T) {
	record := NDJSONUTxO{
		TxHash:        strings.Repeat("aa", 32),
		AddressRawHex: "61" + strings.Repeat("11", 28),
		Amount:        1,
		DatumHash:     "not-hex",
	}

	if _, _, err := convertNDJSONUTxO(record); err == nil {
		t.Fatalf("expected malformed datum hash to fail")
	}
}
