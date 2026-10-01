package snapshot

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"

	"nectar/models"
)

type NDJSONOptions struct {
	Path        string
	BatchSize   int
	Limit       int64
	Apply       bool
	ReplaceUTxO bool
	TipSlot     uint64
}

type NDJSONImporter struct {
	db      *gorm.DB
	options NDJSONOptions
}

type NDJSONUTxO struct {
	TxHash              string        `json:"tx_hash"`
	TxID                string        `json:"tx_id"`
	OutputIndex         uint32        `json:"output_index"`
	Index               uint32        `json:"index"`
	Address             string        `json:"address"`
	AddressRaw          string        `json:"address_raw"`
	AddressRawHex       string        `json:"address_raw_hex"`
	Amount              uint64        `json:"amount"`
	Value               uint64        `json:"value"`
	Assets              []NDJSONAsset `json:"assets"`
	DatumHash           string        `json:"datum_hash"`
	InlineDatumHash     string        `json:"inline_datum_hash"`
	ReferenceScriptHash string        `json:"reference_script_hash"`
}

type NDJSONAsset struct {
	PolicyID  string `json:"policy_id"`
	PolicyId  string `json:"policyId"`
	Policy    string `json:"policy"`
	AssetName string `json:"asset_name"`
	AssetHex  string `json:"asset_name_hex"`
	Name      string `json:"name"`
	NameHex   string `json:"name_hex"`
	Amount    uint64 `json:"amount"`
	Quantity  uint64 `json:"quantity"`
}

func NewNDJSONImporter(db *gorm.DB, options NDJSONOptions) *NDJSONImporter {
	if options.BatchSize <= 0 {
		options.BatchSize = 1000
	}
	return &NDJSONImporter{db: db, options: options}
}

func (i *NDJSONImporter) Import(ctx context.Context) (UTxORPCStats, error) {
	if strings.TrimSpace(i.options.Path) == "" {
		return UTxORPCStats{}, fmt.Errorf("ndjson path is required")
	}
	if i.options.Apply && i.db == nil {
		return UTxORPCStats{}, fmt.Errorf("database handle is required when apply is enabled")
	}

	file, err := os.Open(i.options.Path)
	if err != nil {
		return UTxORPCStats{}, fmt.Errorf("open ndjson: %w", err)
	}
	defer file.Close()

	stats := UTxORPCStats{
		LastTipSlot: i.options.TipSlot,
		Applied:     i.options.Apply,
	}
	if i.options.Apply && i.options.ReplaceUTxO {
		if err := clearCurrentUTxOTables(i.db); err != nil {
			return stats, err
		}
	}

	decoder := json.NewDecoder(file)
	batch := currentUTxOBatch{
		TxOuts:      make([]models.TxOut, 0, i.options.BatchSize),
		MaTxOuts:    make([]models.MaTxOut, 0),
		MultiAssets: make(map[string]models.MultiAsset),
	}
	flush := func() error {
		if len(batch.TxOuts) == 0 && len(batch.MaTxOuts) == 0 && len(batch.MultiAssets) == 0 {
			return nil
		}
		stats.Pages++
		stats.TxOuts += int64(len(batch.TxOuts))
		stats.MaTxOuts += int64(len(batch.MaTxOuts))
		stats.MultiAssets += int64(len(batch.MultiAssets))
		if i.options.Apply {
			if err := writeCurrentUTxOBatch(i.db, batch); err != nil {
				return err
			}
		}
		batch = currentUTxOBatch{
			TxOuts:      make([]models.TxOut, 0, i.options.BatchSize),
			MaTxOuts:    make([]models.MaTxOut, 0),
			MultiAssets: make(map[string]models.MultiAsset),
		}
		return nil
	}

	for {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		if i.options.Limit > 0 && stats.UTxOs >= i.options.Limit {
			break
		}

		var record NDJSONUTxO
		if err := decoder.Decode(&record); err != nil {
			if err == io.EOF {
				break
			}
			return stats, fmt.Errorf("decode ndjson record %d: %w", stats.UTxOs+1, err)
		}

		stats.UTxOs++
		txOut, assets, err := convertNDJSONUTxO(record)
		if err != nil {
			return stats, fmt.Errorf("convert ndjson record %d: %w", stats.UTxOs, err)
		}
		batch.TxOuts = append(batch.TxOuts, txOut)
		for _, asset := range assets {
			key := hex.EncodeToString(asset.Policy) + "." + hex.EncodeToString(asset.Name)
			if _, ok := batch.MultiAssets[key]; !ok {
				batch.MultiAssets[key] = models.MultiAsset{
					Policy:      append([]byte{}, asset.Policy...),
					Name:        append([]byte{}, asset.Name...),
					Fingerprint: common.NewAssetFingerprint(asset.Policy, asset.Name).String(),
				}
			}
			batch.MaTxOuts = append(batch.MaTxOuts, models.MaTxOut{
				TxHash:   append([]byte{}, txOut.TxHash...),
				TxIndex:  txOut.Index,
				Policy:   append([]byte{}, asset.Policy...),
				Name:     append([]byte{}, asset.Name...),
				Quantity: asset.Quantity,
			})
		}

		if len(batch.TxOuts) >= i.options.BatchSize {
			if err := flush(); err != nil {
				return stats, err
			}
		}
	}
	if err := flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

type ndjsonAssetValue struct {
	Policy   []byte
	Name     []byte
	Quantity uint64
}

func convertNDJSONUTxO(record NDJSONUTxO) (models.TxOut, []ndjsonAssetValue, error) {
	txHash, err := decodeRequiredHex(firstNonEmptyString(record.TxHash, record.TxID), "tx_hash")
	if err != nil {
		return models.TxOut{}, nil, err
	}
	if len(txHash) != 32 {
		return models.TxOut{}, nil, fmt.Errorf("tx_hash must be 32 bytes, got %d", len(txHash))
	}

	addressString, addressBytes, err := decodeNDJSONAddress(record)
	if err != nil {
		return models.TxOut{}, nil, err
	}
	if addressString == "" {
		return models.TxOut{}, nil, fmt.Errorf("address is required")
	}
	index := record.OutputIndex
	if index == 0 && record.Index > 0 {
		index = record.Index
	}
	value := record.Amount
	if value == 0 {
		value = record.Value
	}

	dataHash, err := decodeOptionalHex(record.DatumHash, "datum_hash")
	if err != nil {
		return models.TxOut{}, nil, err
	}
	inlineDatumHash, err := decodeOptionalHex(firstNonEmptyString(record.InlineDatumHash, record.DatumHash), "inline_datum_hash")
	if err != nil {
		return models.TxOut{}, nil, err
	}
	referenceScriptHash, err := decodeOptionalHex(record.ReferenceScriptHash, "reference_script_hash")
	if err != nil {
		return models.TxOut{}, nil, err
	}

	txOut := models.TxOut{
		TxHash:              txHash,
		Index:               index,
		Address:             addressString,
		AddressRaw:          addressBytes,
		AddressHasScript:    addressHasScript(addressBytes),
		PaymentCred:         paymentCredential(addressBytes),
		StakeAddressHash:    stakeCredential(addressBytes),
		Value:               value,
		DataHash:            dataHash,
		InlineDatumHash:     inlineDatumHash,
		ReferenceScriptHash: referenceScriptHash,
	}

	assets := make([]ndjsonAssetValue, 0, len(record.Assets))
	for _, asset := range record.Assets {
		policy, err := decodeRequiredHex(firstNonEmptyString(asset.PolicyID, asset.PolicyId, asset.Policy), "asset.policy_id")
		if err != nil {
			return models.TxOut{}, nil, err
		}
		if len(policy) != 28 {
			return models.TxOut{}, nil, fmt.Errorf("asset policy must be 28 bytes, got %d", len(policy))
		}
		name, err := decodeHexAllowEmpty(firstNonEmptyString(asset.AssetName, asset.AssetHex, asset.NameHex, asset.Name), "asset.name")
		if err != nil {
			return models.TxOut{}, nil, err
		}
		quantity := asset.Quantity
		if quantity == 0 {
			quantity = asset.Amount
		}
		if quantity == 0 {
			continue
		}
		assets = append(assets, ndjsonAssetValue{
			Policy:   policy,
			Name:     name,
			Quantity: quantity,
		})
	}
	return txOut, assets, nil
}

func decodeNDJSONAddress(record NDJSONUTxO) (string, []byte, error) {
	rawHex := firstNonEmptyString(record.AddressRaw, record.AddressRawHex)
	if rawHex != "" {
		raw, err := decodeRequiredHex(rawHex, "address_raw")
		if err != nil {
			return "", nil, err
		}
		addr, err := common.NewAddressFromBytes(raw)
		if err != nil {
			return "", nil, fmt.Errorf("decode raw address: %w", err)
		}
		address := record.Address
		if address == "" {
			address = addr.String()
		}
		return address, raw, nil
	}
	if record.Address == "" {
		return "", nil, fmt.Errorf("address or address_raw is required")
	}
	addr, err := common.NewAddress(record.Address)
	if err != nil {
		return "", nil, fmt.Errorf("decode address: %w", err)
	}
	raw, err := addr.Bytes()
	if err != nil {
		return "", nil, fmt.Errorf("encode address bytes: %w", err)
	}
	return record.Address, raw, nil
}

func decodeRequiredHex(value string, field string) ([]byte, error) {
	decoded, err := decodeHexAllowEmpty(value, field)
	if err != nil {
		return nil, err
	}
	if len(decoded) == 0 {
		return nil, fmt.Errorf("%s is required", field)
	}
	return decoded, nil
}

func decodeOptionalHex(value string, field string) ([]byte, error) {
	return decodeHexAllowEmpty(value, field)
}

func decodeHexAllowEmpty(value string, field string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	value = strings.TrimPrefix(value, "0x")
	decoded, err := hex.DecodeString(value)
	if err != nil {
		if field == "" {
			return nil, err
		}
		return nil, fmt.Errorf("%s must be hex: %w", field, err)
	}
	return decoded, nil
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
