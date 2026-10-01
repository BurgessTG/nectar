// Portions of the UTxO-HD tvar and MemPack decoding logic are adapted
// from github.com/blinklabs-io/dingo/ledgerstate.
//
// Copyright 2026 Blink Labs Software.
// Licensed under the Apache License, Version 2.0.

package snapshot

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"syscall"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"

	"nectar/models"
)

type TVarOptions struct {
	Path        string
	BatchSize   int
	Limit       int64
	Apply       bool
	ReplaceUTxO bool
	TipSlot     uint64
}

type TVarImporter struct {
	db      *gorm.DB
	options TVarOptions
}

type tvarUTxO struct {
	TxHash      []byte
	OutputIndex uint32
	Address     []byte
	Amount      uint64
	Assets      []tvarAsset
	DatumHash   []byte
	Datum       []byte
	ScriptRef   []byte
}

type tvarAsset struct {
	PolicyID []byte
	Name     []byte
	Amount   uint64
}

type tvarIterFunc func(keyRaw cbor.RawMessage, valRaw cbor.RawMessage) error

func NewTVarImporter(db *gorm.DB, options TVarOptions) *TVarImporter {
	if options.BatchSize <= 0 {
		options.BatchSize = 1000
	}
	return &TVarImporter{db: db, options: options}
}

func (i *TVarImporter) Import(ctx context.Context) (UTxORPCStats, error) {
	if i.options.Path == "" {
		return UTxORPCStats{}, fmt.Errorf("tvar path is required")
	}
	if i.options.Apply && i.db == nil {
		return UTxORPCStats{}, fmt.Errorf("database handle is required when apply is enabled")
	}

	data, cleanup, err := mapTVarFile(i.options.Path)
	if err != nil {
		return UTxORPCStats{}, err
	}
	defer cleanup()
	if len(data) == 0 {
		return UTxORPCStats{}, fmt.Errorf("empty tvar file")
	}

	stats := UTxORPCStats{
		LastTipSlot: i.options.TipSlot,
		Applied:     i.options.Apply,
	}
	if i.options.Apply && i.options.ReplaceUTxO {
		if err := clearCurrentUTxOTables(i.db); err != nil {
			return stats, err
		}
	}

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

	err = parseTVarUTxOs(data, func(parsed tvarUTxO) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if i.options.Limit > 0 && stats.UTxOs >= i.options.Limit {
			return errTVarLimitReached
		}
		stats.UTxOs++

		txOut, assets, err := convertTVarUTxO(parsed)
		if err != nil {
			return fmt.Errorf("convert tvar utxo %d: %w", stats.UTxOs, err)
		}
		batch.TxOuts = append(batch.TxOuts, txOut)
		for _, asset := range assets {
			if asset.Amount == 0 {
				continue
			}
			key := hex.EncodeToString(asset.PolicyID) + "." + hex.EncodeToString(asset.Name)
			if _, ok := batch.MultiAssets[key]; !ok {
				batch.MultiAssets[key] = models.MultiAsset{
					Policy:      append([]byte{}, asset.PolicyID...),
					Name:        append([]byte{}, asset.Name...),
					Fingerprint: common.NewAssetFingerprint(asset.PolicyID, asset.Name).String(),
				}
			}
			batch.MaTxOuts = append(batch.MaTxOuts, models.MaTxOut{
				TxHash:   append([]byte{}, txOut.TxHash...),
				TxIndex:  txOut.Index,
				Policy:   append([]byte{}, asset.PolicyID...),
				Name:     append([]byte{}, asset.Name...),
				Quantity: asset.Amount,
			})
		}

		if len(batch.TxOuts) >= i.options.BatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errTVarLimitReached) {
		return stats, err
	}
	if err := flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

var errTVarLimitReached = errors.New("tvar import limit reached")

func mapTVarFile(path string) ([]byte, func() error, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, noopCleanup, fmt.Errorf("open tvar: %w", err)
	}
	info, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return nil, noopCleanup, fmt.Errorf("stat tvar: %w", statErr)
	}
	size := info.Size()
	if size == 0 {
		_ = file.Close()
		return nil, noopCleanup, nil
	}
	if size < 0 || size > int64(math.MaxInt) {
		_ = file.Close()
		return nil, noopCleanup, fmt.Errorf("tvar file size %d cannot be mapped on this platform", size)
	}

	data, mmapErr := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_PRIVATE)
	closeErr := file.Close()
	if mmapErr != nil {
		if closeErr != nil {
			return nil, noopCleanup, fmt.Errorf("mmap tvar: %w; close tvar: %v", mmapErr, closeErr)
		}
		return nil, noopCleanup, fmt.Errorf("mmap tvar: %w", mmapErr)
	}
	if closeErr != nil {
		if err := syscall.Munmap(data); err != nil {
			return nil, noopCleanup, fmt.Errorf("close tvar: %w; munmap tvar: %v", closeErr, err)
		}
		return nil, noopCleanup, fmt.Errorf("close tvar: %w", closeErr)
	}

	return data, func() error {
		return syscall.Munmap(data)
	}, nil
}

func noopCleanup() error {
	return nil
}

func parseTVarUTxOs(data []byte, callback func(tvarUTxO) error) error {
	if callback == nil {
		return fmt.Errorf("tvar callback is required")
	}
	if len(data) == 0 {
		return fmt.Errorf("empty tvar data")
	}
	arrayLen, headerLen, err := cborContainerHeader(data, 4)
	if err != nil {
		return fmt.Errorf("decode tvar array header: %w", err)
	}
	if arrayLen != 1 {
		return fmt.Errorf("tvar has %d elements, expected 1", arrayLen)
	}
	mapData := data[headerLen:]
	return iterateTVarMap(mapData, func(keyRaw cbor.RawMessage, valRaw cbor.RawMessage) error {
		utxo, err := parseTVarEntry(keyRaw, valRaw)
		if err != nil {
			return err
		}
		return callback(*utxo)
	})
}

func iterateTVarMap(data []byte, each tvarIterFunc) error {
	if len(data) == 0 {
		return fmt.Errorf("empty UTxO map")
	}

	if data[0] == 0xbf {
		pos := 1
		for entry := 0; pos < len(data); entry++ {
			if data[pos] == 0xff {
				if pos+1 != len(data) {
					return fmt.Errorf("trailing data after indefinite UTxO map break")
				}
				return nil
			}
			keySize, err := tvarCBORItemSize(data[pos:])
			if err != nil {
				return fmt.Errorf("size indefinite UTxO key %d: %w", entry, err)
			}
			keyRaw := cbor.RawMessage(data[pos : pos+keySize])
			pos += keySize

			valSize, err := tvarCBORItemSize(data[pos:])
			if err != nil {
				return fmt.Errorf("size indefinite UTxO value %d: %w", entry, err)
			}
			valRaw := cbor.RawMessage(data[pos : pos+valSize])
			pos += valSize

			if err := each(keyRaw, valRaw); err != nil {
				return err
			}
		}
		return fmt.Errorf("indefinite UTxO map missing break code")
	}

	count, headerLen, err := cborContainerHeader(data, 5)
	if err != nil {
		return fmt.Errorf("decode UTxO map header: %w", err)
	}
	pos := headerLen
	for entry := uint64(0); entry < count; entry++ {
		keySize, err := tvarCBORItemSize(data[pos:])
		if err != nil {
			return fmt.Errorf("size UTxO key %d: %w", entry, err)
		}
		keyRaw := cbor.RawMessage(data[pos : pos+keySize])
		pos += keySize

		valSize, err := tvarCBORItemSize(data[pos:])
		if err != nil {
			return fmt.Errorf("size UTxO value %d: %w", entry, err)
		}
		valRaw := cbor.RawMessage(data[pos : pos+valSize])
		pos += valSize

		if err := each(keyRaw, valRaw); err != nil {
			return err
		}
	}
	if pos != len(data) {
		return fmt.Errorf("trailing data after UTxO map: %d bytes", len(data)-pos)
	}
	return nil
}

func parseTVarEntry(keyRaw cbor.RawMessage, valRaw cbor.RawMessage) (*tvarUTxO, error) {
	txHash, outputIndex, err := decodeTVarTxIn(keyRaw)
	if err != nil {
		return nil, fmt.Errorf("decode TxIn: %w", err)
	}

	txOutData, err := unwrapTVarCborBytes(valRaw)
	if err != nil {
		txOutData = valRaw
	}
	if isTVarMempackFormat(txOutData) {
		return parseTVarMempackTxOut(txHash, outputIndex, txOutData)
	}
	return parseTVarCBORTxOut(txHash, outputIndex, valRaw, txOutData)
}

func convertTVarUTxO(parsed tvarUTxO) (models.TxOut, []tvarAsset, error) {
	if len(parsed.TxHash) != 32 {
		return models.TxOut{}, nil, fmt.Errorf("tx hash must be 32 bytes, got %d", len(parsed.TxHash))
	}
	addr, err := common.NewAddressFromBytes(parsed.Address)
	if err != nil {
		return models.TxOut{}, nil, fmt.Errorf("decode address: %w", err)
	}
	addressBytes, err := addr.Bytes()
	if err != nil {
		addressBytes = append([]byte{}, parsed.Address...)
	}

	txOut := models.TxOut{
		TxHash:           append([]byte{}, parsed.TxHash...),
		Index:            parsed.OutputIndex,
		Address:          addr.String(),
		AddressRaw:       addressBytes,
		AddressHasScript: addressHasScript(addressBytes),
		PaymentCred:      paymentCredential(addressBytes),
		StakeAddressHash: stakeCredential(addressBytes),
		Value:            parsed.Amount,
		DataHash:         append([]byte{}, parsed.DatumHash...),
	}
	if len(parsed.Datum) > 0 {
		datumHash := ledger.NewBlake2b256(parsed.Datum)
		txOut.InlineDatumHash = datumHash[:]
	}
	if len(parsed.ScriptRef) > 0 {
		scriptHash := ledger.NewBlake2b224(parsed.ScriptRef)
		txOut.ReferenceScriptHash = scriptHash[:]
	}
	return txOut, parsed.Assets, nil
}

func decodeTVarTxIn(raw cbor.RawMessage) ([]byte, uint32, error) {
	var keyBytes []byte
	if _, err := cbor.Decode(raw, &keyBytes); err == nil {
		return decodeTVarTxInBytes(keyBytes)
	}

	var txIn []cbor.RawMessage
	if _, err := cbor.Decode(raw, &txIn); err == nil && len(txIn) >= 2 {
		return decodeTVarTxInArray(txIn)
	}

	var wrapped cbor.WrappedCbor
	if _, err := cbor.Decode(raw, &wrapped); err == nil {
		return decodeTVarTxInBytes(wrapped.Bytes())
	}

	return nil, 0, fmt.Errorf("unrecognized TxIn encoding len=%d", len(raw))
}

func decodeTVarTxInArray(txIn []cbor.RawMessage) ([]byte, uint32, error) {
	var txHash []byte
	if _, err := cbor.Decode(txIn[0], &txHash); err != nil {
		return nil, 0, fmt.Errorf("decode TxIn hash: %w", err)
	}
	var outputIndex uint32
	if _, err := cbor.Decode(txIn[1], &outputIndex); err != nil {
		return nil, 0, fmt.Errorf("decode TxIn index: %w", err)
	}
	if len(txHash) != 32 {
		return nil, 0, fmt.Errorf("TxIn hash must be 32 bytes, got %d", len(txHash))
	}
	return txHash, outputIndex, nil
}

func decodeTVarTxInBytes(data []byte) ([]byte, uint32, error) {
	if len(data) == 34 {
		txHash := bytes.Clone(data[:32])
		idx := uint32(data[32]) | uint32(data[33])<<8
		return txHash, idx, nil
	}
	var txIn []cbor.RawMessage
	if _, err := cbor.Decode(data, &txIn); err == nil && len(txIn) >= 2 {
		return decodeTVarTxInArray(txIn)
	}
	return nil, 0, fmt.Errorf("unrecognized TxIn bytes len=%d", len(data))
}

func unwrapTVarCborBytes(raw cbor.RawMessage) (cbor.RawMessage, error) {
	var wrapped cbor.WrappedCbor
	if _, err := cbor.Decode(raw, &wrapped); err == nil {
		return cbor.RawMessage(wrapped.Bytes()), nil
	}
	var bstr []byte
	if _, err := cbor.Decode(raw, &bstr); err == nil {
		return cbor.RawMessage(bstr), nil
	}
	return raw, fmt.Errorf("not a wrapped byte string")
}

func parseTVarCBORTxOut(txHash []byte, outputIndex uint32, valRaw cbor.RawMessage, txOutData cbor.RawMessage) (*tvarUTxO, error) {
	txOut, err := ledger.NewTransactionOutputFromCbor(txOutData)
	if err != nil {
		return nil, fmt.Errorf("decode CBOR TxOut from raw len=%d unwrapped len=%d: %w", len(valRaw), len(txOutData), err)
	}
	addrBytes, err := txOut.Address().Bytes()
	if err != nil {
		return nil, fmt.Errorf("encode CBOR TxOut address: %w", err)
	}

	result := &tvarUTxO{
		TxHash:      append([]byte{}, txHash...),
		OutputIndex: outputIndex,
		Address:     addrBytes,
		Amount:      txOut.Amount(),
	}
	if datumHash := txOut.DatumHash(); datumHash != nil {
		result.DatumHash = datumHash.Bytes()
	}
	if datum := txOut.Datum(); datum != nil {
		result.Datum = datum.Cbor()
	}
	if multiAsset := txOut.Assets(); multiAsset != nil {
		result.Assets = convertTVarMultiAsset(multiAsset)
	}
	return result, nil
}

func convertTVarMultiAsset(multiAsset *common.MultiAsset[common.MultiAssetTypeOutput]) []tvarAsset {
	var assets []tvarAsset
	for _, policyID := range multiAsset.Policies() {
		policyBytes := policyID.Bytes()
		for _, assetName := range multiAsset.Assets(policyID) {
			amount := multiAsset.Asset(policyID, assetName)
			assets = append(assets, tvarAsset{
				PolicyID: append([]byte{}, policyBytes...),
				Name:     append([]byte{}, assetName...),
				Amount:   amount,
			})
		}
	}
	return assets
}

func parseTVarMempackTxOut(txHash []byte, outputIndex uint32, data []byte) (*tvarUTxO, error) {
	decoded, err := decodeTVarMempackTxOut(data)
	if err != nil {
		return nil, err
	}
	return &tvarUTxO{
		TxHash:      append([]byte{}, txHash...),
		OutputIndex: outputIndex,
		Address:     decoded.Address,
		Amount:      decoded.Lovelace,
		Assets:      decoded.Assets,
		DatumHash:   decoded.DatumHash,
		Datum:       decoded.Datum,
		ScriptRef:   decoded.ScriptRef,
	}, nil
}

type tvarMempackReader struct {
	data []byte
	pos  int
}

type decodedTVarMempackTxOut struct {
	Address   []byte
	Lovelace  uint64
	Assets    []tvarAsset
	DatumHash []byte
	Datum     []byte
	ScriptRef []byte
}

func newTVarMempackReader(data []byte) *tvarMempackReader {
	return &tvarMempackReader{data: data}
}

func (r *tvarMempackReader) readByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("mempack: unexpected EOF at pos %d", r.pos)
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *tvarMempackReader) readBytes(n int) ([]byte, error) {
	if n < 0 || n > len(r.data)-r.pos {
		return nil, fmt.Errorf("mempack: need %d bytes at pos %d, have %d", n, r.pos, len(r.data)-r.pos)
	}
	b := bytes.Clone(r.data[r.pos : r.pos+n])
	r.pos += n
	return b, nil
}

func (r *tvarMempackReader) readWord64LE() (uint64, error) {
	b, err := r.readBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *tvarMempackReader) readVarLen() (int, error) {
	value, err := r.readVarLenUint64()
	if err != nil {
		return 0, err
	}
	if value > uint64(math.MaxInt) {
		return 0, fmt.Errorf("mempack: VarLen value %d overflows int", value)
	}
	return int(value), nil
}

func (r *tvarMempackReader) readVarLenUint64() (uint64, error) {
	var acc uint64
	for range 10 {
		b, err := r.readByte()
		if err != nil {
			return 0, fmt.Errorf("read VarLen: %w", err)
		}
		if acc > math.MaxUint64>>7 {
			return 0, fmt.Errorf("mempack: VarLen overflow at pos %d", r.pos-1)
		}
		acc = (acc << 7) | uint64(b&0x7f)
		if b&0x80 == 0 {
			return acc, nil
		}
	}
	return 0, fmt.Errorf("mempack: VarLen overflow at pos %d", r.pos)
}

func (r *tvarMempackReader) readLengthPrefixedBytes() ([]byte, error) {
	length, err := r.readVarLen()
	if err != nil {
		return nil, fmt.Errorf("read byte string length: %w", err)
	}
	return r.readBytes(length)
}

func decodeTVarMempackTxOut(data []byte) (*decodedTVarMempackTxOut, error) {
	r := newTVarMempackReader(data)
	tag, err := r.readByte()
	if err != nil {
		return nil, fmt.Errorf("read TxOut tag: %w", err)
	}

	var result *decodedTVarMempackTxOut
	switch tag {
	case 0:
		result, err = decodeTVarTxOutCompact(r)
	case 1:
		result, err = decodeTVarTxOutCompactDH(r)
	case 2:
		result, err = decodeTVarTxOutAddrHash28(r, false)
	case 3:
		result, err = decodeTVarTxOutAddrHash28(r, true)
	case 4:
		result, err = decodeTVarTxOutCompactDatum(r)
	case 5:
		result, err = decodeTVarTxOutCompactRefScript(r)
	default:
		return nil, fmt.Errorf("unknown MemPack TxOut tag: %d", tag)
	}
	if err != nil {
		return nil, err
	}
	if r.pos != len(r.data) {
		return nil, fmt.Errorf("mempack: %d trailing bytes after TxOut tag %d", len(r.data)-r.pos, tag)
	}
	return result, nil
}

func decodeTVarTxOutCompact(r *tvarMempackReader) (*decodedTVarMempackTxOut, error) {
	addr, err := r.readLengthPrefixedBytes()
	if err != nil {
		return nil, fmt.Errorf("read CompactAddr: %w", err)
	}
	lovelace, assets, err := decodeTVarCompactValue(r)
	if err != nil {
		return nil, fmt.Errorf("read Value: %w", err)
	}
	return &decodedTVarMempackTxOut{Address: addr, Lovelace: lovelace, Assets: assets}, nil
}

func decodeTVarTxOutCompactDH(r *tvarMempackReader) (*decodedTVarMempackTxOut, error) {
	out, err := decodeTVarTxOutCompact(r)
	if err != nil {
		return nil, err
	}
	out.DatumHash, err = r.readBytes(32)
	if err != nil {
		return nil, fmt.Errorf("read DataHash: %w", err)
	}
	return out, nil
}

func decodeTVarTxOutCompactDatum(r *tvarMempackReader) (*decodedTVarMempackTxOut, error) {
	out, err := decodeTVarTxOutCompact(r)
	if err != nil {
		return nil, err
	}
	out.Datum, err = r.readLengthPrefixedBytes()
	if err != nil {
		return nil, fmt.Errorf("read inline Datum: %w", err)
	}
	return out, nil
}

func decodeTVarTxOutCompactRefScript(r *tvarMempackReader) (*decodedTVarMempackTxOut, error) {
	out, err := decodeTVarTxOutCompact(r)
	if err != nil {
		return nil, err
	}
	out.DatumHash, out.Datum, err = decodeTVarMempackDatum(r)
	if err != nil {
		return nil, fmt.Errorf("read Datum: %w", err)
	}
	out.ScriptRef, err = decodeTVarMempackScript(r)
	if err != nil {
		return nil, fmt.Errorf("read Script: %w", err)
	}
	return out, nil
}

func decodeTVarTxOutAddrHash28(r *tvarMempackReader, hasDataHash bool) (*decodedTVarMempackTxOut, error) {
	stakingCredTag, err := r.readByte()
	if err != nil {
		return nil, fmt.Errorf("read staking Credential tag: %w", err)
	}
	if stakingCredTag > 1 {
		return nil, fmt.Errorf("invalid staking Credential tag: %d", stakingCredTag)
	}
	stakingHash, err := r.readBytes(28)
	if err != nil {
		return nil, fmt.Errorf("read staking Credential hash: %w", err)
	}
	w0, err := r.readWord64LE()
	if err != nil {
		return nil, fmt.Errorf("read Addr28Extra w0: %w", err)
	}
	w1, err := r.readWord64LE()
	if err != nil {
		return nil, fmt.Errorf("read Addr28Extra w1: %w", err)
	}
	w2, err := r.readWord64LE()
	if err != nil {
		return nil, fmt.Errorf("read Addr28Extra w2: %w", err)
	}
	w3, err := r.readWord64LE()
	if err != nil {
		return nil, fmt.Errorf("read Addr28Extra w3: %w", err)
	}
	coinTag, err := r.readByte()
	if err != nil {
		return nil, fmt.Errorf("read CompactForm Coin tag: %w", err)
	}
	if coinTag != 0 {
		return nil, fmt.Errorf("expected CompactForm Coin tag 0, got %d", coinTag)
	}
	coin, err := r.readVarLenUint64()
	if err != nil {
		return nil, fmt.Errorf("read Coin VarLen: %w", err)
	}

	var datumHash []byte
	if hasDataHash {
		datumHash, err = r.readBytes(32)
		if err != nil {
			return nil, fmt.Errorf("read DataHash32: %w", err)
		}
	}

	return &decodedTVarMempackTxOut{
		Address:   reconstructTVarAddr28(stakingCredTag, stakingHash, w0, w1, w2, w3),
		Lovelace:  coin,
		DatumHash: datumHash,
	}, nil
}

func decodeTVarCompactValue(r *tvarMempackReader) (uint64, []tvarAsset, error) {
	tag, err := r.readByte()
	if err != nil {
		return 0, nil, fmt.Errorf("read Value tag: %w", err)
	}
	switch tag {
	case 0:
		coin, err := r.readVarLenUint64()
		if err != nil {
			return 0, nil, fmt.Errorf("read AdaOnly Coin: %w", err)
		}
		return coin, nil, nil
	case 1:
		coin, err := r.readVarLenUint64()
		if err != nil {
			return 0, nil, fmt.Errorf("read MultiAsset Coin: %w", err)
		}
		numAssets, err := r.readVarLen()
		if err != nil {
			return 0, nil, fmt.Errorf("read asset count: %w", err)
		}
		flatBytes, err := r.readLengthPrefixedBytes()
		if err != nil {
			return 0, nil, fmt.Errorf("read multi-asset bytes: %w", err)
		}
		assets, err := decodeTVarFlatMultiAsset(numAssets, flatBytes)
		if err != nil {
			return coin, nil, err
		}
		return coin, assets, nil
	default:
		return 0, nil, fmt.Errorf("unknown CompactValue tag: %d", tag)
	}
}

func decodeTVarFlatMultiAsset(numAssets int, flat []byte) ([]tvarAsset, error) {
	if numAssets == 0 {
		return nil, nil
	}
	if numAssets > len(flat)/12 {
		return nil, fmt.Errorf("multi-asset count %d exceeds maximum for buffer size %d", numAssets, len(flat))
	}
	quantitiesEnd := numAssets * 8
	pidOffsetsEnd := quantitiesEnd + numAssets*2
	nameOffsetsEnd := pidOffsetsEnd + numAssets*2
	if len(flat) < nameOffsetsEnd {
		return nil, fmt.Errorf("multi-asset data too short: need %d, have %d", nameOffsetsEnd, len(flat))
	}

	type entry struct {
		quantity uint64
		pidOff   int
		nameOff  int
	}
	entries := make([]entry, numAssets)
	uniquePidOffs := make(map[int]struct{})
	for idx := 0; idx < numAssets; idx++ {
		entries[idx].quantity = binary.LittleEndian.Uint64(flat[idx*8 : idx*8+8])
		entries[idx].pidOff = int(binary.LittleEndian.Uint16(flat[quantitiesEnd+idx*2 : quantitiesEnd+idx*2+2]))
		entries[idx].nameOff = int(binary.LittleEndian.Uint16(flat[pidOffsetsEnd+idx*2 : pidOffsetsEnd+idx*2+2]))
		if entries[idx].pidOff < nameOffsetsEnd {
			return nil, fmt.Errorf("policy ID offset %d for asset %d is before content region", entries[idx].pidOff, idx)
		}
		if entries[idx].nameOff < nameOffsetsEnd {
			return nil, fmt.Errorf("asset name offset %d for asset %d is before content region", entries[idx].nameOff, idx)
		}
		uniquePidOffs[entries[idx].pidOff] = struct{}{}
	}

	knownNameBytes := 0
	for idx := 0; idx < numAssets-1; idx++ {
		nameLen := entries[idx+1].nameOff - entries[idx].nameOff
		if nameLen < 0 {
			return nil, fmt.Errorf("multi-asset name offsets not ascending at index %d", idx)
		}
		knownNameBytes += nameLen
	}
	regionEStart := nameOffsetsEnd + len(uniquePidOffs)*28
	if regionEStart+knownNameBytes > len(flat) {
		return nil, fmt.Errorf("multi-asset buffer too short for content")
	}
	for idx := range entries {
		if entries[idx].nameOff < regionEStart {
			return nil, fmt.Errorf("asset name offset %d for asset %d falls within policy ID region", entries[idx].nameOff, idx)
		}
	}

	assets := make([]tvarAsset, numAssets)
	for idx, entry := range entries {
		if entry.pidOff+28 > len(flat) {
			return nil, fmt.Errorf("policy ID offset %d out of bounds", entry.pidOff)
		}
		nameEnd := len(flat)
		if idx+1 < numAssets {
			nameEnd = entries[idx+1].nameOff
		}
		if entry.nameOff > nameEnd || nameEnd > len(flat) {
			return nil, fmt.Errorf("asset name offset %d out of bounds", entry.nameOff)
		}
		assets[idx] = tvarAsset{
			PolicyID: bytes.Clone(flat[entry.pidOff : entry.pidOff+28]),
			Name:     bytes.Clone(flat[entry.nameOff:nameEnd]),
			Amount:   entry.quantity,
		}
	}
	return assets, nil
}

func decodeTVarMempackDatum(r *tvarMempackReader) ([]byte, []byte, error) {
	tag, err := r.readByte()
	if err != nil {
		return nil, nil, fmt.Errorf("read Datum tag: %w", err)
	}
	switch tag {
	case 0:
		return nil, nil, nil
	case 1:
		hash, err := r.readBytes(32)
		return hash, nil, err
	case 2:
		datum, err := r.readLengthPrefixedBytes()
		return nil, datum, err
	default:
		return nil, nil, fmt.Errorf("unknown Datum tag: %d", tag)
	}
}

func decodeTVarMempackScript(r *tvarMempackReader) ([]byte, error) {
	scriptTag, err := r.readByte()
	if err != nil {
		return nil, fmt.Errorf("read AlonzoScript tag: %w", err)
	}
	switch scriptTag {
	case 0:
		return r.readLengthPrefixedBytes()
	case 1:
		versionTag, err := r.readByte()
		if err != nil {
			return nil, fmt.Errorf("read PlutusScript version tag: %w", err)
		}
		if versionTag > 2 {
			return nil, fmt.Errorf("unknown PlutusScript version tag: %d", versionTag)
		}
		script, err := r.readLengthPrefixedBytes()
		if err != nil {
			return nil, fmt.Errorf("read PlutusScript V%d: %w", versionTag+1, err)
		}
		return append([]byte{versionTag}, script...), nil
	default:
		return nil, fmt.Errorf("unknown AlonzoScript tag: %d", scriptTag)
	}
}

func reconstructTVarAddr28(stakingCredTag byte, stakingHash []byte, w0, w1, w2, w3 uint64) []byte {
	paymentHash := make([]byte, 28)
	binary.BigEndian.PutUint64(paymentHash[0:8], w0)
	binary.BigEndian.PutUint64(paymentHash[8:16], w1)
	binary.BigEndian.PutUint64(paymentHash[16:24], w2)
	binary.BigEndian.PutUint32(paymentHash[24:28], uint32(w3>>32))

	isPayKeyHash := (w3 & 1) != 0
	isMainnet := (w3 & 2) != 0
	isStakingKeyHash := stakingCredTag == 1

	var networkNibble byte
	if isMainnet {
		networkNibble = 1
	}

	var addrType byte
	switch {
	case isPayKeyHash && isStakingKeyHash:
		addrType = 0
	case !isPayKeyHash && isStakingKeyHash:
		addrType = 1
	case isPayKeyHash && !isStakingKeyHash:
		addrType = 2
	default:
		addrType = 3
	}

	addr := make([]byte, 57)
	addr[0] = (addrType << 4) | networkNibble
	copy(addr[1:29], paymentHash)
	copy(addr[29:57], stakingHash)
	return addr
}

func isTVarMempackFormat(data []byte) bool {
	return len(data) > 0 && data[0] <= 5
}

func cborContainerHeader(data []byte, expectedMajor byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("empty data")
	}
	major := data[0] >> 5
	if major != expectedMajor {
		return 0, 0, fmt.Errorf("expected CBOR major type %d, got %d", expectedMajor, major)
	}
	info := data[0] & 0x1f
	if info == 31 {
		return 0, 0, fmt.Errorf("indefinite-length container is not valid here")
	}
	return tvarCBORArgument(data)
}

func tvarCBORArgument(data []byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("empty data")
	}
	info := data[0] & 0x1f
	switch {
	case info < 24:
		return uint64(info), 1, nil
	case info == 24:
		if len(data) < 2 {
			return 0, 0, fmt.Errorf("truncated header")
		}
		return uint64(data[1]), 2, nil
	case info == 25:
		if len(data) < 3 {
			return 0, 0, fmt.Errorf("truncated header")
		}
		return uint64(data[1])<<8 | uint64(data[2]), 3, nil
	case info == 26:
		if len(data) < 5 {
			return 0, 0, fmt.Errorf("truncated header")
		}
		return uint64(data[1])<<24 | uint64(data[2])<<16 | uint64(data[3])<<8 | uint64(data[4]), 5, nil
	case info == 27:
		if len(data) < 9 {
			return 0, 0, fmt.Errorf("truncated header")
		}
		return uint64(data[1])<<56 | uint64(data[2])<<48 | uint64(data[3])<<40 | uint64(data[4])<<32 | uint64(data[5])<<24 | uint64(data[6])<<16 | uint64(data[7])<<8 | uint64(data[8]), 9, nil
	default:
		return 0, 0, fmt.Errorf("unsupported CBOR additional info: %d", info)
	}
}

func tvarCBORItemSize(data []byte) (int, error) {
	return tvarCBORItemSizeDepth(data, 0)
}

func tvarCBORItemSizeDepth(data []byte, depth int) (int, error) {
	if depth > 128 {
		return 0, fmt.Errorf("CBOR nesting exceeds max depth")
	}
	if len(data) == 0 {
		return 0, fmt.Errorf("empty data")
	}

	major := data[0] >> 5
	info := data[0] & 0x1f
	switch major {
	case 0, 1:
		_, hLen, err := tvarCBORArgument(data)
		return hLen, err
	case 2, 3:
		if info == 31 {
			pos := 1
			for pos < len(data) {
				if data[pos] == 0xff {
					return pos + 1, nil
				}
				chunkSize, err := tvarCBORItemSizeDepth(data[pos:], depth+1)
				if err != nil {
					return 0, err
				}
				pos += chunkSize
			}
			return 0, fmt.Errorf("unterminated indefinite string")
		}
		arg, hLen, err := tvarCBORArgument(data)
		if err != nil {
			return 0, err
		}
		if arg > uint64(math.MaxInt-hLen) {
			return 0, fmt.Errorf("byte/text string length %d overflows int", arg)
		}
		size := hLen + int(arg)
		if size > len(data) {
			return 0, fmt.Errorf("byte/text string claims %d bytes but only %d available", size, len(data))
		}
		return size, nil
	case 4:
		if info == 31 {
			pos := 1
			for pos < len(data) {
				if data[pos] == 0xff {
					return pos + 1, nil
				}
				itemSize, err := tvarCBORItemSizeDepth(data[pos:], depth+1)
				if err != nil {
					return 0, err
				}
				pos += itemSize
			}
			return 0, fmt.Errorf("unterminated indefinite array")
		}
		count, hLen, err := tvarCBORArgument(data)
		if err != nil {
			return 0, err
		}
		pos := hLen
		for idx := uint64(0); idx < count; idx++ {
			itemSize, err := tvarCBORItemSizeDepth(data[pos:], depth+1)
			if err != nil {
				return 0, err
			}
			pos += itemSize
		}
		return pos, nil
	case 5:
		if info == 31 {
			pos := 1
			for pos < len(data) {
				if data[pos] == 0xff {
					return pos + 1, nil
				}
				keySize, err := tvarCBORItemSizeDepth(data[pos:], depth+1)
				if err != nil {
					return 0, err
				}
				pos += keySize
				valSize, err := tvarCBORItemSizeDepth(data[pos:], depth+1)
				if err != nil {
					return 0, err
				}
				pos += valSize
			}
			return 0, fmt.Errorf("unterminated indefinite map")
		}
		count, hLen, err := tvarCBORArgument(data)
		if err != nil {
			return 0, err
		}
		pos := hLen
		for idx := uint64(0); idx < count; idx++ {
			keySize, err := tvarCBORItemSizeDepth(data[pos:], depth+1)
			if err != nil {
				return 0, err
			}
			pos += keySize
			valSize, err := tvarCBORItemSizeDepth(data[pos:], depth+1)
			if err != nil {
				return 0, err
			}
			pos += valSize
		}
		return pos, nil
	case 6:
		_, hLen, err := tvarCBORArgument(data)
		if err != nil {
			return 0, err
		}
		contentSize, err := tvarCBORItemSizeDepth(data[hLen:], depth+1)
		if err != nil {
			return 0, err
		}
		return hLen + contentSize, nil
	case 7:
		switch info {
		case 24:
			if len(data) < 2 {
				return 0, fmt.Errorf("truncated simple value")
			}
			return 2, nil
		case 25:
			if len(data) < 3 {
				return 0, fmt.Errorf("truncated float16")
			}
			return 3, nil
		case 26:
			if len(data) < 5 {
				return 0, fmt.Errorf("truncated float32")
			}
			return 5, nil
		case 27:
			if len(data) < 9 {
				return 0, fmt.Errorf("truncated float64")
			}
			return 9, nil
		case 31:
			return 0, fmt.Errorf("unexpected break code")
		default:
			return 1, nil
		}
	default:
		return 0, fmt.Errorf("unknown CBOR major type %d", major)
	}
}
