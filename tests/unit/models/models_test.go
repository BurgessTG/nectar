package models_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"nectar/models"
)

func ptr[T any](v T) *T {
	return &v
}

func setupModelTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)

	err = db.AutoMigrate(
		&models.Block{},
		&models.Tx{},
		&models.MultiAsset{},
		&models.TokenHolder{},
		&models.TokenWalletConnection{},
	)
	require.NoError(t, err)

	return db
}

func testBytes(size int, fill byte) []byte {
	return bytes.Repeat([]byte{fill}, size)
}

func TestCurrentTableNames(t *testing.T) {
	assert.Equal(t, "blocks", models.Block{}.TableName())
	assert.Equal(t, "txes", models.Tx{}.TableName())
	assert.Equal(t, "multi_assets", models.MultiAsset{}.TableName())
	assert.Equal(t, "token_holders", models.TokenHolder{}.TableName())
	assert.Equal(t, "token_wallet_connections", models.TokenWalletConnection{}.TableName())
}

func TestBlockAndTxUseHashPrimaryKeys(t *testing.T) {
	db := setupModelTestDB(t)
	blockHash := testBytes(32, 0x01)
	txHash := testBytes(32, 0x02)
	fee := int64(170000)

	block := models.Block{
		Hash:           blockHash,
		EpochNo:        ptr(uint32(365)),
		SlotNo:         ptr(uint64(1000000)),
		BlockNo:        ptr(uint32(50000)),
		SlotLeaderHash: testBytes(28, 0x03),
		Size:           2048,
		Time:           time.Unix(1_700_000_000, 0).UTC(),
		TxCount:        1,
		ProtoMajor:     8,
		ProtoMinor:     0,
		Era:            "Babbage",
	}
	require.NoError(t, db.Create(&block).Error)

	tx := models.Tx{
		Hash:       txHash,
		BlockHash:  blockHash,
		BlockIndex: 0,
		Fee:        &fee,
		Size:       128,
	}
	require.NoError(t, db.Create(&tx).Error)

	var got models.Tx
	err := db.Preload("Block").Where("hash = ?", txHash).First(&got).Error

	require.NoError(t, err)
	assert.Equal(t, txHash, got.Hash)
	assert.Equal(t, blockHash, got.BlockHash)
	assert.Equal(t, uint32(0), got.BlockIndex)
	assert.Equal(t, fee, *got.Fee)
	assert.Equal(t, blockHash, got.Block.Hash)
}

func TestBlockHashIsUnique(t *testing.T) {
	db := setupModelTestDB(t)
	block := models.Block{
		Hash:           testBytes(32, 0x11),
		SlotNo:         ptr(uint64(42)),
		SlotLeaderHash: testBytes(28, 0x12),
		Size:           1,
		Time:           time.Unix(10, 0).UTC(),
		TxCount:        0,
		ProtoMajor:     8,
		ProtoMinor:     0,
		Era:            "Babbage",
	}
	require.NoError(t, db.Create(&block).Error)

	duplicate := block
	duplicate.SlotNo = ptr(uint64(43))

	assert.Error(t, db.Create(&duplicate).Error)
}

func TestMultiAssetCompositeKey(t *testing.T) {
	db := setupModelTestDB(t)
	asset := models.MultiAsset{
		Policy:      testBytes(28, 0x21),
		Name:        []byte("TOKEN"),
		Fingerprint: "asset1testfingerprint",
	}

	require.NoError(t, db.Create(&asset).Error)

	var got models.MultiAsset
	err := db.Where("policy = ? AND name = ?", asset.Policy, asset.Name).First(&got).Error

	require.NoError(t, err)
	assert.Equal(t, asset.Policy, got.Policy)
	assert.Equal(t, asset.Name, got.Name)
	assert.Equal(t, asset.Fingerprint, got.Fingerprint)
}

func TestTokenHolderCompositeKey(t *testing.T) {
	db := setupModelTestDB(t)
	holder := models.TokenHolder{
		Policy:          testBytes(28, 0x31),
		Name:            []byte("SNEK"),
		Address:         "addr1testholder",
		Amount:          1_000_000,
		TxHash:          testBytes(32, 0x32),
		OutputIndex:     1,
		StakeAddress:    "stake1test",
		LastUpdatedSlot: 12345,
	}

	require.NoError(t, db.Create(&holder).Error)

	var got models.TokenHolder
	err := db.Where("policy = ? AND name = ? AND address = ?", holder.Policy, holder.Name, holder.Address).First(&got).Error

	require.NoError(t, err)
	assert.Equal(t, holder.Amount, got.Amount)
	assert.Equal(t, holder.TxHash, got.TxHash)
	assert.Equal(t, holder.LastUpdatedSlot, got.LastUpdatedSlot)
}

func TestTokenWalletConnectionCompositeKey(t *testing.T) {
	db := setupModelTestDB(t)
	connection := models.TokenWalletConnection{
		TxHash:           testBytes(32, 0x41),
		TxIndex:          2,
		PolicyID:         testBytes(28, 0x42),
		AssetName:        []byte("TOKEN"),
		FromAddress:      "addr1from",
		ToAddress:        "addr1to",
		FromStakeAddress: "stake1from",
		ToStakeAddress:   "stake1to",
		Quantity:         500,
		Slot:             999,
		BlockHeight:      100,
	}

	require.NoError(t, db.Create(&connection).Error)

	var got models.TokenWalletConnection
	err := db.Where(
		"tx_hash = ? AND tx_index = ? AND policy_id = ? AND asset_name = ?",
		connection.TxHash,
		connection.TxIndex,
		connection.PolicyID,
		connection.AssetName,
	).First(&got).Error

	require.NoError(t, err)
	assert.Equal(t, connection.FromAddress, got.FromAddress)
	assert.Equal(t, connection.ToAddress, got.ToAddress)
	assert.Equal(t, connection.Quantity, got.Quantity)
	assert.Equal(t, connection.Slot, got.Slot)
}
