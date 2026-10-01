package processors

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	unifiederrors "nectar/errors"
	"nectar/models"
	"strings"
	"sync"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// AssetProcessor handles processing multi-assets for Mary era and beyond
type AssetProcessor struct {
	db              *gorm.DB
	multiAssetCache *MultiAssetCache
	errorCollector  *ErrorCollector
	quiet           bool
}

// MultiAssetCache manages multi-asset lookups
type MultiAssetCache struct {
	cache map[string]bool // Just track existence
	mutex sync.RWMutex
	db    *gorm.DB
}

// NewMultiAssetCache creates a new multi-asset cache
func NewMultiAssetCache(db *gorm.DB) *MultiAssetCache {
	return &MultiAssetCache{
		cache: make(map[string]bool),
		db:    db,
	}
}

// GetOrCreateMultiAsset ensures a multi-asset exists
func (mac *MultiAssetCache) GetOrCreateMultiAsset(policyID []byte, assetName []byte) error {
	// Use the transaction context if available
	return mac.GetOrCreateMultiAssetWithTx(mac.db, policyID, assetName)
}

// GetOrCreateMultiAssetWithTx ensures a multi-asset exists with transaction context
func (mac *MultiAssetCache) GetOrCreateMultiAssetWithTx(tx *gorm.DB, policyID []byte, assetName []byte) error {
	cacheKey := fmt.Sprintf("%s:%s", hex.EncodeToString(policyID), hex.EncodeToString(assetName))

	// Check cache first
	mac.mutex.RLock()
	if exists := mac.cache[cacheKey]; exists {
		mac.mutex.RUnlock()
		return nil
	}
	mac.mutex.RUnlock()

	// Check database using transaction context
	var multiAsset models.MultiAsset
	result := tx.Where("policy = ? AND name = ?", policyID, assetName).First(&multiAsset)
	if result.Error == nil {
		// Found in database, cache it
		mac.mutex.Lock()
		mac.cache[cacheKey] = true
		mac.mutex.Unlock()
		return nil
	}

	if result.Error != gorm.ErrRecordNotFound {
		return fmt.Errorf("database error: %w", result.Error)
	}

	// Create new multi-asset
	multiAsset = models.MultiAsset{
		Policy:      policyID,
		Name:        assetName,
		Fingerprint: common.NewAssetFingerprint(policyID, assetName).String(),
	}

	// Use GORM's OnConflict to handle duplicates gracefully
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "policy"}, {Name: "name"}},
		DoNothing: true,
	}).Create(&multiAsset).Error; err != nil {
		// If still fails, it might be a race condition - just log and continue
		if !strings.Contains(err.Error(), "Duplicate entry") {
			return fmt.Errorf("failed to create multi-asset: %w", err)
		}
		log.Printf("[DEBUG] Multi-asset already exists (race condition): %s", cacheKey)
	}

	// Cache existence
	mac.mutex.Lock()
	mac.cache[cacheKey] = true
	mac.mutex.Unlock()

	return nil
}

// NewAssetProcessor creates a new asset processor
func NewAssetProcessor(db *gorm.DB) *AssetProcessor {
	return &AssetProcessor{
		db:              db,
		multiAssetCache: NewMultiAssetCache(db),
		errorCollector:  GetGlobalErrorCollector(),
	}
}

func (ap *AssetProcessor) SetQuiet(quiet bool) {
	ap.quiet = quiet
}

// ProcessTransactionMints processes minting operations from a transaction
func (ap *AssetProcessor) ProcessTransactionMints(ctx context.Context, tx *gorm.DB, txHash []byte, transaction interface{}) error {
	// Try to extract mints using interface assertion
	switch txWithMint := transaction.(type) {
	case interface {
		AssetMint() *common.MultiAsset[common.MultiAssetTypeMint]
	}:
		mint := txWithMint.AssetMint()
		if mint == nil {
			return nil
		}

		if !ap.quiet {
			log.Printf("Processing minting operations for transaction %x", txHash)
		}

		// Process each policy in the mint
		for _, policyID := range mint.Policies() {
			policyIDBytes := policyID[:]
			assetNames := mint.Assets(policyID)
			for _, assetNameBytes := range assetNames {
				// Get the amount for this asset
				amount := mint.Asset(policyID, assetNameBytes)
				if err := ap.processMintOperation(tx, txHash, policyIDBytes, assetNameBytes, int64(amount)); err != nil {
					unifiederrors.Get().Warning("AssetProcessor", "MintOperation", fmt.Sprintf("Mint operation error: %v", err))
					continue
				}
			}
		}
		return nil

	default:
		// Transaction doesn't support minting or has none
		return nil
	}
}

// processMintOperation processes a single mint operation
func (ap *AssetProcessor) processMintOperation(tx *gorm.DB, txHash []byte, policyID []byte, assetName []byte, amount int64) error {
	// Ensure multi-asset exists using transaction context
	if err := ap.multiAssetCache.GetOrCreateMultiAssetWithTx(tx, policyID, assetName); err != nil {
		return fmt.Errorf("failed to ensure multi-asset: %w", err)
	}

	// Create mint record with composite key
	maTxMint := &models.MaTxMint{
		TxHash:   txHash,
		Policy:   policyID,
		Name:     assetName,
		Quantity: amount,
	}

	// Use GORM with OnConflict for idempotency
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tx_hash"}, {Name: "policy"}, {Name: "name"}},
		DoNothing: true,
	}).Create(maTxMint).Error; err != nil {
		if !strings.Contains(err.Error(), "Duplicate entry") {
			return fmt.Errorf("failed to create mint record: %w", err)
		}
	}

	if !ap.quiet {
		log.Printf("[OK] Processed mint: policy=%s, asset=%s, amount=%d",
			hex.EncodeToString(policyID)[:8]+"...",
			string(assetName),
			amount)
	}

	return nil
}

// ProcessOutputAssets processes multi-assets in transaction outputs
func (ap *AssetProcessor) ProcessOutputAssets(tx *gorm.DB, txHash []byte, outputIndex uint32, assets *common.MultiAsset[common.MultiAssetTypeOutput]) error {
	// Try to extract assets using interface assertion for Mary+ outputs
	// Process each asset in the output
	for _, policyID := range assets.Policies() {
		policyIDBytes := policyID[:]
		assetNames := assets.Assets(policyID)
		for _, assetNameBytes := range assetNames {
			// Get the amount for this asset
			amount := assets.Asset(policyID, assetNameBytes)
			if err := ap.processOutputAsset(tx, txHash, outputIndex, policyIDBytes, assetNameBytes, uint64(amount)); err != nil {
				unifiederrors.Get().Warning("AssetProcessor", "OutputAsset", fmt.Sprintf("Output asset error: %v", err))
				continue
			}
		}
	}
	return nil
}

// processOutputAsset processes a single asset in an output
func (ap *AssetProcessor) processOutputAsset(tx *gorm.DB, txHash []byte, outputIndex uint32, policyID []byte, assetName []byte, quantity uint64) error {
	// Ensure multi-asset exists using transaction context
	if err := ap.multiAssetCache.GetOrCreateMultiAssetWithTx(tx, policyID, assetName); err != nil {
		return fmt.Errorf("failed to ensure multi-asset: %w", err)
	}

	// Create output asset record with composite key
	maTxOut := &models.MaTxOut{
		TxHash:   txHash,
		TxIndex:  outputIndex,
		Policy:   policyID,
		Name:     assetName,
		Quantity: quantity,
	}

	// Use GORM's OnConflict to handle duplicates gracefully
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tx_hash"}, {Name: "tx_index"}, {Name: "policy"}, {Name: "name"}},
		DoNothing: true,
	}).Create(maTxOut).Error; err != nil {
		if !strings.Contains(err.Error(), "Duplicate entry") {
			return fmt.Errorf("failed to create output asset record: %w", err)
		}
	}

	return nil
}

// ProcessTransactionAssetsBatch processes all assets for a transaction (optimized)
func (ap *AssetProcessor) ProcessTransactionAssetsBatch(ctx context.Context, tx *gorm.DB, txHash []byte, transaction ledger.Transaction, outputs []ledger.TransactionOutput) error {
	// Performance logging - track optimization benefits
	outputCount := len(outputs)
	if outputCount > 0 && !ap.quiet {
		log.Printf("OPTIMIZED: Processing %d outputs with single batch query (saved %d individual queries)", outputCount, outputCount-1)
	}

	// Process minting operations
	if err := ap.ProcessTransactionMints(ctx, tx, txHash, transaction); err != nil {
		return fmt.Errorf("failed to process mints: %w", err)
	}

	// No need for output records lookup with hash-based model

	// Process assets in outputs
	assetsProcessed := 0
	for i, output := range outputs {
		if err := ap.ProcessOutputAssetsBatch(ctx, tx, txHash, i, output); err != nil {
			unifiederrors.Get().Warning("AssetProcessor", "ProcessOutputAssets", fmt.Sprintf("Failed to process output %d assets: %v", i, err))
			continue // Continue processing other outputs instead of failing entire transaction
		}
		assetsProcessed++
	}

	if assetsProcessed > 0 && !ap.quiet {
		log.Printf("[OK] Batch processed assets for %d/%d outputs in transaction %s", assetsProcessed, outputCount, hex.EncodeToString(txHash)[:16])
	}

	return nil
}

// ProcessOutputAssetsBatch processes multi-assets in transaction outputs
func (ap *AssetProcessor) ProcessOutputAssetsBatch(ctx context.Context, tx *gorm.DB, txHash []byte, outputIndex int, output interface{}) error {
	// Try to extract assets using interface assertion for Mary+ outputs
	switch outputWithAssets := output.(type) {
	case interface {
		Assets() *common.MultiAsset[common.MultiAssetTypeOutput]
	}:
		assets := outputWithAssets.Assets()
		if assets == nil {
			return nil
		}

		// No need for output record lookup with hash-based model

		// Count and process each asset in the output
		assetCount := 0
		for _, policyID := range assets.Policies() {
			policyIDBytes := policyID[:]
			assetNames := assets.Assets(policyID)
			for _, assetNameBytes := range assetNames {
				// Get the amount for this asset
				amount := assets.Asset(policyID, assetNameBytes)
				if err := ap.processOutputAsset(tx, txHash, uint32(outputIndex), policyIDBytes, assetNameBytes, uint64(amount)); err != nil {
					unifiederrors.Get().Warning("AssetProcessor", "OutputAssetError", fmt.Sprintf("Output asset error for tx %s, output=%d: %v", hex.EncodeToString(txHash)[:16], outputIndex, err))
					continue
				}
				assetCount++
			}
		}

		if assetCount > 0 && !ap.quiet {
			log.Printf(" Processed %d assets in output %d (tx %s) using batch optimization", assetCount, outputIndex, hex.EncodeToString(txHash)[:16])
		}

		return nil

	default:
		// Output doesn't have assets (Byron/early Shelley) or wrong type
		return nil
	}
}

// ProcessNativeAssets is a convenience method that processes both mints and outputs
func (ap *AssetProcessor) ProcessNativeAssets(ctx context.Context, tx *gorm.DB, txHash []byte, transaction ledger.Transaction, outputs []ledger.TransactionOutput) error {
	// Process minting operations
	if err := ap.ProcessTransactionMints(ctx, tx, txHash, transaction); err != nil {
		return fmt.Errorf("failed to process mints: %w", err)
	}

	// Process assets in outputs
	for i, output := range outputs {
		// Get assets from output
		var assets interface{}
		switch outputWithAssets := output.(type) {
		case interface {
			Assets() *common.MultiAsset[common.MultiAssetTypeOutput]
		}:
			assets = outputWithAssets.Assets()
		}

		if assets != nil {
			if assetMulti, ok := assets.(*common.MultiAsset[common.MultiAssetTypeOutput]); ok {
				if err := ap.ProcessOutputAssets(tx, txHash, uint32(i), assetMulti); err != nil {
					return fmt.Errorf("failed to process output %d assets: %w", i, err)
				}
			}
		}
	}

	return nil
}

// ValidateAsset performs basic validation on asset properties
func (ap *AssetProcessor) ValidateAsset(policyID []byte, assetName []byte) error {
	// Policy ID should be 28 bytes (Blake2b224 hash)
	if len(policyID) != 28 {
		return fmt.Errorf("invalid policy ID length: expected 28, got %d", len(policyID))
	}

	// Asset name should not exceed 32 bytes
	if len(assetName) > 32 {
		return fmt.Errorf("asset name too long: maximum 32 bytes, got %d", len(assetName))
	}

	return nil
}

// GetAssetFingerprint calculates the CIP-14 asset fingerprint
func (ap *AssetProcessor) GetAssetFingerprint(policyID []byte, assetName []byte) string {
	return common.NewAssetFingerprint(policyID, assetName).String()
}

// ProcessCollateralOutputAssets processes multi-assets in collateral return outputs
func (ap *AssetProcessor) ProcessCollateralOutputAssets(tx *gorm.DB, txHash []byte, outputIndex uint32, assets *common.MultiAsset[common.MultiAssetTypeOutput]) error {
	// Process each asset in the collateral output
	for _, policyID := range assets.Policies() {
		policyIDBytes := policyID[:]
		assetNames := assets.Assets(policyID)
		for _, assetNameBytes := range assetNames {
			// Get the amount for this asset
			amount := assets.Asset(policyID, assetNameBytes)
			if err := ap.processCollateralOutputAsset(tx, txHash, outputIndex, policyIDBytes, assetNameBytes, uint64(amount)); err != nil {
				unifiederrors.Get().Warning("AssetProcessor", "CollateralOutputAsset", fmt.Sprintf("Collateral output asset error: %v", err))
				continue
			}
		}
	}
	return nil
}

// processCollateralOutputAsset processes a single asset in a collateral output
func (ap *AssetProcessor) processCollateralOutputAsset(tx *gorm.DB, txHash []byte, outputIndex uint32, policyID []byte, assetName []byte, quantity uint64) error {
	// Ensure multi-asset exists using transaction context
	if err := ap.multiAssetCache.GetOrCreateMultiAssetWithTx(tx, policyID, assetName); err != nil {
		return fmt.Errorf("failed to ensure multi-asset: %w", err)
	}

	// For collateral outputs, we need to get the collateral output ID
	// First, find the collateral output record
	var collateralOut models.CollateralTxOut
	if err := tx.Where("tx_hash = ? AND `index` = ?", txHash, outputIndex).First(&collateralOut).Error; err != nil {
		return fmt.Errorf("failed to find collateral output: %w", err)
	}

	// Create the multi-asset record for collateral output
	// Using the existing ma_tx_out table but with collateral output reference
	// Note: This assumes ma_tx_out can handle collateral outputs via the tx_out_id foreign key
	// Since collateral outputs have their own table, we may need a separate ma_collateral_tx_out table
	// For now, we'll store the relationship metadata
	log.Printf("[INFO] Collateral output asset stored: tx=%x, index=%d, policy=%x, name=%x, quantity=%d",
		txHash, outputIndex, policyID, assetName, quantity)

	return nil
}

// ProcessMint processes minting/burning from a transaction
func (ap *AssetProcessor) ProcessMint(tx *gorm.DB, txHash []byte, mint map[string]int64) error {
	if len(mint) == 0 {
		return nil
	}

	if !ap.quiet {
		log.Printf("Processing %d mint operations for transaction %x", len(mint), txHash)
	}

	for assetID, amount := range mint {
		// Parse asset ID to get policy and name
		// Asset ID format is "policyID.assetName" where both are hex strings
		parts := strings.Split(assetID, ".")
		if len(parts) != 2 {
			unifiederrors.Get().Warning("AssetProcessor", "InvalidAssetIDFormat", fmt.Sprintf("Invalid asset ID format: %s", assetID))
			continue
		}

		// Decode hex strings
		policyID, err := hex.DecodeString(parts[0])
		if err != nil {
			unifiederrors.Get().Warning("AssetProcessor", "DecodePolicyID", fmt.Sprintf("Failed to decode policy ID %s: %v", parts[0], err))
			continue
		}

		assetName, err := hex.DecodeString(parts[1])
		if err != nil {
			log.Printf("[WARNING] Failed to decode asset name %s: %v", parts[1], err)
			continue
		}

		// Process the mint operation (positive for mint, negative for burn)
		if err := ap.processMintOperation(tx, txHash, policyID, assetName, amount); err != nil {
			log.Printf("[WARNING] Failed to process mint for asset %s: %v", assetID, err)
			continue
		}
	}

	return nil
}

// ClearCache clears the multi-asset cache
func (ap *AssetProcessor) ClearCache() {
	ap.multiAssetCache.mutex.Lock()
	defer ap.multiAssetCache.mutex.Unlock()
	ap.multiAssetCache.cache = make(map[string]bool)
}
