package processors

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"nectar/models"

	"github.com/blinklabs-io/gouroboros/cbor"
	"gorm.io/gorm"
)

// MetadataProcessor handles processing transaction metadata for Shelley era and beyond
type MetadataProcessor struct {
	db             *gorm.DB
	errorCollector *ErrorCollector
	quiet          bool
}

// NewMetadataProcessor creates a new metadata processor
func NewMetadataProcessor(db *gorm.DB) *MetadataProcessor {
	return &MetadataProcessor{
		db:             db,
		errorCollector: GetGlobalErrorCollector(),
	}
}

func (mp *MetadataProcessor) SetQuiet(quiet bool) {
	mp.quiet = quiet
}

// ProcessTransactionMetadata processes metadata from a transaction
func (mp *MetadataProcessor) ProcessTransactionMetadata(ctx context.Context, tx *gorm.DB, txHash []byte, transaction interface{}, blockType uint) error {
	// All eras can have metadata - Byron stores it in Attributes field
	// Shelley+ stores it in TxMetadata field
	// Both return *cbor.LazyValue from Metadata() method

	// Try to extract metadata using the CORRECT interface assertion
	switch txWithMetadata := transaction.(type) {
	case interface{ Metadata() *cbor.LazyValue }:
		lazyMetadata := txWithMetadata.Metadata()
		if lazyMetadata == nil {
			return nil
		}

		// Decode the CBOR lazy value to get the actual metadata
		decodedValue := lazyMetadata.Value()
		if decodedValue == nil {
			// Try to decode if Value() returns nil
			decoded, err := lazyMetadata.Decode()
			if err != nil {
				mp.errorCollector.ProcessingWarning("MetadataProcessor", "ProcessTransactionMetadata",
					fmt.Sprintf("failed to decode metadata: %v", err),
					fmt.Sprintf("tx_hash:%x", txHash))
				return nil
			}
			decodedValue = decoded
		}

		// Handle different metadata formats
		var metadata map[uint64]interface{}

		switch v := decodedValue.(type) {
		case map[uint64]interface{}:
			// Standard format for Shelley+ eras
			metadata = v

		case map[interface{}]interface{}:
			// Alternative format - convert keys to uint64
			metadata = make(map[uint64]interface{})
			for k, val := range v {
				switch key := k.(type) {
				case uint64:
					metadata[key] = val
				case int64:
					metadata[uint64(key)] = val
				case int:
					metadata[uint64(key)] = val
				case float64:
					metadata[uint64(key)] = val
				default:
					// For non-numeric keys, use hash of string representation
					keyStr := fmt.Sprintf("%v", key)
					// Use a simple hash for non-numeric keys
					var hash uint64
					for _, c := range keyStr {
						hash = hash*31 + uint64(c)
					}
					metadata[hash] = val
				}
			}

		case []interface{}:
			// Byron might store metadata as array - wrap in map
			metadata = map[uint64]interface{}{0: v}

		default:
			// Any other format - wrap in map with key 0
			if decodedValue != nil {
				metadata = map[uint64]interface{}{0: decodedValue}
			}
		}

		if len(metadata) == 0 {
			return nil
		}

		if !mp.quiet {
			log.Printf("Processing %d metadata entries for transaction %x (era %d)", len(metadata), txHash, blockType)
		}

		for key, value := range metadata {
			if err := mp.processMetadataEntry(tx, txHash, key, value); err != nil {
				mp.errorCollector.ProcessingWarning("MetadataProcessor", "processMetadataEntry",
					fmt.Sprintf("failed to process metadata entry %d: %v", key, err),
					fmt.Sprintf("tx_hash:%x, key:%d", txHash, key))
			}
		}

		return nil

	case interface{ AuxDataHash() []byte }:
		// Transaction has metadata hash but no direct metadata access
		// This is normal for transactions where metadata is provided separately
		return nil

	default:
		// This should never happen as all transaction types implement Metadata() *cbor.LazyValue
		mp.errorCollector.ProcessingWarning("MetadataProcessor", "ProcessTransactionMetadata",
			"transaction does not implement Metadata() method",
			fmt.Sprintf("tx_hash:%x, type:%T", txHash, transaction))
		return nil
	}
}

// processMetadataEntry processes a single metadata entry
func (mp *MetadataProcessor) processMetadataEntry(tx *gorm.DB, txHash []byte, key uint64, value interface{}) error {
	// Convert value to JSON if possible
	var jsonStr *string
	var rawBytes []byte

	// Try to convert to JSON
	if jsonBytes, err := json.Marshal(value); err == nil {
		jsonString := string(jsonBytes)
		jsonStr = &jsonString
	}

	// Try to get raw bytes representation
	switch v := value.(type) {
	case []byte:
		rawBytes = v
	case string:
		rawBytes = []byte(v)
	default:
		// For complex types, use JSON representation as bytes
		if jsonStr != nil {
			rawBytes = []byte(*jsonStr)
		}
	}

	// Create metadata record
	var jsonBytes []byte
	if jsonStr != nil {
		jsonBytes = []byte(*jsonStr)
	}
	metadata := &models.TxMetadata{
		TxHash: txHash,
		Key:    key,
		Json:   jsonBytes,
		Bytes:  rawBytes,
	}

	if err := tx.Create(metadata).Error; err != nil {
		return fmt.Errorf("failed to create metadata record: %w", err)
	}

	if !mp.quiet {
		log.Printf("[OK] Processed metadata entry: tx_hash=%x, key=%d", txHash, key)
	}
	return nil
}

// processScript processes a single script
func (mp *MetadataProcessor) processScript(tx *gorm.DB, txHash []byte, script interface{}, scriptType string) error {
	// Extract script hash and bytes
	var scriptHash []byte
	var scriptBytes []byte

	switch s := script.(type) {
	case interface{ Hash() []byte }:
		scriptHash = s.Hash()
	case interface{ Bytes() []byte }:
		scriptBytes = s.Bytes()
	}

	// Script JSON conversion removed - handled by script processor

	// If we don't have script bytes but have the script object, try to encode it
	if scriptBytes == nil && script != nil {
		if encoded, err := json.Marshal(script); err == nil {
			scriptBytes = encoded
		}
	}

	// Skip if we don't have enough data
	if scriptHash == nil && scriptBytes == nil {
		return fmt.Errorf("script has neither hash nor bytes")
	}

	// Create script record
	// Note: This would typically be handled by the script processor
	// but shown here for completeness
	if !mp.quiet {
		log.Printf("Found script in metadata: type=%s, hash=%s", scriptType, hex.EncodeToString(scriptHash))
	}

	return nil
}

// ValidateMetadata validates metadata according to Cardano rules
func (mp *MetadataProcessor) ValidateMetadata(key uint64, value interface{}, blockType uint) error {
	// Basic validation - key is already uint64 so no need to check max value

	// Convert to JSON to check size
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("metadata value not JSON serializable: %w", err)
	}

	// Shelley era metadata size limit
	const maxMetadataSize = 16384 // 16KB
	if len(jsonBytes) > maxMetadataSize {
		return fmt.Errorf("metadata value too large: %d bytes (max %d)", len(jsonBytes), maxMetadataSize)
	}

	// Additional era-specific validation could go here
	switch blockType {
	case 2, 3, 4: // Shelley, Allegra, Mary
		// Basic validation only
	case 5, 6, 7: // Alonzo, Babbage, Conway
		// Could add Plutus-specific metadata validation
	}

	return nil
}

// getEraName returns the human-readable era name
func (mp *MetadataProcessor) getEraName(blockType uint) string {
	switch blockType {
	case 0:
		return "Byron"
	case 1:
		return "Byron EBB"
	case 2:
		return "Shelley"
	case 3:
		return "Allegra"
	case 4:
		return "Mary"
	case 5:
		return "Alonzo"
	case 6:
		return "Babbage"
	case 7:
		return "Conway"
	default:
		return fmt.Sprintf("Unknown(%d)", blockType)
	}
}

// ProcessMetadataFromCBOR processes metadata from raw CBOR bytes
func (mp *MetadataProcessor) ProcessMetadataFromCBOR(ctx context.Context, tx *gorm.DB, txHash []byte, cborBytes []byte) error {
	// This would decode CBOR and process metadata
	// Implementation depends on CBOR library usage
	return nil
}

// BatchProcessMetadata processes multiple metadata entries efficiently
func (mp *MetadataProcessor) BatchProcessMetadata(ctx context.Context, tx *gorm.DB, metadataBatch []MetadataBatch) error {
	if len(metadataBatch) == 0 {
		return nil
	}

	if !mp.quiet {
		log.Printf("Batch processing %d metadata entries", len(metadataBatch))
	}

	for _, item := range metadataBatch {
		if err := mp.processMetadataEntry(tx, item.TxHash, item.Key, item.Value); err != nil {
			mp.errorCollector.ProcessingWarning("MetadataProcessor", "batchProcessMetadata",
				fmt.Sprintf("failed to process metadata: %v", err),
				fmt.Sprintf("tx_hash:%x,key:%d", item.TxHash, item.Key))
			continue
		}
	}

	return nil
}

// MetadataBatch represents a batch of metadata to process
type MetadataBatch struct {
	TxHash []byte
	Key    uint64
	Value  interface{}
}

// GetMetadataStats returns statistics about metadata
func (mp *MetadataProcessor) GetMetadataStats(tx *gorm.DB) (*MetadataStats, error) {
	var stats MetadataStats

	// Total metadata entries
	if err := tx.Model(&models.TxMetadata{}).Count(&stats.TotalEntries).Error; err != nil {
		return nil, fmt.Errorf("failed to count metadata entries: %w", err)
	}

	// Unique transactions with metadata
	if err := tx.Model(&models.TxMetadata{}).
		Select("COUNT(DISTINCT tx_id)").
		Scan(&stats.UniqueTxs).Error; err != nil {
		return nil, fmt.Errorf("failed to count unique transactions: %w", err)
	}

	// Most common keys
	rows, err := tx.Model(&models.TxMetadata{}).
		Select("key, COUNT(*) as count").
		Group("key").
		Order("count DESC").
		Limit(10).
		Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to get common keys: %w", err)
	}
	defer rows.Close()

	stats.CommonKeys = make(map[uint64]int64)
	for rows.Next() {
		var key uint64
		var count int64
		if err := rows.Scan(&key, &count); err != nil {
			continue
		}
		stats.CommonKeys[key] = count
	}

	return &stats, nil
}

// MetadataStats contains metadata statistics
type MetadataStats struct {
	TotalEntries int64
	UniqueTxs    int64
	CommonKeys   map[uint64]int64
}

// ProcessMetadata processes metadata from a transaction
func (mp *MetadataProcessor) ProcessMetadata(tx *gorm.DB, txHash []byte, metadata *cbor.LazyValue) error {
	if metadata == nil {
		return nil
	}

	// Decode the CBOR lazy value to get the actual metadata
	decodedValue := metadata.Value()
	if decodedValue == nil {
		// Try to decode if Value() returns nil
		decoded, err := metadata.Decode()
		if err != nil {
			mp.errorCollector.ProcessingWarning("MetadataProcessor", "ProcessMetadata",
				fmt.Sprintf("failed to decode metadata: %v", err),
				fmt.Sprintf("tx_hash:%x", txHash))
			return nil
		}
		decodedValue = decoded
	}

	// Handle different metadata formats
	var metadataMap map[uint64]interface{}

	switch v := decodedValue.(type) {
	case map[uint64]interface{}:
		// Standard format for Shelley+ eras
		metadataMap = v

	case map[interface{}]interface{}:
		// Alternative format - convert keys to uint64
		metadataMap = make(map[uint64]interface{})
		for k, val := range v {
			switch key := k.(type) {
			case uint64:
				metadataMap[key] = val
			case int64:
				metadataMap[uint64(key)] = val
			case int:
				metadataMap[uint64(key)] = val
			case float64:
				metadataMap[uint64(key)] = val
			default:
				// For non-numeric keys, use hash of string representation
				keyStr := fmt.Sprintf("%v", key)
				// Use a simple hash for non-numeric keys
				var hash uint64
				for _, c := range keyStr {
					hash = hash*31 + uint64(c)
				}
				metadataMap[hash] = val
			}
		}

	case []interface{}:
		// Byron might store metadata as array - wrap in map
		metadataMap = map[uint64]interface{}{0: v}

	default:
		// Any other format - wrap in map with key 0
		if decodedValue != nil {
			metadataMap = map[uint64]interface{}{0: decodedValue}
		}
	}

	if len(metadataMap) == 0 {
		return nil
	}

	if !mp.quiet {
		log.Printf("Processing %d metadata entries for transaction %x", len(metadataMap), txHash)
	}

	for key, value := range metadataMap {
		if err := mp.processMetadataEntry(tx, txHash, key, value); err != nil {
			mp.errorCollector.ProcessingWarning("MetadataProcessor", "processMetadataEntry",
				fmt.Sprintf("failed to process metadata entry %d: %v", key, err),
				fmt.Sprintf("tx_hash:%x, key:%d", txHash, key))
		}
	}

	return nil
}
