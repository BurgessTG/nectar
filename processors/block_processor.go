package processors

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"log"
	"nectar/config"
	"nectar/constants"
	"nectar/database"
	unifiederrors "nectar/errors"
	"nectar/models"
	"strings"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Block type constants - These match the ledger era tags from gouroboros
const (
	BlockTypeByronEBB  = 0
	BlockTypeByronMain = 1
	BlockTypeShelley   = 2
	BlockTypeAllegra   = 3
	BlockTypeMary      = 4
	BlockTypeAlonzo    = 5
	BlockTypeBabbage   = 6
	BlockTypeConway    = 7
)

// StateQueryService interface for reward calculation
type StateQueryService interface {
	CalculateEpochRewards(epochNo uint32) error
	ProcessRefunds(epochNo uint32) error
}

// BlockProcessor handles processing blocks and storing them in TiDB
type BlockProcessor struct {
	db                        *gorm.DB
	config                    *config.IndexingConfig // Selective indexing configuration
	stakeAddressCache         *StakeAddressCache
	errorCollector            *ErrorCollector
	assetProcessor            *AssetProcessor
	metadataProcessor         *MetadataProcessor
	scriptProcessor           *ScriptProcessor
	walletConnectionProcessor *WalletConnectionProcessor
	epochParamsProvider       *EpochParamsProvider
	stateQueryService         StateQueryService
	txSemaphore               chan struct{} // Limits concurrent transaction processing
	currentEraConfig          *EraConfig    // Current era configuration
	currentEpoch              uint64        // Track current epoch for era detection
}

// NewBlockProcessor creates a new block processor
func NewBlockProcessor(db *gorm.DB, cfg *config.IndexingConfig) *BlockProcessor {
	stakeAddressCache := NewStakeAddressCache(db)
	bp := &BlockProcessor{
		db:                        db,
		config:                    cfg,
		stakeAddressCache:         stakeAddressCache,
		errorCollector:            GetGlobalErrorCollector(),
		assetProcessor:            NewAssetProcessor(db),
		metadataProcessor:         NewMetadataProcessor(db),
		scriptProcessor:           NewScriptProcessor(db),
		walletConnectionProcessor: NewWalletConnectionProcessor(db),
		epochParamsProvider:       NewEpochParamsProvider(db),
		txSemaphore:               make(chan struct{}, 32), // Increased to 32 for our powerful system
		currentEraConfig:          GetEraConfig(0),         // Start with Byron config
		currentEpoch:              0,
	}
	return bp
}

// ProcessBlock processes a single block and its transactions
func (bp *BlockProcessor) ProcessBlock(ctx context.Context, block ledger.Block, blockType uint) error {
	slotNumber := block.Header().SlotNumber()
	blockNumber := block.BlockNumber()
	epochNo := bp.getEpochForSlot(slotNumber, blockType)

	// Check if we need to update era configuration
	if uint64(epochNo) != bp.currentEpoch {
		bp.updateEraConfig(epochNo)
	}

	// Log block processing start
	if blockNumber%100 == 0 {
		log.Printf("[BLOCK] Processing block %d (slot %d, era %s)", blockNumber, slotNumber, bp.getEraName(blockType))
	}

	// Special handling for Byron-Shelley boundary
	if slotNumber >= 4492800 && slotNumber <= 4493000 {
		// Processing boundary block

		// Add extra debugging for the problematic slot
		if slotNumber == 4492900 {
			log.Printf("[DEBUG] Slot 4492900 - Block type: %d, Era: %s", blockType, bp.getEraName(blockType))
			log.Printf("[DEBUG] Block details: Number=%d, TxCount=%d", block.BlockNumber(), len(block.Transactions()))
			log.Printf("[DEBUG] Block hash: %x", block.Header().Hash())
			log.Printf("[DEBUG] Epoch calculated: %d", bp.getEpochForSlot(slotNumber, blockType))
			log.Printf("[DEBUG] Block time: %v", bp.getBlockTime(slotNumber, blockType))

			// Check first few transactions for any issues
			txs := block.Transactions()
			if len(txs) > 0 {
				log.Printf("[DEBUG] First transaction has %d inputs, %d outputs",
					len(txs[0].Inputs()), len(txs[0].Outputs()))
				if len(txs[0].Outputs()) > 0 {
					firstOutput := txs[0].Outputs()[0]
					log.Printf("[DEBUG] First output address type: %T", firstOutput.Address())
				}
			}
		}
	}

	// Ensure we start with a healthy connection (use context for timeout)
	if err := bp.EnsureHealthyConnectionWithContext(ctx); err != nil {
		return fmt.Errorf("connection unhealthy before processing: %w", err)
	}

	// Check context before starting work
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Process block header in its own transaction with retry logic
	// Note: Transaction isolation is set at session level in database/tidb.go
	// Don't pass context to transaction - let worker-level timeout handle cancellation
	var dbBlock *models.Block
	startTime := time.Now()
	err := database.RetryTransaction(bp.db, func(tx *gorm.DB) error {
		// Check context before processing (fast bail-out)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var err error
		dbBlock, err = bp.processBlockHeader(ctx, tx, block, blockType)
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to process block header: %w", err)
	}

	headerTime := time.Since(startTime)
	if headerTime > 5*time.Second {
		log.Printf("[PERF] Block header processing took %v for block %d", headerTime, blockNumber)
	}

	// Check context before processing transactions
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Process transactions with smaller scope (each tx in its own DB transaction)
	txStartTime := time.Now()
	if err := bp.processEraAwareTransactions(ctx, bp.db, dbBlock, block, blockType); err != nil {
		return fmt.Errorf("failed to process transactions: %w", err)
	}

	txTime := time.Since(txStartTime)
	totalTime := time.Since(startTime)
	if totalTime > 10*time.Second {
		log.Printf("[PERF] Block %d processing: header=%v, txs=%v, total=%v (tx count=%d)",
			blockNumber, headerTime, txTime, totalTime, len(block.Transactions()))
	}

	return nil
}

// processBlockHeader processes and stores the block header with hash-based primary key
func (bp *BlockProcessor) processBlockHeader(ctx context.Context, tx *gorm.DB, block ledger.Block, blockType uint) (*models.Block, error) {
	// Extract block data
	blockHeader := block.Header()
	hash := blockHeader.Hash()
	hashBytes := hash[:]

	slotNumber := blockHeader.SlotNumber()
	epochNo := bp.getEpochForSlot(slotNumber, blockType)
	blockNumber64 := block.BlockNumber()
	blockNumber := uint32(blockNumber64)

	// Extract slot leader hash directly without database insertion
	var slotLeaderHash []byte
	if blockType == BlockTypeByronEBB || blockType == BlockTypeByronMain {
		slotLeaderHash = hash[:28]
	} else {
		switch header := blockHeader.(type) {
		case *ledger.ShelleyBlockHeader:
			slotLeaderHash = header.Body.IssuerVkey[:28]
		case *ledger.BabbageBlockHeader:
			slotLeaderHash = header.Body.IssuerVkey[:28]
		default:
			slotLeaderHash = hash[:28]
		}
	}

	// Create the block record with hash as primary key
	dbBlock := &models.Block{
		Hash:           hashBytes,
		SlotNo:         &slotNumber,
		EpochNo:        &epochNo,
		BlockNo:        &blockNumber,
		SlotLeaderHash: slotLeaderHash,
		Size:           uint32(block.BlockBodySize()),
		Time:           bp.getBlockTime(slotNumber, blockType),
		TxCount:        uint64(len(block.Transactions())),
		ProtoMajor:     0, // NOTE: Set by setEraSpecificBlockFields() based on era
		ProtoMinor:     0, // NOTE: Set by setEraSpecificBlockFields() based on era
		Era:            bp.getEraName(blockType),
	}

	// Set previous block hash if not genesis
	if blockNumber64 > 0 {
		prevHash := block.Header().PrevHash()
		// Convert Blake2b256 to []byte
		if !isZeroHash(prevHash) {
			dbBlock.PreviousHash = prevHash[:]
		}
	}

	// Set era-specific fields
	bp.setEraSpecificBlockFields(dbBlock, block, blockType)

	// Insert block with fast duplicate handling
	if err := database.FastCreate(tx, dbBlock); err != nil {
		// If it failed for a reason other than duplicate, check if block exists
		var existingBlock models.Block
		if err2 := tx.Where("hash = ?", hashBytes).First(&existingBlock).Error; err2 == nil {
			// Block already exists, use it
			return &existingBlock, nil
		}
		return nil, fmt.Errorf("failed to create block: %w", err)
	}

	// Check for epoch boundary
	if bp.isEpochBoundary(slotNumber, blockType) {
		if err := bp.processEpochBoundary(ctx, tx, dbBlock, epochNo, blockType); err != nil {
			unifiederrors.Get().Warning("BlockProcessor", "ProcessEpochBoundary", fmt.Sprintf("Failed to process epoch boundary at slot %d: %v", slotNumber, err))
		}
	}

	return dbBlock, nil
}

// processEraAwareTransactions processes transactions with era-specific handling
func (bp *BlockProcessor) processEraAwareTransactions(ctx context.Context, db *gorm.DB, dbBlock *models.Block, block ledger.Block, blockType uint) error {
	transactions := block.Transactions()
	if len(transactions) == 0 {
		return nil
	}

	// Get block data for transactions
	blockHash := dbBlock.Hash
	blockNumber := block.BlockNumber()
	blockTime := dbBlock.Time

	// Process transactions sequentially to avoid database connection issues
	// Each transaction still gets its own DB transaction for atomicity
	for idx, tx := range transactions {
		// Special debugging for Byron-Shelley boundary transactions
		// Skip verbose logging for boundary blocks

		// Log slow transactions
		txStartTime := time.Now()

		// Use RetryTransaction for automatic retry with exponential backoff
		// Note: Transaction isolation is set at session level in database/tidb.go
		slotNo := *dbBlock.SlotNo
		err := database.RetryTransaction(db, func(dbTx *gorm.DB) error {
			return bp.processTransaction(ctx, dbTx, blockHash, blockNumber, blockTime, slotNo, idx, tx, blockType)
		})

		txProcessTime := time.Since(txStartTime)
		if txProcessTime > 5*time.Second {
			log.Printf("[PERF] Transaction %d in block %d took %v to process", idx, blockNumber, txProcessTime)
		}

		if err != nil {
			// Check if it's a duplicate key error (already processed)
			if strings.Contains(err.Error(), "Duplicate entry") {
				// Don't log this - it's normal during parallel processing
				continue // Not an error, continue with next transaction
			}

			// For Byron-Shelley boundary, add more context to errors
			if blockNumber >= 4492800 && blockNumber <= 4493000 {
				return fmt.Errorf("transaction %d at Byron-Shelley boundary (block %d): %w",
					idx, blockNumber, err)
			}

			return fmt.Errorf("transaction %d: %w", idx, err)
		}
	}

	return nil
}

// processTransaction processes a single transaction in its own DB transaction
func (bp *BlockProcessor) processTransaction(ctx context.Context, tx *gorm.DB, blockHash []byte, blockNumber uint64, blockTime time.Time, slotNo uint64, blockIndex int, transaction ledger.Transaction, blockType uint) error {
	hash := transaction.Hash()
	txHash := hash[:]

	// Create transaction record
	dbTx := models.Tx{
		Hash:       txHash,
		BlockHash:  blockHash,
		BlockIndex: uint32(blockIndex),
		Size:       uint32(len(transaction.Cbor())),
	}

	// Set basic transaction fields
	if fee := transaction.Fee(); fee > 0 {
		fee64 := int64(fee)
		dbTx.Fee = &fee64
	}

	// Calculate output sum
	if outputs := transaction.Outputs(); len(outputs) > 0 {
		var outSum int64
		for _, output := range outputs {
			outSum += int64(output.Amount())
		}
		dbTx.OutSum = &outSum
	}

	// Era-specific transaction processing
	bp.setEraSpecificTxFields(&dbTx, transaction, blockType)

	// Insert transaction with fast duplicate handling
	if err := database.FastCreate(tx, &dbTx); err != nil {
		return fmt.Errorf("failed to insert transaction: %w", err)
	}

	// Process transaction components sequentially to avoid "commands out of sync" errors
	// IMPORTANT: We cannot use concurrent goroutines on the same database transaction

	// Process inputs (CONDITIONAL - only if UTxOs/Inputs enabled)
	if bp.config.Inputs || bp.config.UTXOs {
		if err := bp.processTransactionInputs(ctx, tx, txHash, transaction, blockType); err != nil {
			unifiederrors.Get().Warning("BlockProcessor", "ProcessInputs", fmt.Sprintf("Failed to process inputs for tx %d: %v", blockIndex, err))
		}
	}

	// Process outputs (CONDITIONAL - only if UTxOs/Outputs enabled)
	if bp.config.Outputs || bp.config.UTXOs {
		if err := bp.processTransactionOutputs(ctx, tx, txHash, transaction, blockType); err != nil {
			if strings.Contains(err.Error(), "Data too long") || strings.Contains(err.Error(), "failed to batch insert") {
				unifiederrors.Get().DatabaseError("BlockProcessor", "ProcessOutputs", fmt.Errorf("Component error in tx %d: outputs: %v", blockIndex, err))
			} else {
				unifiederrors.Get().Warning("BlockProcessor", "ProcessOutputs", fmt.Sprintf("Failed to process outputs for tx %d: %v", blockIndex, err))
			}
		}
	}

	// Process metadata (CONDITIONAL - with label filtering)
	if bp.config.Metadata {
		if metadata := transaction.Metadata(); metadata != nil {
			if metaValue := metadata.Value(); metaValue != nil {
				if err := bp.processFilteredMetadata(ctx, tx, txHash, metadata); err != nil {
					unifiederrors.Get().Warning("BlockProcessor", "ProcessMetadata", fmt.Sprintf("Failed to process metadata: %v", err))
				}
			}
		}
	}

	// Process minting/burning (CONDITIONAL)
	if bp.config.Minting || bp.config.Assets {
		if mint := bp.getTransactionMint(transaction, blockType); len(mint) > 0 {
			if err := bp.assetProcessor.ProcessMint(tx, txHash, mint); err != nil {
				unifiederrors.Get().Warning("BlockProcessor", "ProcessMint", fmt.Sprintf("Failed to process mint: %v", err))
			}
		}
	}

	// Process scripts (CONDITIONAL)
	if bp.config.Scripts {
		if blockType >= BlockTypeAlonzo {
			// Log every 100th transaction in smart contract eras to verify we're processing them
			if blockIndex%100 == 0 {
				log.Printf("[SCRIPT_CHECK] Processing tx %x at index %d in era %d", txHash, blockIndex, blockType)
			}

			// For the first few transactions in Babbage, log detailed info
			if blockType == BlockTypeBabbage && blockIndex < 5 {
				log.Printf("[BABBAGE_TX] Processing transaction %d: hash=%x, type=%T", blockIndex, txHash, transaction)
			}
		}
		if err := bp.scriptProcessor.ProcessTransaction(tx, txHash, transaction, blockType); err != nil {
			unifiederrors.Get().Warning("BlockProcessor", "ProcessScripts", fmt.Sprintf("Failed to process scripts: %v", err))
		}
	}

	// Process collateral inputs (CONDITIONAL - Alonzo+)
	if bp.config.Collateral && blockType >= BlockTypeAlonzo {
		if err := bp.processCollateralInputs(ctx, tx, txHash, transaction, blockType); err != nil {
			unifiederrors.Get().Warning("BlockProcessor", "ProcessCollateral", fmt.Sprintf("Failed to process collateral inputs: %v", err))
		}
	}

	// Process collateral return (CONDITIONAL - Babbage+)
	if bp.config.Collateral && blockType >= BlockTypeBabbage {
		if err := bp.processCollateralReturn(ctx, tx, txHash, transaction, blockType); err != nil {
			unifiederrors.Get().Warning("BlockProcessor", "ProcessCollateralReturn", fmt.Sprintf("Failed to process collateral return: %v", err))
		}
	}

	// Process reference inputs (CONDITIONAL - Babbage+)
	if bp.config.Collateral && blockType >= BlockTypeBabbage {
		if err := bp.processReferenceInputs(ctx, tx, txHash, transaction, blockType); err != nil {
			unifiederrors.Get().Warning("BlockProcessor", "ProcessReferenceInputs", fmt.Sprintf("Failed to process reference inputs: %v", err))
		}
	}

	// Process required signers (CONDITIONAL - Alonzo+)
	if bp.config.Scripts && blockType >= BlockTypeAlonzo {
		if err := bp.processRequiredSigners(ctx, tx, txHash, transaction, blockType); err != nil {
			unifiederrors.Get().Warning("BlockProcessor", "ProcessRequiredSigners", fmt.Sprintf("Failed to process required signers: %v", err))
		}
	}

	// Process wallet connections - track sender→receiver relationships
	// This runs after inputs and outputs are processed so we can look up spent UTxO addresses
	if bp.walletConnectionProcessor != nil {
		if err := bp.walletConnectionProcessor.ProcessTransaction(tx, txHash, slotNo, transaction); err != nil {
			unifiederrors.Get().Warning("BlockProcessor", "ProcessWalletConnections", fmt.Sprintf("Failed to process wallet connections: %v", err))
		}
	}

	return nil
}

// processFilteredMetadata processes only allowed metadata labels
func (bp *BlockProcessor) processFilteredMetadata(ctx context.Context, tx *gorm.DB, txHash []byte, metadata *cbor.LazyValue) error {
	// If no filter specified, process all metadata
	if len(bp.config.MetadataLabels) == 0 {
		return bp.metadataProcessor.ProcessMetadata(tx, txHash, metadata)
	}

	// Create allowlist map for fast lookup
	allowedLabels := make(map[uint64]bool)
	for _, label := range bp.config.MetadataLabels {
		allowedLabels[label] = true
	}

	// Decode metadata
	decodedValue := metadata.Value()
	if decodedValue == nil {
		decoded, err := metadata.Decode()
		if err != nil {
			return err
		}
		decodedValue = decoded
	}

	// Handle different metadata formats
	var metadataMap map[uint64]interface{}
	switch v := decodedValue.(type) {
	case map[uint64]interface{}:
		metadataMap = v
	case map[interface{}]interface{}:
		// Convert to map[uint64]interface{}
		metadataMap = make(map[uint64]interface{})
		for k, val := range v {
			switch key := k.(type) {
			case uint64:
				metadataMap[key] = val
			case int64:
				metadataMap[uint64(key)] = val
			case int:
				metadataMap[uint64(key)] = val
			}
		}
	default:
		// Can't filter non-map metadata, process as-is
		return bp.metadataProcessor.ProcessMetadata(tx, txHash, metadata)
	}

	// Process only allowed labels
	for key, value := range metadataMap {
		if !allowedLabels[key] {
			// Skip this label - not in allowlist
			continue
		}

		// Process this metadata entry
		if err := bp.metadataProcessor.processMetadataEntry(tx, txHash, key, value); err != nil {
			return fmt.Errorf("failed to process metadata label %d: %w", key, err)
		}
	}

	return nil
}

// processTransactionBatch processes a batch of transactions with hash-based keys
func (bp *BlockProcessor) processTransactionBatch(ctx context.Context, tx *gorm.DB, blockHash []byte, transactions []ledger.Transaction, startIndex int, blockType uint) error {
	txBatch := make([]models.Tx, 0, len(transactions))

	// First pass: create transaction records
	for i, transaction := range transactions {
		blockIndex := startIndex + i
		hash := transaction.Hash()
		hashBytes := hash[:]

		dbTx := models.Tx{
			Hash:       hashBytes,
			BlockHash:  blockHash,
			BlockIndex: uint32(blockIndex),
			Size:       uint32(len(transaction.Cbor())),
		}

		// Set basic transaction fields
		if fee := transaction.Fee(); fee > 0 {
			fee64 := int64(fee)
			dbTx.Fee = &fee64
		}

		// Calculate output sum
		if outputs := transaction.Outputs(); len(outputs) > 0 {
			var outSum int64
			for _, output := range outputs {
				outSum += int64(output.Amount())
			}
			dbTx.OutSum = &outSum
		}

		// Era-specific transaction processing
		bp.setEraSpecificTxFields(&dbTx, transaction, blockType)

		txBatch = append(txBatch, dbTx)
	}

	// Batch insert transactions with fast duplicate handling
	if err := database.FastCreateInBatches(tx, txBatch, bp.currentEraConfig.TxBatchSize); err != nil {
		return fmt.Errorf("failed to batch insert transactions: %w", err)
	}

	// Second pass: process transaction components
	for i, transaction := range transactions {
		hash := transaction.Hash()
		txHash := hash[:]

		// Process transaction inputs
		if err := bp.processTransactionInputs(ctx, tx, txHash, transaction, blockType); err != nil {
			unifiederrors.Get().Warning("BlockProcessor", "ProcessInputs", fmt.Sprintf("Failed to process inputs for tx %d: %v", i, err))
		}

		// Process transaction outputs
		if err := bp.processTransactionOutputs(ctx, tx, txHash, transaction, blockType); err != nil {
			unifiederrors.Get().Warning("BlockProcessor", "ProcessOutputs", fmt.Sprintf("Failed to process outputs for tx %d: %v", i, err))
		}

		// Process metadata
		if metadata := transaction.Metadata(); metadata != nil {
			// Check if metadata has any content
			if metaValue := metadata.Value(); metaValue != nil {
				if err := bp.metadataProcessor.ProcessMetadata(tx, txHash, metadata); err != nil {
					unifiederrors.Get().Warning("BlockProcessor", "ProcessMetadata", fmt.Sprintf("Failed to process metadata: %v", err))
				}
			}
		}

		// Process minting/burning
		if mint := bp.getTransactionMint(transaction, blockType); len(mint) > 0 {
			if err := bp.assetProcessor.ProcessMint(tx, txHash, mint); err != nil {
				unifiederrors.Get().Warning("BlockProcessor", "ProcessMint", fmt.Sprintf("Failed to process mint: %v", err))
			}
		}

		// Process scripts
		if err := bp.scriptProcessor.ProcessTransaction(tx, txHash, transaction, blockType); err != nil {
			unifiederrors.Get().Warning("BlockProcessor", "ProcessScripts", fmt.Sprintf("Failed to process scripts: %v", err))
		}
	}

	return nil
}

// processTransactionInputs processes transaction inputs with hash references
func (bp *BlockProcessor) processTransactionInputs(ctx context.Context, tx *gorm.DB, txHash []byte, transaction ledger.Transaction, blockType uint) error {
	inputs := transaction.Inputs()
	if len(inputs) == 0 {
		return nil
	}

	inputBatch := make([]models.TxIn, 0, len(inputs))

	for i, input := range inputs {
		// Get the referenced transaction output
		id := input.Id()
		outTxHash := id[:]
		outIndex := input.Index()

		dbInput := models.TxIn{
			TxInHash:   txHash,
			TxInIndex:  uint32(i),
			TxOutHash:  outTxHash,
			TxOutIndex: outIndex,
		}

		// Handle redeemer if present
		if redeemer := bp.getInputRedeemer(transaction, i, blockType); redeemer != nil {
			redeemerHash := models.GenerateRedeemerHash(txHash, "spend", uint32(i))
			dbInput.RedeemerHash = redeemerHash
		}

		inputBatch = append(inputBatch, dbInput)
	}

	// Batch insert inputs with fast duplicate handling
	// Use retry operation for transient errors
	err := database.RetryOperation(func() error {
		return database.FastCreateInBatches(tx, inputBatch, bp.currentEraConfig.TxInBatchSize)
	})

	if err != nil {
		// If it's a duplicate key error, log and continue
		if strings.Contains(err.Error(), "Duplicate entry") {
			log.Printf("[INFO] Some inputs already exist for tx %x, skipping duplicates", txHash)
			return nil
		}
		return fmt.Errorf("failed to batch insert inputs: %w", err)
	}

	return nil
}

// processTransactionOutputs processes transaction outputs with composite keys
func (bp *BlockProcessor) processTransactionOutputs(ctx context.Context, tx *gorm.DB, txHash []byte, transaction ledger.Transaction, blockType uint) error {
	outputs := transaction.Outputs()
	if len(outputs) == 0 {
		return nil
	}

	outputBatch := make([]models.TxOut, 0, len(outputs))

	for i, output := range outputs {
		address := output.Address()

		// Safely get address string - handle potential panics from gouroboros
		var addressStr string
		func() {
			defer func() {
				if r := recover(); r != nil {
					// For problematic addresses, use base58 encoding of raw bytes as fallback
					if addrBytes, err := address.Bytes(); err == nil {
						addressStr = fmt.Sprintf("byron_raw_%x", addrBytes)
						unifiederrors.Get().Warning("BlockProcessor", "AddressFormat",
							fmt.Sprintf("Address.String() panicked, using raw format: %v", r))
					} else {
						addressStr = fmt.Sprintf("invalid_address_%d", i)
						unifiederrors.Get().LogError(unifiederrors.ErrorTypeProcessing, "BlockProcessor", "GetAddress",
							fmt.Sprintf("Failed to get address string and bytes: %v", r))
					}
				}
			}()
			addressStr = address.String()
		}()

		// Handle extremely long Byron addresses that exceed database column limit
		// Some Byron addresses with derivation paths can be extremely long when converted to string
		const maxAddressLength = 2048
		if len(addressStr) > maxAddressLength {
			// For addresses that are too long, use a hash-based representation
			// This preserves uniqueness while fitting in the database
			addressBytes, err := address.Bytes()
			if err == nil {
				// Use hex encoding of the raw bytes with a prefix to indicate truncation
				addressStr = fmt.Sprintf("byron_long_%x", addressBytes)
				if len(addressStr) > maxAddressLength {
					// If even the hex is too long, use a hash
					hash := sha256.Sum256(addressBytes)
					addressStr = fmt.Sprintf("byron_hash_%x", hash)
				}
			} else {
				// Fallback: use index-based identifier
				addressStr = fmt.Sprintf("byron_toolong_%d_%d", len(addressStr), i)
			}
			unifiederrors.Get().Warning("BlockProcessor", "AddressLength",
				fmt.Sprintf("Byron address too long (%d chars), using alternative representation: %s", len(address.String()), addressStr))
		}

		addressBytes, err := address.Bytes()
		if err != nil {
			// Use empty bytes on error
			addressBytes = []byte{}
		}

		dbOutput := models.TxOut{
			TxHash:     txHash,
			Index:      uint32(i),
			Address:    addressStr,
			AddressRaw: addressBytes,
			Value:      output.Amount(),
		}

		// Extract payment credential
		if paymentCred := bp.extractPaymentCredential(address); paymentCred != nil {
			dbOutput.PaymentCred = paymentCred
		}

		// Extract stake address hash from base addresses
		if bp.extractStakeAddress(address) {
			// Get address bytes
			addrBytes, err := address.Bytes()
			if err == nil && len(addrBytes) >= 57 {
				// For base addresses, bytes 29-56 contain the stake credential hash
				stakeCredHash := addrBytes[29:57]
				dbOutput.StakeAddressHash = stakeCredHash

				// Also ensure the stake address is registered in the stake_addresses table
				// Create a reward address for this stake credential
				_, err := bp.stakeAddressCache.GetOrCreateStakeAddressFromBytesWithTx(tx, stakeCredHash)
				if err != nil {
					unifiederrors.Get().Warning("BlockProcessor", "StakeAddressRegistration", fmt.Sprintf("Failed to register stake address: %v", err))
				}
			}
		}

		// Handle datum hash
		if datumHash := output.DatumHash(); datumHash != nil {
			dbOutput.DataHash = datumHash.Bytes()
		}

		// Handle inline datum (Babbage+)
		if inlineDatum := bp.getOutputInlineDatum(output, blockType); inlineDatum != nil {
			dbOutput.InlineDatumHash = inlineDatum
		}

		// Handle reference script (Babbage+)
		if refScript := bp.getOutputReferenceScript(output, blockType); refScript != nil {
			dbOutput.ReferenceScriptHash = refScript
		}

		outputBatch = append(outputBatch, dbOutput)
	}

	// Batch insert outputs
	// Use larger batch size if we have a lot of outputs
	batchSize := bp.currentEraConfig.TxOutBatchSize
	if len(outputBatch) > bp.currentEraConfig.TxOutBatchSize*2 {
		batchSize = bp.currentEraConfig.TxOutBatchSize * 2 // Double batch size for large sets
	}

	// Use retry operation for transient errors
	err := database.RetryOperation(func() error {
		return database.FastCreateInBatches(tx, outputBatch, batchSize)
	})

	if err != nil {
		// Debug: Log the addresses that are failing
		for _, out := range outputBatch {
			if len(out.Address) > 2048 {
				unifiederrors.Get().LogError(unifiederrors.ErrorTypeValidation, "BlockProcessor", "AddressLength",
					fmt.Sprintf("Address too long (%d chars): %s", len(out.Address), out.Address))
			}
		}
		return fmt.Errorf("failed to batch insert outputs: %w", err)
	}

	// Process multi-assets in outputs
	for i, output := range outputs {
		if assets := output.Assets(); assets != nil {
			if err := bp.assetProcessor.ProcessOutputAssets(tx, txHash, uint32(i), assets); err != nil {
				return fmt.Errorf("failed to process output %d assets: %w", i, err)
			}
		}
	}

	return nil
}

// Helper functions remain largely the same but work with hashes instead of IDs
func (bp *BlockProcessor) getEpochForSlot(slot uint64, blockType uint) uint32 {
	// Special handling for Byron-Shelley boundary
	if slot == constants.ByronEraEndSlot+1 {
		return 208
	}

	switch blockType {
	case BlockTypeByronEBB, BlockTypeByronMain:
		return uint32(slot / 21600) // Byron epoch = 21600 slots
	case BlockTypeShelley, BlockTypeAllegra, BlockTypeMary, BlockTypeAlonzo, BlockTypeBabbage, BlockTypeConway:
		// For Shelley+, we need to account for the Byron slots
		// Epoch 208 starts at slot 4492800
		if slot < 4492800 {
			// This shouldn't happen for Shelley blocks
			log.Printf("[WARNING] Shelley block with Byron slot %d, calculating Byron epoch", slot)
			return uint32(slot / 21600)
		}
		// Calculate Shelley epoch
		// Slot 4492800 = start of epoch 208
		// Each Shelley epoch is 432000 slots
		return 208 + uint32((slot-4492800)/432000)
	default:
		// Default handling
		if slot < 4492800 {
			return uint32(slot / 21600) // Byron epoch
		}
		return 208 + uint32((slot-4492800)/432000) // Shelley+ epoch
	}
}

func (bp *BlockProcessor) getBlockTime(slot uint64, blockType uint) time.Time {
	// Shelley start time: July 29, 2020 21:44:51 UTC
	shelleyStart := time.Date(2020, 7, 29, 21, 44, 51, 0, time.UTC)

	// Special handling for Byron-Shelley boundary
	// The transition happens at slot 4492800
	if slot == 4492800 {
		return shelleyStart
	}

	switch blockType {
	case BlockTypeByronEBB, BlockTypeByronMain:
		// Byron era: 20 second slots, started at mainnet launch
		byronStart := time.Date(2017, 9, 23, 21, 44, 51, 0, time.UTC)
		return byronStart.Add(time.Duration(slot) * 20 * time.Second)
	case BlockTypeShelley, BlockTypeAllegra, BlockTypeMary, BlockTypeAlonzo, BlockTypeBabbage, BlockTypeConway:
		// Shelley+ era: 1 second slots
		// Adjust for Byron slots (4492800 Byron slots at 20s each)
		if slot < 4492800 {
			// This shouldn't happen for Shelley blocks, but handle it gracefully
			log.Printf("[WARNING] Shelley block with Byron slot number: %d", slot)
			byronStart := time.Date(2017, 9, 23, 21, 44, 51, 0, time.UTC)
			return byronStart.Add(time.Duration(slot) * 20 * time.Second)
		}
		shelleySlot := slot - 4492800
		return shelleyStart.Add(time.Duration(shelleySlot) * time.Second)
	default:
		// Default to Shelley+ era timing
		if slot < 4492800 {
			// Byron slot
			byronStart := time.Date(2017, 9, 23, 21, 44, 51, 0, time.UTC)
			return byronStart.Add(time.Duration(slot) * 20 * time.Second)
		}
		shelleySlot := slot - 4492800
		return shelleyStart.Add(time.Duration(shelleySlot) * time.Second)
	}
}

func (bp *BlockProcessor) getEraName(blockType uint) string {
	// The blockType parameter from gouroboros represents the ledger era tag
	// This mapping has been verified and corrected for all eras.

	// DEBUG: Uncomment to verify blockType values during development:
	// log.Printf("[DEBUG] getEraName called with blockType: %d", blockType)

	switch blockType {
	case BlockTypeByronEBB, BlockTypeByronMain:
		return "Byron"
	case BlockTypeShelley:
		return "Shelley"
	case BlockTypeAllegra: // FIXED: Allegra is blockType 3, not 4
		return "Allegra"
	case BlockTypeMary:
		return "Mary"
	case BlockTypeAlonzo:
		return "Alonzo"
	case BlockTypeBabbage:
		return "Babbage"
	case BlockTypeConway:
		return "Conway"
	default:
		// Log unknown blockType for debugging
		unifiederrors.Get().Warning("BlockProcessor", "UnknownBlockType", fmt.Sprintf("Unknown blockType: %d", blockType))
		return "Unknown"
	}
}

func (bp *BlockProcessor) isEpochBoundary(slot uint64, blockType uint) bool {
	switch blockType {
	case BlockTypeByronEBB:
		return true // EBB blocks are by definition epoch boundaries
	case BlockTypeByronMain:
		return slot%21600 == 0
	case BlockTypeShelley, BlockTypeAllegra, BlockTypeMary, BlockTypeAlonzo, BlockTypeBabbage, BlockTypeConway:
		return slot%432000 == 0
	default:
		return slot%432000 == 0
	}
}

// Stub functions for era-specific processing
func (bp *BlockProcessor) setEraSpecificBlockFields(block *models.Block, ledgerBlock ledger.Block, blockType uint) {
	header := ledgerBlock.Header()

	// Set protocol version based on era
	// IMPORTANT: These are approximate protocol versions based on era boundaries.
	// Actual protocol versions are determined by on-chain governance votes and
	// protocol parameter updates which can happen mid-era. For exact protocol
	// versions, one would need to track protocol parameter update proposals
	// and their activation points. This approximation is sufficient for most
	// indexing purposes but should not be used for protocol-critical decisions.
	switch blockType {
	case BlockTypeByronEBB, BlockTypeByronMain:
		// Byron uses protocol version 0.0 or 1.0
		block.ProtoMajor = 1
		block.ProtoMinor = 0
	case BlockTypeShelley:
		// Shelley: protocol version 2.0
		block.ProtoMajor = 2
		block.ProtoMinor = 0
	case BlockTypeAllegra:
		// Allegra: protocol version 3.0
		block.ProtoMajor = 3
		block.ProtoMinor = 0
	case BlockTypeMary:
		// Mary: protocol version 4.0
		block.ProtoMajor = 4
		block.ProtoMinor = 0
	case BlockTypeAlonzo:
		// Alonzo: protocol version 5.0 or 6.0
		block.ProtoMajor = 6
		block.ProtoMinor = 0
	case BlockTypeBabbage:
		// Babbage: protocol version 7.0 or 8.0
		block.ProtoMajor = 8
		block.ProtoMinor = 0
	case BlockTypeConway:
		// Conway: protocol version 9.0+
		block.ProtoMajor = 9
		block.ProtoMinor = 0
	}

	// Extract VRF key for Shelley+ blocks
	if blockType >= BlockTypeShelley {
		switch h := header.(type) {
		case *ledger.ShelleyBlockHeader:
			if h.Body.VrfKey != nil {
				block.VrfKey = stringPtr(fmt.Sprintf("%x", h.Body.VrfKey))
			}
		case *ledger.BabbageBlockHeader:
			if h.Body.VrfKey != nil {
				block.VrfKey = stringPtr(fmt.Sprintf("%x", h.Body.VrfKey))
			}
		}
	}
}

func (bp *BlockProcessor) setEraSpecificTxFields(tx *models.Tx, ledgerTx ledger.Transaction, blockType uint) {
	// Era-specific transaction fields
	switch blockType {
	case BlockTypeAllegra, BlockTypeMary, BlockTypeAlonzo, BlockTypeBabbage, BlockTypeConway:
		// Validity interval (introduced in Allegra)
		validity := ledgerTx.ValidityIntervalStart()
		if validity > 0 {
			validityInt := int64(validity)
			tx.InvalidBefore = &validityInt
		}

		// TTL (InvalidHereafter)
		if ttl := ledgerTx.TTL(); ttl > 0 {
			ttlInt := int64(ttl)
			tx.InvalidHereafter = &ttlInt
		}
	}

	// Alonzo+ specific fields
	if blockType >= BlockTypeAlonzo {
		// Script validity
		if ledgerTx.IsValid() {
			valid := true
			tx.ValidContract = &valid
		} else {
			valid := false
			tx.ValidContract = &valid
		}

		// Note: Collateral inputs are handled separately in the transaction processing
		// Reference inputs would need to be tracked in a separate table

		// Total collateral amount
		if totalCollateral := ledgerTx.TotalCollateral(); totalCollateral > 0 {
			totalCollateralInt := int64(totalCollateral)
			tx.TotalCollateral = &totalCollateralInt
		}
	}

	// Set deposit amount if applicable (handled in base transaction processing)

	// Treasury donation for Conway era
	if blockType >= BlockTypeConway {
		if donation := ledgerTx.Donation(); donation > 0 {
			donationInt := int64(donation)
			tx.TreasuryDonation = &donationInt
		}
	}
}

func (bp *BlockProcessor) processEpochBoundary(ctx context.Context, tx *gorm.DB, block *models.Block, epochNo uint32, blockType uint) error {
	// Only process epoch boundaries for Shelley+ eras
	if blockType < BlockTypeShelley {
		return nil
	}

	// Create epoch record
	epoch := &models.Epoch{
		No:        epochNo,
		StartTime: block.Time,
		EndTime:   block.Time.Add(5 * 24 * time.Hour), // Approximate 5-day epoch
	}

	// Insert or update epoch
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "no"}},
		UpdateAll: true,
	}).Create(epoch).Error; err != nil {
		return fmt.Errorf("failed to create epoch: %w", err)
	}

	// Log epoch transition
	log.Printf("[INFO] Epoch boundary: entering epoch %d at slot %d", epochNo, *block.SlotNo)

	// Provide epoch parameters
	if bp.epochParamsProvider != nil {
		if err := bp.epochParamsProvider.ProvideEpochParams(epochNo); err != nil {
			log.Printf("[WARNING] Failed to provide epoch params for epoch %d: %v", epochNo, err)
		}
	}

	// Calculate rewards for the previous epoch (rewards are calculated 2 epochs later)
	if bp.stateQueryService != nil && epochNo >= 213 { // Rewards start being paid in epoch 213
		// Calculate rewards for epoch N-2
		rewardEpoch := epochNo - 2
		log.Printf("[INFO] Calculating rewards for epoch %d (current epoch: %d)", rewardEpoch, epochNo)

		if err := bp.stateQueryService.CalculateEpochRewards(epochNo); err != nil {
			log.Printf("[WARNING] Failed to calculate rewards for epoch %d: %v", rewardEpoch, err)
		}

		// Process refunds for the current epoch
		if err := bp.stateQueryService.ProcessRefunds(epochNo); err != nil {
			log.Printf("[WARNING] Failed to process refunds for epoch %d: %v", epochNo, err)
		}
	}

	// Note: Full epoch boundary processing would also include:
	// - Snapshot stake distribution
	// - Update pool parameters
	// - Process protocol parameter updates
	// These require additional state query integration

	return nil
}

func (bp *BlockProcessor) getTransactionCertificates(tx ledger.Transaction, blockType uint) []interface{} {
	// Certificates are only available from Shelley onwards
	if blockType < BlockTypeShelley {
		return nil
	}

	// Get certificates from transaction
	certs := tx.Certificates()
	if len(certs) == 0 {
		return nil
	}

	// Return certificates as interface slice for the certificate processor
	result := make([]interface{}, len(certs))
	for i, cert := range certs {
		result[i] = cert
	}

	return result
}

func (bp *BlockProcessor) getTransactionWithdrawals(tx ledger.Transaction, blockType uint) map[string]uint64 {
	// Withdrawals are only available from Shelley onwards
	if blockType < BlockTypeShelley {
		return nil
	}

	// Get withdrawals from transaction
	withdrawals := tx.Withdrawals()
	if len(withdrawals) == 0 {
		return nil
	}

	// Convert to string map for the withdrawal processor
	result := make(map[string]uint64)
	for addr, amount := range withdrawals {
		// Convert address to string (bech32 format)
		addrStr := addr.String()
		result[addrStr] = amount
	}

	return result
}

func (bp *BlockProcessor) getTransactionMint(tx ledger.Transaction, blockType uint) map[string]int64 {
	// Multi-assets minting is only available from Mary era onwards
	if blockType < BlockTypeMary {
		return nil
	}

	// Get mint data from transaction
	mintAssets := tx.AssetMint()
	if mintAssets == nil {
		return nil
	}

	// Convert to our expected format
	result := make(map[string]int64)

	// Iterate through all policies
	for _, policyID := range mintAssets.Policies() {
		policyIDHex := fmt.Sprintf("%x", policyID[:])

		// Get all assets for this policy
		assetNames := mintAssets.Assets(policyID)
		for _, assetName := range assetNames {
			assetNameHex := fmt.Sprintf("%x", assetName)

			// Get the mint amount (positive for minting, negative for burning)
			amount := mintAssets.Asset(policyID, assetName)

			// Create key in format "policyId.assetName"
			key := fmt.Sprintf("%s.%s", policyIDHex, assetNameHex)

			// Store the actual amount as-is (positive for mint, negative for burn)
			result[key] = amount
		}
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

func (bp *BlockProcessor) hasGovernanceData(tx ledger.Transaction, blockType uint) bool {
	// Governance data is only available from Conway era onwards
	if blockType < BlockTypeConway {
		return false
	}

	// Check if transaction has voting procedures
	if tx.VotingProcedures() != nil {
		return true
	}

	// Check if transaction has proposal procedures
	if proposals := tx.ProposalProcedures(); len(proposals) > 0 {
		return true
	}

	// Check if transaction has Conway-specific certificates
	certs := tx.Certificates()
	for _, cert := range certs {
		certType := cert.Type()
		// Conway certificate types: 8-17
		if certType >= 8 && certType <= 17 {
			return true
		}
	}

	return false
}

func (bp *BlockProcessor) getInputRedeemer(tx ledger.Transaction, index int, blockType uint) interface{} {
	// Redeemers are only available from Alonzo era onwards
	if blockType < BlockTypeAlonzo {
		return nil
	}

	// Get witnesses which contain redeemers
	witnesses := tx.Witnesses()
	if witnesses == nil {
		return nil
	}

	// Get redeemers from witnesses
	// Note: The exact method to access redeemers depends on the transaction type
	// For now, return nil as the interface may vary between transaction versions

	// GOUROBOROS LIMITATION: Redeemer access not yet exposed
	// The transaction witnesses contain redeemers, but gouroboros
	// doesn't currently expose methods to access them directly.
	// When available, we need to:
	// - Get redeemers array from witnesses
	// - Find redeemer with Purpose=Spend and Index=index
	// - Return the redeemer object for storage

	return nil
}

func (bp *BlockProcessor) getOutputInlineDatum(output ledger.TransactionOutput, blockType uint) []byte {
	// Inline datums are only available from Babbage era onwards
	if blockType < BlockTypeBabbage {
		return nil
	}

	// Get inline datum from output
	datum := output.Datum()
	if datum == nil {
		return nil
	}

	// Calculate hash of the datum for storage
	datumBytes := datum.Cbor()
	if len(datumBytes) == 0 {
		return nil
	}

	// Return the hash of the datum
	hash := ledger.NewBlake2b256(datumBytes)
	return hash[:]
}

func (bp *BlockProcessor) getOutputReferenceScript(output ledger.TransactionOutput, blockType uint) []byte {
	// Reference scripts are only available from Babbage era onwards
	if blockType < BlockTypeBabbage {
		return nil
	}

	// Try to extract reference script using type assertion to Babbage output
	switch o := output.(type) {
	case interface{ ScriptRef() *cbor.Tag }:
		// This interface doesn't exist yet in gouroboros
		// We need to use reflection or wait for gouroboros update
		return nil
	default:
		// GOUROBOROS LIMITATION: ScriptRef field not exposed
		// The BabbageTransactionOutput struct contains a ScriptRef field
		// but it's not accessible through the TransactionOutput interface.
		// This prevents us from indexing reference scripts attached to outputs.
		// When gouroboros adds this to the interface, we can extract and
		// store the reference script hash.
		_ = o // avoid unused variable warning
		return nil
	}
}

func (bp *BlockProcessor) extractPaymentCredential(addr ledger.Address) []byte {
	// Extract payment credential hash from address bytes
	addrBytes, err := addr.Bytes()
	if err != nil || len(addrBytes) < 29 {
		return nil
	}

	// For most address types, bytes 1-28 contain the payment credential hash
	// (byte 0 is the address type/network byte)
	return addrBytes[1:29]
}

func (bp *BlockProcessor) extractStakeAddress(addr ledger.Address) bool {
	// For Shelley+ addresses, we need to check if this is a base address with stake component

	addrBytes, err := addr.Bytes()
	if err != nil || len(addrBytes) < 57 {
		// Not a base address with stake component
		return false
	}

	// Check address type (first byte)
	addrType := addrBytes[0] & 0xF0

	// Base addresses (type 0x00-0x07) contain stake credentials
	if addrType>>4 > 7 {
		return false
	}

	// This is a base address with stake component
	return true
}

// SetStateQueryService sets the state query service for reward calculation
func (bp *BlockProcessor) SetStateQueryService(service StateQueryService) {
	bp.stateQueryService = service
}

func stringPtr(s string) *string {
	return &s
}

// isZeroHash checks if a Blake2b256 hash is all zeros
func isZeroHash(hash [32]byte) bool {
	for _, b := range hash {
		if b != 0 {
			return false
		}
	}
	return true
}

// updateEraConfig updates the era configuration when epoch changes
func (bp *BlockProcessor) updateEraConfig(epochNo uint32) {
	oldEra := GetEraName(bp.currentEpoch)
	newEra := GetEraName(uint64(epochNo))

	// Update configuration
	bp.currentEpoch = uint64(epochNo)
	bp.currentEraConfig = GetEraConfig(uint64(epochNo))

	// Log era transition
	if oldEra != newEra {
		log.Printf("[ERA TRANSITION] Entering %s era at epoch %d", newEra, epochNo)
		log.Printf("[ERA CONFIG] TxBatch: %d, TxOutBatch: %d, AssetBatch: %d",
			bp.currentEraConfig.TxBatchSize,
			bp.currentEraConfig.TxOutBatchSize,
			bp.currentEraConfig.AssetBatchSize)
	}
}

// processCollateralInputs processes collateral inputs for a transaction
func (bp *BlockProcessor) processCollateralInputs(ctx context.Context, tx *gorm.DB, txHash []byte, transaction ledger.Transaction, blockType uint) error {
	// Only Alonzo and later have collateral
	if blockType < BlockTypeAlonzo {
		return nil
	}

	collateral := transaction.Collateral()
	if len(collateral) == 0 {
		return nil
	}

	inputBatch := make([]models.CollateralTxIn, 0, len(collateral))

	for _, input := range collateral {
		inputHash := input.Id()
		dbInput := models.CollateralTxIn{
			TxInHash:   txHash,
			TxOutHash:  inputHash[:],
			TxOutIndex: input.Index(),
		}
		inputBatch = append(inputBatch, dbInput)
	}

	if len(inputBatch) > 0 {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tx_in_hash"}, {Name: "tx_out_hash"}, {Name: "tx_out_index"}},
			DoNothing: true,
		}).CreateInBatches(inputBatch, 1000).Error; err != nil {
			return fmt.Errorf("failed to batch insert collateral inputs: %w", err)
		}
	}

	return nil
}

// processCollateralReturn processes the collateral return output for a transaction
func (bp *BlockProcessor) processCollateralReturn(ctx context.Context, tx *gorm.DB, txHash []byte, transaction ledger.Transaction, blockType uint) error {
	// Only Babbage and later have collateral return
	if blockType < BlockTypeBabbage {
		return nil
	}

	collateralReturn := transaction.CollateralReturn()
	if collateralReturn == nil {
		return nil
	}

	address := collateralReturn.Address()

	// Safely get address string
	var addressStr string
	func() {
		defer func() {
			if r := recover(); r != nil {
				if addrBytes, err := address.Bytes(); err == nil {
					addressStr = fmt.Sprintf("byron_raw_%x", addrBytes)
					unifiederrors.Get().Warning("BlockProcessor", "CollateralAddressFormat", fmt.Sprintf("Collateral return address.String() panicked, using raw format: %v", r))
				} else {
					addressStr = "invalid_collateral_address"
					unifiederrors.Get().LogError(unifiederrors.ErrorTypeProcessing, "BlockProcessor", "GetCollateralAddress", fmt.Sprintf("Failed to get collateral return address string and bytes: %v", r))
				}
			}
		}()
		addressStr = address.String()
	}()

	// Handle extremely long Byron addresses that exceed database column limit
	// Note: collateral_tx_outs table has VARCHAR(100) for address, much smaller than tx_outs
	const maxCollateralAddressLength = 100
	if len(addressStr) > maxCollateralAddressLength {
		// For addresses that are too long, use a hash-based representation
		addressBytes, err := address.Bytes()
		if err == nil {
			// Use a short hash representation for collateral addresses
			hash := sha256.Sum256(addressBytes)
			addressStr = fmt.Sprintf("byron_h_%x", hash[:8]) // Use first 8 bytes of hash
		} else {
			// Fallback: use truncated identifier
			addressStr = fmt.Sprintf("byron_long_%d", len(addressStr))
		}
		unifiederrors.Get().Warning("BlockProcessor", "CollateralAddressLength",
			fmt.Sprintf("Collateral address too long (%d chars), using hash: %s", len(address.String()), addressStr))
	}

	addressBytes, err := address.Bytes()
	if err != nil {
		addressBytes = []byte{}
	}

	dbOutput := models.CollateralTxOut{
		TxHash:           txHash,
		Index:            0, // Collateral return always has index 0
		Address:          addressStr,
		AddressRaw:       addressBytes,
		AddressHasScript: false, // Will be implemented when address type detection is added
		Value:            collateralReturn.Amount(),
	}

	// Extract payment credential
	if paymentCred := bp.extractPaymentCredential(address); paymentCred != nil {
		dbOutput.PaymentCred = paymentCred
	}

	// Extract stake address hash
	if bp.extractStakeAddress(address) {
		addrBytes, err := address.Bytes()
		if err == nil && len(addrBytes) >= 57 {
			stakeCredHash := addrBytes[29:57]
			dbOutput.StakeAddressHash = stakeCredHash

			// Register stake address
			_, err := bp.stakeAddressCache.GetOrCreateStakeAddressFromBytesWithTx(tx, stakeCredHash)
			if err != nil {
				unifiederrors.Get().Warning("BlockProcessor", "CollateralStakeAddress", fmt.Sprintf("Failed to register stake address from collateral return: %v", err))
			}
		}
	}

	// Handle datum hash if present
	if datumHash := collateralReturn.DatumHash(); datumHash != nil {
		dbOutput.DataHash = datumHash.Bytes()
	}

	// Handle inline datum (Babbage+)
	if inlineDatum := bp.getOutputInlineDatum(collateralReturn, blockType); inlineDatum != nil {
		dbOutput.InlineDatumHash = inlineDatum
	}

	// Handle reference script (Babbage+)
	if refScript := bp.getOutputReferenceScript(collateralReturn, blockType); refScript != nil {
		dbOutput.ReferenceScriptHash = refScript
	}

	// Process multi-assets if present
	if assets := collateralReturn.Assets(); assets != nil {
		// Store the collateral output first with duplicate handling
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tx_hash"}, {Name: "index"}},
			DoNothing: true,
		}).Create(&dbOutput).Error; err != nil {
			if !strings.Contains(err.Error(), "Duplicate entry") {
				return fmt.Errorf("failed to insert collateral return output: %w", err)
			}
		}

		// Process assets with the output ID
		if err := bp.assetProcessor.ProcessCollateralOutputAssets(tx, txHash, 0, assets); err != nil {
			return fmt.Errorf("failed to process collateral return assets: %w", err)
		}

		// Return early since we already inserted the output
		return nil
	}

	// Insert the collateral return output with duplicate handling
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tx_hash"}, {Name: "index"}},
		DoNothing: true,
	}).Create(&dbOutput).Error; err != nil {
		if !strings.Contains(err.Error(), "Duplicate entry") {
			return fmt.Errorf("failed to insert collateral return output: %w", err)
		}
	}

	return nil
}

// processReferenceInputs processes reference inputs for a transaction
func (bp *BlockProcessor) processReferenceInputs(ctx context.Context, tx *gorm.DB, txHash []byte, transaction ledger.Transaction, blockType uint) error {
	// Only Babbage and later have reference inputs
	if blockType < BlockTypeBabbage {
		return nil
	}

	referenceInputs := transaction.ReferenceInputs()
	if len(referenceInputs) == 0 {
		return nil
	}

	inputBatch := make([]models.ReferenceTxIn, 0, len(referenceInputs))

	for _, input := range referenceInputs {
		inputHash := input.Id()
		dbInput := models.ReferenceTxIn{
			TxInHash:   txHash,
			TxOutHash:  inputHash[:],
			TxOutIndex: input.Index(),
		}
		inputBatch = append(inputBatch, dbInput)
	}

	if len(inputBatch) > 0 {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tx_in_hash"}, {Name: "tx_out_hash"}, {Name: "tx_out_index"}},
			DoNothing: true,
		}).CreateInBatches(inputBatch, 1000).Error; err != nil {
			return fmt.Errorf("failed to batch insert reference inputs: %w", err)
		}
	}

	return nil
}

// processRequiredSigners extracts and stores required signers for a transaction
func (bp *BlockProcessor) processRequiredSigners(ctx context.Context, tx *gorm.DB, txHash []byte, transaction ledger.Transaction, blockType uint) error {
	// Only Alonzo and later have required signers
	if blockType < BlockTypeAlonzo {
		return nil
	}

	requiredSigners := transaction.RequiredSigners()
	if len(requiredSigners) == 0 {
		return nil
	}

	// Create batch of required signers
	signerBatch := make([]models.RequiredSigner, 0, len(requiredSigners))
	for i, signer := range requiredSigners {
		signerRecord := models.RequiredSigner{
			TxHash:      txHash,
			SignerIndex: uint32(i),
			SignerHash:  signer[:],
		}
		signerBatch = append(signerBatch, signerRecord)
	}

	// Batch insert required signers
	if len(signerBatch) > 0 {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tx_hash"}, {Name: "signer_index"}},
			DoNothing: true,
		}).CreateInBatches(signerBatch, 100).Error; err != nil {
			return fmt.Errorf("failed to batch insert required signers: %w", err)
		}
	}

	return nil
}

// GetDB returns the database connection
func (bp *BlockProcessor) GetDB() *gorm.DB {
	return bp.db
}

// EnsureHealthyConnection ensures the database connection is healthy
func (bp *BlockProcessor) EnsureHealthyConnection() error {
	return bp.EnsureHealthyConnectionWithContext(context.Background())
}

// EnsureHealthyConnectionWithContext ensures the database connection is healthy with timeout support
func (bp *BlockProcessor) EnsureHealthyConnectionWithContext(ctx context.Context) error {
	var sqlDB *sql.DB
	sqlDB, err := bp.db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying database connection: %w", err)
	}

	// Get connection pool stats
	stats := sqlDB.Stats()
	if stats.OpenConnections > 100 || stats.InUse > 50 {
		log.Printf("[CONN] Connection pool stats: open=%d, in_use=%d, idle=%d, wait_count=%d, wait_duration=%v",
			stats.OpenConnections, stats.InUse, stats.Idle, stats.WaitCount, stats.WaitDuration)
	}

	// Try a simple ping first with context for timeout support
	if err := sqlDB.PingContext(ctx); err != nil {
		// Connection is unhealthy, but DON'T close it here
		// The ConnectionPoolManager should handle recovery
		log.Printf("[ERROR] Connection ping failed: %v (open=%d, in_use=%d)",
			err, stats.OpenConnections, stats.InUse)
		return fmt.Errorf("connection ping failed: %w", err)
	}

	// Also check for any pending results that might cause "commands out of sync"
	// This is a common issue when a previous query wasn't fully consumed
	if err := bp.db.WithContext(ctx).Exec("SELECT 1").Error; err != nil {
		if strings.Contains(err.Error(), "commands out of sync") {
			// Try to clear the connection state
			bp.db.Exec("DO 1") // Simple no-op to clear state
			return fmt.Errorf("connection has pending results: %w", err)
		}
		return fmt.Errorf("connection test query failed: %w", err)
	}

	return nil
}

// SetDB allows updating the database connection for this processor
func (bp *BlockProcessor) SetDB(db *gorm.DB) {
	bp.db = db
}
