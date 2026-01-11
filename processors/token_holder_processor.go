package processors

import (
	"encoding/hex"
	"fmt"
	"log"
	"nectar/database"
	"nectar/models"
	"strings"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// TokenHolderProcessor handles incremental updates to the token_holders table.
// It tracks token balance changes by processing transaction inputs (decreases)
// and outputs (increases), then applies the net changes atomically.
type TokenHolderProcessor struct {
	db        *gorm.DB
	eventChan chan<- models.TokenHolderEvent // Optional channel for real-time events
}

// NewTokenHolderProcessor creates a new token holder processor.
// If eventChan is nil, no events will be emitted (useful for backfill).
func NewTokenHolderProcessor(db *gorm.DB, eventChan chan<- models.TokenHolderEvent) *TokenHolderProcessor {
	return &TokenHolderProcessor{
		db:        db,
		eventChan: eventChan,
	}
}

// ProcessTransaction updates token_holders based on transaction inputs and outputs
// This is called for each transaction during block processing
func (thp *TokenHolderProcessor) ProcessTransaction(tx *gorm.DB, txHash []byte, slotNo uint64, transaction ledger.Transaction) error {
	inputs := transaction.Inputs()
	outputs := transaction.Outputs()

	// Track balance changes: map[policy+name+address] -> delta (can be negative)
	balanceChanges := make(map[string]*holderChange)

	// Step 1: Process INPUTS (tokens leaving wallets - DECREASE)
	for _, input := range inputs {
		id := input.Id()
		outTxHash := id[:]
		outIndex := input.Index()

		// Look up the spent UTxO to get address
		var spentOut models.TxOut
		err := tx.Where("tx_hash = ? AND `index` = ?", outTxHash, outIndex).First(&spentOut).Error
		if err != nil {
			// UTxO might not exist if from before our sync start
			continue
		}

		// Look up any tokens in this spent UTxO
		var spentTokens []models.MaTxOut
		err = tx.Where("tx_hash = ? AND tx_index = ?", outTxHash, outIndex).Find(&spentTokens).Error
		if err != nil {
			continue
		}

		// For each token in the spent UTxO, record a DECREASE
		for _, token := range spentTokens {
			key := makeHolderKey(token.Policy, token.Name, spentOut.Address)
			if _, exists := balanceChanges[key]; !exists {
				balanceChanges[key] = &holderChange{
					policy:       token.Policy,
					name:         token.Name,
					address:      spentOut.Address,
					stakeAddress: thp.deriveStakeAddress(spentOut.Address, spentOut.StakeAddressHash),
					delta:        0,
				}
			}
			balanceChanges[key].delta -= int64(token.Quantity)
		}
	}

	// Step 2: Process OUTPUTS (tokens arriving at wallets - INCREASE)
	for i, output := range outputs {
		// Get address
		addr := output.Address()
		var addressStr string
		func() {
			defer func() {
				if r := recover(); r != nil {
					addressStr = ""
				}
			}()
			addressStr = addr.String()
		}()

		if addressStr == "" || len(addressStr) > 256 {
			continue
		}

		// Get stake address
		stakeAddr := thp.deriveStakeAddressFromAddr(addr)

		// Get assets from output
		assets := thp.getOutputAssets(output)
		if assets == nil {
			continue
		}

		// For each token in the output, record an INCREASE
		for _, policyID := range assets.Policies() {
			policyIDBytes := policyID[:]
			assetNames := assets.Assets(policyID)
			for _, assetNameBytes := range assetNames {
				amount := assets.Asset(policyID, assetNameBytes)
				if amount == 0 {
					continue
				}

				key := makeHolderKey(policyIDBytes, assetNameBytes, addressStr)
				if _, exists := balanceChanges[key]; !exists {
					balanceChanges[key] = &holderChange{
						policy:       policyIDBytes,
						name:         assetNameBytes,
						address:      addressStr,
						stakeAddress: stakeAddr,
						delta:        0,
						txHash:       txHash,
						outputIndex:  uint32(i),
					}
				}
				balanceChanges[key].delta += int64(amount)
				balanceChanges[key].txHash = txHash
				balanceChanges[key].outputIndex = uint32(i)
			}
		}
	}

	// Step 3: Apply balance changes to database
	if len(balanceChanges) == 0 {
		return nil
	}

	return thp.applyBalanceChanges(tx, balanceChanges, slotNo)
}

// holderChange tracks the net change for a specific holder
type holderChange struct {
	policy       []byte
	name         []byte
	address      string
	stakeAddress string
	delta        int64 // Can be negative (spending) or positive (receiving)
	txHash       []byte
	outputIndex  uint32
}

// makeHolderKey creates a unique key for a holder
func makeHolderKey(policy, name []byte, address string) string {
	return fmt.Sprintf("%s:%s:%s", hex.EncodeToString(policy), hex.EncodeToString(name), address)
}

// applyBalanceChanges applies all balance changes to the database
func (thp *TokenHolderProcessor) applyBalanceChanges(tx *gorm.DB, changes map[string]*holderChange, slotNo uint64) error {
	// Separate increases and decreases for different handling
	var increases []*holderChange
	var decreases []*holderChange

	for _, change := range changes {
		if change.delta > 0 {
			increases = append(increases, change)
		} else if change.delta < 0 {
			decreases = append(decreases, change)
		}
		// delta == 0 means no net change, skip
	}

	// Handle INCREASES (new holders or balance additions)
	if len(increases) > 0 {
		if err := thp.upsertIncreases(tx, increases, slotNo); err != nil {
			return fmt.Errorf("failed to upsert increases: %w", err)
		}
	}

	// Handle DECREASES (balance reductions, possibly to zero)
	if len(decreases) > 0 {
		if err := thp.applyDecreases(tx, decreases, slotNo); err != nil {
			return fmt.Errorf("failed to apply decreases: %w", err)
		}
	}

	return nil
}

// upsertIncreases handles balance increases using batch INSERT ON DUPLICATE KEY UPDATE.
// Uses slot-based protection: only updates if new slot >= existing slot to prevent
// race conditions between backfill and live sync.
func (thp *TokenHolderProcessor) upsertIncreases(tx *gorm.DB, increases []*holderChange, slotNo uint64) error {
	if len(increases) == 0 {
		return nil
	}

	// Build batch INSERT with multiple VALUES
	var valueStrings []string
	var valueArgs []interface{}

	for _, inc := range increases {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs,
			inc.policy,
			inc.name,
			inc.address,
			uint64(inc.delta), // We know delta > 0 here
			inc.txHash,
			inc.outputIndex,
			inc.stakeAddress,
			slotNo,
		)

		// Emit event if channel is configured (non-blocking)
		thp.emitEvent(models.TokenHolderEvent{
			EventType: "balance_increased",
			Policy:    inc.policy,
			Name:      hex.EncodeToString(inc.name), // Hex encode binary name
			Address:   inc.address,
			NewAmount: uint64(inc.delta),
			TxHash:    inc.txHash,
			Slot:      slotNo,
		})
	}

	// Race condition protection: Only update if new slot >= existing slot.
	// This ensures live sync (higher slots) takes precedence over backfill (lower slots)
	// when both are writing to the same holder.
	sql := fmt.Sprintf(`
		INSERT INTO token_holders
			(policy, name, address, amount, tx_hash, output_index, stake_address, last_updated_slot)
		VALUES %s
		ON DUPLICATE KEY UPDATE
			amount = CASE WHEN VALUES(last_updated_slot) >= last_updated_slot
			              THEN amount + VALUES(amount)
			              ELSE amount END,
			tx_hash = CASE WHEN VALUES(last_updated_slot) >= last_updated_slot
			               THEN VALUES(tx_hash)
			               ELSE tx_hash END,
			output_index = CASE WHEN VALUES(last_updated_slot) >= last_updated_slot
			                    THEN VALUES(output_index)
			                    ELSE output_index END,
			last_updated_slot = CASE WHEN VALUES(last_updated_slot) >= last_updated_slot
			                         THEN VALUES(last_updated_slot)
			                         ELSE last_updated_slot END
	`, strings.Join(valueStrings, ","))

	return database.RetryOperation(func() error {
		return tx.Exec(sql, valueArgs...).Error
	})
}

// applyDecreases handles balance decreases, deleting holders if balance reaches 0.
// Uses slot-based protection: only updates if new slot >= existing slot.
func (thp *TokenHolderProcessor) applyDecreases(tx *gorm.DB, decreases []*holderChange, slotNo uint64) error {
	for _, dec := range decreases {
		decreaseAmount := uint64(-dec.delta) // Convert negative delta to positive

		// Race condition protection: Only decrease if new slot >= existing slot.
		// This prevents backfill from overwriting live sync updates.
		result := tx.Exec(`
			UPDATE token_holders
			SET amount = CASE
				WHEN ? >= last_updated_slot AND amount >= ? THEN amount - ?
				WHEN ? >= last_updated_slot THEN 0
				ELSE amount
			END,
			last_updated_slot = CASE
				WHEN ? >= last_updated_slot THEN ?
				ELSE last_updated_slot
			END
			WHERE policy = ? AND name = ? AND address = ?
		`, slotNo, decreaseAmount, decreaseAmount, slotNo, slotNo, slotNo, dec.policy, dec.name, dec.address)

		if result.Error != nil {
			log.Printf("[TOKEN_HOLDER] Warning: Failed to decrease balance for %s: %v", dec.address, result.Error)
			continue
		}

		// Then delete any rows where amount is now 0
		tx.Exec(`
			DELETE FROM token_holders
			WHERE policy = ? AND name = ? AND address = ? AND amount = 0
		`, dec.policy, dec.name, dec.address)

		// Emit event if channel is configured (non-blocking)
		thp.emitEvent(models.TokenHolderEvent{
			EventType: "balance_decreased",
			Policy:    dec.policy,
			Name:      hex.EncodeToString(dec.name), // Hex encode binary name
			Address:   dec.address,
			OldAmount: decreaseAmount,
			TxHash:    dec.txHash,
			Slot:      slotNo,
		})
	}

	return nil
}

// emitEvent sends an event to the event channel if configured.
// Uses non-blocking send to avoid slowing down block processing.
func (thp *TokenHolderProcessor) emitEvent(event models.TokenHolderEvent) {
	if thp.eventChan == nil {
		return
	}
	// Non-blocking send - drop event if channel is full rather than block
	select {
	case thp.eventChan <- event:
	default:
		log.Printf("[TOKEN_HOLDER] Warning: Event channel full, dropping event for %s", event.Address)
	}
}

// getOutputAssets extracts multi-assets from a transaction output
func (thp *TokenHolderProcessor) getOutputAssets(output ledger.TransactionOutput) *common.MultiAsset[common.MultiAssetTypeOutput] {
	switch outputWithAssets := output.(type) {
	case interface {
		Assets() *common.MultiAsset[common.MultiAssetTypeOutput]
	}:
		return outputWithAssets.Assets()
	default:
		return nil
	}
}

// deriveStakeAddress derives a stake address identifier from the stake hash.
// Returns empty string if no stake hash is available.
func (thp *TokenHolderProcessor) deriveStakeAddress(address string, stakeHash []byte) string {
	if len(stakeHash) == 0 {
		return ""
	}
	// Return hex-encoded stake hash with prefix for identification
	// Full bech32 encoding could be added later if needed
	hexStr := hex.EncodeToString(stakeHash)
	if len(hexStr) > 56 {
		hexStr = hexStr[:56]
	}
	return "stake_" + hexStr
}

// deriveStakeAddressFromAddr derives stake address from a ledger address.
// Only works for base addresses (types 0x00 and 0x01) which contain stake credentials.
func (thp *TokenHolderProcessor) deriveStakeAddressFromAddr(addr ledger.Address) string {
	addrBytes, err := addr.Bytes()
	if err != nil || len(addrBytes) < 57 {
		return ""
	}

	// Check address type - only base addresses (0x00, 0x01) have stake components
	addrType := addrBytes[0] >> 4
	if addrType > 1 {
		return "" // Enterprise, pointer, reward, or bootstrap address - no stake key
	}

	// For base addresses, bytes 29-56 contain the stake credential hash (28 bytes)
	stakeCredHash := addrBytes[29:57]
	hexStr := hex.EncodeToString(stakeCredHash)
	return "stake_" + hexStr
}

// CleanupZeroBalances removes any holders with zero balance (maintenance task)
func (thp *TokenHolderProcessor) CleanupZeroBalances() error {
	return thp.db.Exec("DELETE FROM token_holders WHERE amount = 0").Error
}

// GetHolderCount returns the total number of holder records
func (thp *TokenHolderProcessor) GetHolderCount() (int64, error) {
	var count int64
	err := thp.db.Model(&models.TokenHolder{}).Count(&count).Error
	return count, err
}
