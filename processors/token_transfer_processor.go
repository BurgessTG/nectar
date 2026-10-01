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

// TokenTransferProcessor tracks token transfers between wallets.
// It processes transaction inputs (senders) and outputs (receivers)
// to record which addresses sent tokens to which other addresses.
//
// This complements WalletConnectionProcessor (ADA transfers) by
// tracking native asset transfers for the visualization graph.
type TokenTransferProcessor struct {
	db        *gorm.DB
	eventChan chan<- models.TokenTransferEvent // Optional channel for real-time events
}

// NewTokenTransferProcessor creates a new token transfer processor.
// If eventChan is nil, no events will be emitted.
func NewTokenTransferProcessor(db *gorm.DB, eventChan chan<- models.TokenTransferEvent) *TokenTransferProcessor {
	return &TokenTransferProcessor{
		db:        db,
		eventChan: eventChan,
	}
}

// tokenOutput represents a token output for transfer tracking
type tokenOutput struct {
	index        uint32
	address      string
	stakeAddress string
	policyID     []byte
	assetName    []byte
	quantity     uint64
}

// tokenInput represents a token input (spent UTxO) for transfer tracking
type tokenInput struct {
	address      string
	stakeAddress string
	policyID     []byte
	assetName    []byte
	quantity     uint64
}

// ProcessTransaction extracts token transfers from a transaction.
// A transfer is recorded when:
//   - An input spends a UTxO containing token X from address A
//   - An output creates a new UTxO with token X at address B
//   - A != B (not a self-transfer/change output)
func (ttp *TokenTransferProcessor) ProcessTransaction(tx *gorm.DB, txHash []byte, slotNo uint64, transaction ledger.Transaction) error {
	inputs := transaction.Inputs()
	outputs := transaction.Outputs()

	if len(inputs) == 0 || len(outputs) == 0 {
		return nil
	}

	// Step 1: Collect all token INPUTS (who is sending tokens)
	tokenInputs := make([]tokenInput, 0)
	for _, input := range inputs {
		id := input.Id()
		outTxHash := id[:]
		outIndex := input.Index()

		// Look up the spent UTxO
		var spentOut models.TxOut
		err := tx.Where("tx_hash = ? AND `index` = ?", outTxHash, outIndex).First(&spentOut).Error
		if err != nil {
			continue // UTxO not found
		}

		// Look up tokens in the spent UTxO
		var spentTokens []models.MaTxOut
		err = tx.Where("tx_hash = ? AND tx_index = ?", outTxHash, outIndex).Find(&spentTokens).Error
		if err != nil {
			continue
		}

		// Record each token being spent
		stakeAddr := ttp.deriveStakeAddress(spentOut.Address, spentOut.StakeAddressHash)
		for _, token := range spentTokens {
			tokenInputs = append(tokenInputs, tokenInput{
				address:      spentOut.Address,
				stakeAddress: stakeAddr,
				policyID:     token.Policy,
				assetName:    token.Name,
				quantity:     token.Quantity,
			})
		}
	}

	if len(tokenInputs) == 0 {
		return nil // No token inputs to track
	}

	// Step 2: Collect all token OUTPUTS (who is receiving tokens)
	tokenOutputs := make([]tokenOutput, 0)
	for i, output := range outputs {
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

		stakeAddr := ttp.deriveStakeAddressFromAddr(addr)

		// Get assets from output
		assets := ttp.getOutputAssets(output)
		if assets == nil {
			continue
		}

		for _, policyID := range assets.Policies() {
			policyIDBytes := policyID[:]
			assetNames := assets.Assets(policyID)
			for _, assetNameBytes := range assetNames {
				amount := assets.Asset(policyID, assetNameBytes)
				if amount == 0 {
					continue
				}

				tokenOutputs = append(tokenOutputs, tokenOutput{
					index:        uint32(i),
					address:      addressStr,
					stakeAddress: stakeAddr,
					policyID:     policyIDBytes,
					assetName:    assetNameBytes,
					quantity:     uint64(amount),
				})
			}
		}
	}

	if len(tokenOutputs) == 0 {
		return nil // No token outputs
	}

	// Step 3: Match inputs to outputs to create transfer records
	// For each output, find inputs of the same token from different addresses
	transfers := make([]models.TokenWalletConnection, 0)

	for _, out := range tokenOutputs {
		for _, in := range tokenInputs {
			// Same token?
			if !bytesEqual(in.policyID, out.policyID) || !bytesEqual(in.assetName, out.assetName) {
				continue
			}

			// Different addresses? (not a self-transfer)
			if in.address == out.address {
				continue
			}

			// Record the transfer
			transfers = append(transfers, models.TokenWalletConnection{
				TxHash:           txHash,
				TxIndex:          out.index,
				PolicyID:         out.policyID,
				AssetName:        out.assetName,
				FromAddress:      in.address,
				ToAddress:        out.address,
				FromStakeAddress: in.stakeAddress,
				ToStakeAddress:   out.stakeAddress,
				Quantity:         out.quantity, // Use output quantity
				Slot:             slotNo,
			})

			// Emit event if channel is configured
			ttp.emitEvent(models.TokenTransferEvent{
				EventType:   "token_transfer",
				PolicyID:    hex.EncodeToString(out.policyID),
				AssetName:   hex.EncodeToString(out.assetName),
				FromAddress: in.address,
				ToAddress:   out.address,
				Quantity:    out.quantity,
				TxHash:      hex.EncodeToString(txHash),
				Slot:        slotNo,
			})
		}
	}

	// Step 4: Batch insert transfers
	if len(transfers) > 0 {
		return ttp.insertTransfers(tx, transfers)
	}

	return nil
}

// insertTransfers batch inserts transfer records
func (ttp *TokenTransferProcessor) insertTransfers(tx *gorm.DB, transfers []models.TokenWalletConnection) error {
	if len(transfers) == 0 {
		return nil
	}

	// Build batch INSERT with multiple VALUES
	var valueStrings []string
	var valueArgs []interface{}

	for _, t := range transfers {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs,
			t.TxHash,
			t.TxIndex,
			t.PolicyID,
			t.AssetName,
			t.FromAddress,
			t.ToAddress,
			t.FromStakeAddress,
			t.ToStakeAddress,
			t.Quantity,
			t.Slot,
			t.BlockHeight,
		)
	}

	sql := fmt.Sprintf(`
		INSERT INTO token_wallet_connections
			(tx_hash, tx_index, policy_id, asset_name, from_address, to_address,
			 from_stake_address, to_stake_address, quantity, slot, block_height)
		VALUES %s
		ON DUPLICATE KEY UPDATE
			quantity = VALUES(quantity)
	`, strings.Join(valueStrings, ","))

	return database.RetryOperation(func() error {
		return tx.Exec(sql, valueArgs...).Error
	})
}

// emitEvent sends an event to the event channel if configured.
func (ttp *TokenTransferProcessor) emitEvent(event models.TokenTransferEvent) {
	if ttp.eventChan == nil {
		return
	}
	// Non-blocking send
	select {
	case ttp.eventChan <- event:
	default:
		log.Printf("[TOKEN_TRANSFER] Warning: Event channel full, dropping event")
	}
}

// bytesEqual compares two byte slices
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// getOutputAssets extracts multi-assets from a transaction output
func (ttp *TokenTransferProcessor) getOutputAssets(output ledger.TransactionOutput) *common.MultiAsset[common.MultiAssetTypeOutput] {
	switch outputWithAssets := output.(type) {
	case interface {
		Assets() *common.MultiAsset[common.MultiAssetTypeOutput]
	}:
		return outputWithAssets.Assets()
	default:
		return nil
	}
}

// deriveStakeAddress derives stake address from stake hash
func (ttp *TokenTransferProcessor) deriveStakeAddress(address string, stakeHash []byte) string {
	if len(stakeHash) == 0 {
		return ""
	}
	hexStr := hex.EncodeToString(stakeHash)
	if len(hexStr) > 56 {
		hexStr = hexStr[:56]
	}
	return "stake_" + hexStr
}

// deriveStakeAddressFromAddr derives stake address from a ledger address
func (ttp *TokenTransferProcessor) deriveStakeAddressFromAddr(addr ledger.Address) string {
	addrBytes, err := addr.Bytes()
	if err != nil || len(addrBytes) < 57 {
		return ""
	}

	addrType := addrBytes[0] >> 4
	if addrType > 1 {
		return ""
	}

	stakeCredHash := addrBytes[29:57]
	hexStr := hex.EncodeToString(stakeCredHash)
	return "stake_" + hexStr
}

// HandleRollback deletes all token_wallet_connections after the rollback slot.
// This table stores individual events with a slot column, so a simple delete suffices.
func (ttp *TokenTransferProcessor) HandleRollback(tx *gorm.DB, rollbackSlot uint64) error {
	result := tx.Exec(`DELETE FROM token_wallet_connections WHERE slot > ?`, rollbackSlot)
	if result.Error != nil {
		return fmt.Errorf("failed to delete rolled-back token transfers: %w", result.Error)
	}
	log.Printf("[TOKEN_TRANSFER] Rollback: deleted %d token_wallet_connections rows", result.RowsAffected)
	return nil
}

// BackfillFromSlotRange populates token transfers from existing data in a slot range.
// This is the CORE backfill query that joins:
//   - ma_tx_outs (token outputs - receivers)
//   - tx_outs (output addresses)
//   - tx_ins (which inputs were spent)
//   - tx_outs (input addresses - senders)
//   - ma_tx_outs (verify sender had the same token)
func (ttp *TokenTransferProcessor) BackfillFromSlotRange(startSlot, endSlot uint64) error {
	log.Printf("[TOKEN_TRANSFER] Backfilling transfers for slots %d to %d", startSlot, endSlot)

	// This query finds all token transfers in the slot range.
	// It matches token outputs with inputs that had the same token.
	sql := `
		INSERT INTO token_wallet_connections
			(tx_hash, tx_index, policy_id, asset_name, from_address, to_address,
			 from_stake_address, to_stake_address, quantity, slot, block_height)
		SELECT
			output_ma.tx_hash,
			output_ma.tx_index,
			output_ma.policy,
			output_ma.name,
			input_addr.address AS from_address,
			output_addr.address AS to_address,
			COALESCE(CONCAT('stake_', HEX(input_addr.stake_address_hash)), '') AS from_stake_address,
			COALESCE(CONCAT('stake_', HEX(output_addr.stake_address_hash)), '') AS to_stake_address,
			output_ma.quantity,
			b.slot_no AS slot,
			b.block_no AS block_height
		FROM ma_tx_outs output_ma
		-- Get receiving address
		INNER JOIN tx_outs output_addr
			ON output_ma.tx_hash = output_addr.tx_hash
			AND output_ma.tx_index = output_addr.` + "`index`" + `
		-- Get the transaction
		INNER JOIN txes t ON t.hash = output_ma.tx_hash
		-- Get block info
		INNER JOIN blocks b ON b.hash = t.block_hash
		-- Get inputs spent in this tx
		INNER JOIN tx_ins ti ON ti.tx_in_hash = output_ma.tx_hash
		-- Get the input's original UTxO address
		INNER JOIN tx_outs input_addr
			ON ti.tx_out_hash = input_addr.tx_hash
			AND ti.tx_out_index = input_addr.` + "`index`" + `
		-- Verify input also had the same token
		INNER JOIN ma_tx_outs input_ma
			ON ti.tx_out_hash = input_ma.tx_hash
			AND ti.tx_out_index = input_ma.tx_index
			AND input_ma.policy = output_ma.policy
			AND input_ma.name = output_ma.name
		WHERE b.slot_no >= ? AND b.slot_no < ?
			AND input_addr.address != output_addr.address
		ON DUPLICATE KEY UPDATE
			quantity = VALUES(quantity)
	`

	err := ttp.db.Exec(sql, startSlot, endSlot).Error
	if err != nil {
		return fmt.Errorf("failed to backfill transfers for slots %d-%d: %w", startSlot, endSlot, err)
	}

	log.Printf("[TOKEN_TRANSFER] Completed backfill for slots %d to %d", startSlot, endSlot)
	return nil
}
