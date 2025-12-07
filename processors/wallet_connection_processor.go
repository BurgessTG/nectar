package processors

import (
	"fmt"
	"log"
	"nectar/database"
	"nectar/models"
	"strings"

	"github.com/blinklabs-io/gouroboros/ledger"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// WalletConnectionProcessor handles tracking wallet-to-wallet transfer relationships
type WalletConnectionProcessor struct {
	db *gorm.DB
}

// NewWalletConnectionProcessor creates a new wallet connection processor
func NewWalletConnectionProcessor(db *gorm.DB) *WalletConnectionProcessor {
	return &WalletConnectionProcessor{
		db: db,
	}
}

// ProcessTransaction extracts wallet connections from a transaction
// It looks up the sender addresses (from spent UTxOs) and receiver addresses (from outputs)
func (wcp *WalletConnectionProcessor) ProcessTransaction(tx *gorm.DB, txHash []byte, slotNo uint64, transaction ledger.Transaction) error {
	inputs := transaction.Inputs()
	outputs := transaction.Outputs()

	if len(inputs) == 0 || len(outputs) == 0 {
		return nil // No connections to track
	}

	// Get sender addresses by looking up the spent UTxOs
	senderAddresses := make(map[string]bool)
	for _, input := range inputs {
		id := input.Id()
		outTxHash := id[:]
		outIndex := input.Index()

		// Look up the spent UTxO to get sender address
		var spentOut models.TxOut
		err := tx.Where("tx_hash = ? AND `index` = ?", outTxHash, outIndex).First(&spentOut).Error
		if err != nil {
			// UTxO might not exist yet if it was created in an earlier block we haven't indexed
			// This is normal for genesis UTxOs or if we're catching up
			continue
		}
		senderAddresses[spentOut.Address] = true
	}

	if len(senderAddresses) == 0 {
		return nil // No sender addresses found (all inputs reference unknown UTxOs)
	}

	// Get receiver addresses from outputs
	receiverData := make([]struct {
		address string
		value   uint64
	}, 0, len(outputs))

	for _, output := range outputs {
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
			continue // Skip invalid or overly long addresses
		}

		receiverData = append(receiverData, struct {
			address string
			value   uint64
		}{
			address: addressStr,
			value:   output.Amount(),
		})
	}

	if len(receiverData) == 0 {
		return nil
	}

	// Create wallet connections for each sender→receiver pair
	connections := make([]models.WalletConnection, 0)
	connectionTxs := make([]models.WalletConnectionTx, 0)

	for senderAddr := range senderAddresses {
		for _, recv := range receiverData {
			// Skip self-transfers (change outputs)
			if senderAddr == recv.address {
				continue
			}

			// Create aggregated connection
			connections = append(connections, models.WalletConnection{
				SenderAddress:   senderAddr,
				ReceiverAddress: recv.address,
				TotalTxCount:    1,
				TotalAdaSent:    recv.value,
				FirstTxSlot:     slotNo,
				LastTxSlot:      slotNo,
				LastTxHash:      txHash,
			})

			// Create individual transaction record
			connectionTxs = append(connectionTxs, models.WalletConnectionTx{
				SenderAddress:   senderAddr,
				ReceiverAddress: recv.address,
				TxHash:          txHash,
				SlotNo:          slotNo,
				AdaAmount:       recv.value,
			})
		}
	}

	// Batch insert/update wallet connections
	if len(connections) > 0 {
		err := wcp.upsertConnections(tx, connections)
		if err != nil {
			return fmt.Errorf("failed to upsert wallet connections: %w", err)
		}
	}

	// Batch insert individual transaction records
	if len(connectionTxs) > 0 {
		err := database.RetryOperation(func() error {
			return tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(connectionTxs, 500).Error
		})
		if err != nil && !strings.Contains(err.Error(), "Duplicate entry") {
			log.Printf("[WALLET_CONN] Warning: Failed to insert connection txs: %v", err)
		}
	}

	return nil
}

// upsertConnections inserts new connections or updates existing ones using batch INSERT
func (wcp *WalletConnectionProcessor) upsertConnections(tx *gorm.DB, connections []models.WalletConnection) error {
	if len(connections) == 0 {
		return nil
	}

	// Build batch INSERT with multiple VALUES - single DB round trip
	var valueStrings []string
	var valueArgs []interface{}

	for _, conn := range connections {
		valueStrings = append(valueStrings, "(?, ?, 1, ?, ?, ?, ?)")
		valueArgs = append(valueArgs,
			conn.SenderAddress,
			conn.ReceiverAddress,
			conn.TotalAdaSent,
			conn.FirstTxSlot,
			conn.LastTxSlot,
			conn.LastTxHash,
		)
	}

	sql := fmt.Sprintf(`
		INSERT INTO wallet_connections
			(sender_address, receiver_address, total_tx_count, total_ada_sent, first_tx_slot, last_tx_slot, last_tx_hash)
		VALUES %s
		ON DUPLICATE KEY UPDATE
			total_tx_count = total_tx_count + 1,
			total_ada_sent = total_ada_sent + VALUES(total_ada_sent),
			last_tx_slot = GREATEST(last_tx_slot, VALUES(last_tx_slot)),
			last_tx_hash = VALUES(last_tx_hash)
	`, strings.Join(valueStrings, ","))

	return tx.Exec(sql, valueArgs...).Error
}

// BackfillFromSlotRange populates wallet connections from existing data in a slot range
// This is useful for backfilling data that was indexed before this feature was added
func (wcp *WalletConnectionProcessor) BackfillFromSlotRange(startSlot, endSlot uint64) error {
	log.Printf("[WALLET_CONN] Backfilling connections for slots %d to %d", startSlot, endSlot)

	// This query finds all wallet connections in the slot range and inserts them
	sql := `
		INSERT INTO wallet_connections
			(sender_address, receiver_address, total_tx_count, total_ada_sent, first_tx_slot, last_tx_slot, last_tx_hash)
		SELECT
			spent_out.address as sender_address,
			recv_out.address as receiver_address,
			COUNT(DISTINCT ti.tx_in_hash) as total_tx_count,
			COALESCE(SUM(recv_out.value), 0) as total_ada_sent,
			MIN(b.slot_no) as first_tx_slot,
			MAX(b.slot_no) as last_tx_slot,
			MAX(ti.tx_in_hash) as last_tx_hash
		FROM tx_ins ti
		INNER JOIN tx_outs spent_out ON spent_out.tx_hash = ti.tx_out_hash AND spent_out.` + "`index`" + ` = ti.tx_out_index
		INNER JOIN tx_outs recv_out ON recv_out.tx_hash = ti.tx_in_hash
		INNER JOIN txes t ON t.hash = ti.tx_in_hash
		INNER JOIN blocks b ON b.hash = t.block_hash
		WHERE b.slot_no >= ? AND b.slot_no < ?
			AND spent_out.address != recv_out.address
		GROUP BY spent_out.address, recv_out.address
		ON DUPLICATE KEY UPDATE
			total_tx_count = wallet_connections.total_tx_count + VALUES(total_tx_count),
			total_ada_sent = wallet_connections.total_ada_sent + VALUES(total_ada_sent),
			last_tx_slot = GREATEST(wallet_connections.last_tx_slot, VALUES(last_tx_slot)),
			last_tx_hash = VALUES(last_tx_hash)
	`

	err := wcp.db.Exec(sql, startSlot, endSlot).Error
	if err != nil {
		return fmt.Errorf("failed to backfill connections for slots %d-%d: %w", startSlot, endSlot, err)
	}

	log.Printf("[WALLET_CONN] Completed backfill for slots %d to %d", startSlot, endSlot)
	return nil
}

// BackfillIndividualTxs populates wallet_connection_txs from existing data in a slot range
func (wcp *WalletConnectionProcessor) BackfillIndividualTxs(startSlot, endSlot uint64) error {
	log.Printf("[WALLET_CONN] Backfilling individual txs for slots %d to %d", startSlot, endSlot)

	sql := `
		INSERT IGNORE INTO wallet_connection_txs
			(sender_address, receiver_address, tx_hash, slot_no, ada_amount)
		SELECT
			spent_out.address as sender_address,
			recv_out.address as receiver_address,
			ti.tx_in_hash as tx_hash,
			b.slot_no as slot_no,
			recv_out.value as ada_amount
		FROM tx_ins ti
		INNER JOIN tx_outs spent_out ON spent_out.tx_hash = ti.tx_out_hash AND spent_out.` + "`index`" + ` = ti.tx_out_index
		INNER JOIN tx_outs recv_out ON recv_out.tx_hash = ti.tx_in_hash
		INNER JOIN txes t ON t.hash = ti.tx_in_hash
		INNER JOIN blocks b ON b.hash = t.block_hash
		WHERE b.slot_no >= ? AND b.slot_no < ?
			AND spent_out.address != recv_out.address
	`

	err := wcp.db.Exec(sql, startSlot, endSlot).Error
	if err != nil {
		return fmt.Errorf("failed to backfill individual txs for slots %d-%d: %w", startSlot, endSlot, err)
	}

	log.Printf("[WALLET_CONN] Completed individual tx backfill for slots %d to %d", startSlot, endSlot)
	return nil
}
