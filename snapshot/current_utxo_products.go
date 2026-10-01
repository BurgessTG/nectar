package snapshot

import (
	"fmt"

	"gorm.io/gorm"
)

// RefreshTokenHoldersFromCurrentUTxO rebuilds token_holders from a current UTxO
// snapshot and returns the number of holder rows inserted. Unlike the historical
// product rebuild, this deliberately does not join txes, blocks, or tx_ins: the
// snapshot import table set is already the current unspent outputs.
func RefreshTokenHoldersFromCurrentUTxO(db *gorm.DB, lastUpdatedSlot uint64) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("database handle is required")
	}

	var rowsAffected int64
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("DELETE FROM token_holders").Error; err != nil {
			return fmt.Errorf("clear token_holders: %w", err)
		}

		result := tx.Exec(CurrentUTxOHolderRefreshSQL(), lastUpdatedSlot)
		if result.Error != nil {
			return fmt.Errorf("seed token_holders from current UTxO: %w", result.Error)
		}
		rowsAffected = result.RowsAffected

		return nil
	})
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

func CurrentUTxOHolderRefreshSQL() string {
	return `
			INSERT INTO token_holders
				(policy, name, address, amount, tx_hash, output_index, stake_address, last_updated_slot)
			SELECT
				mao.policy,
				mao.name,
				txo.address,
				SUM(mao.quantity) AS amount,
				MAX(mao.tx_hash) AS tx_hash,
				MAX(mao.tx_index) AS output_index,
				COALESCE(CONCAT('stake_', HEX(txo.stake_address_hash)), '') AS stake_address,
				? AS last_updated_slot
			FROM ma_tx_outs mao
			JOIN tx_outs txo
				ON mao.tx_hash = txo.tx_hash
				AND mao.tx_index = txo.` + "`index`" + `
			GROUP BY mao.policy, mao.name, txo.address, txo.stake_address_hash
			ON DUPLICATE KEY UPDATE
				amount = VALUES(amount),
				tx_hash = VALUES(tx_hash),
				output_index = VALUES(output_index),
				stake_address = VALUES(stake_address),
				last_updated_slot = VALUES(last_updated_slot)
		`
}
