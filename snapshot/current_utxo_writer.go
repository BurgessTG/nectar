package snapshot

import (
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"nectar/models"
)

type currentUTxOBatch struct {
	TxOuts      []models.TxOut
	MaTxOuts    []models.MaTxOut
	MultiAssets map[string]models.MultiAsset
}

func writeCurrentUTxOBatch(db *gorm.DB, batch currentUTxOBatch) error {
	if db == nil {
		return fmt.Errorf("database handle is required")
	}

	return db.Transaction(func(tx *gorm.DB) error {
		if len(batch.MultiAssets) > 0 {
			values := make([]models.MultiAsset, 0, len(batch.MultiAssets))
			for _, value := range batch.MultiAssets {
				values = append(values, value)
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "policy"}, {Name: "name"}},
				DoNothing: true,
			}).CreateInBatches(values, len(values)).Error; err != nil {
				return fmt.Errorf("upsert multi_assets: %w", err)
			}
		}

		if len(batch.TxOuts) > 0 {
			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "tx_hash"}, {Name: "index"}},
				DoUpdates: clause.AssignmentColumns([]string{
					"address",
					"address_raw",
					"address_has_script",
					"payment_cred",
					"stake_address_hash",
					"value",
					"data_hash",
					"inline_datum_hash",
					"reference_script_hash",
				}),
			}).CreateInBatches(batch.TxOuts, len(batch.TxOuts)).Error; err != nil {
				return fmt.Errorf("upsert tx_outs: %w", err)
			}
		}

		if len(batch.MaTxOuts) > 0 {
			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "tx_hash"}, {Name: "tx_index"}, {Name: "policy"}, {Name: "name"}},
				DoUpdates: clause.AssignmentColumns([]string{
					"quantity",
				}),
			}).CreateInBatches(batch.MaTxOuts, len(batch.MaTxOuts)).Error; err != nil {
				return fmt.Errorf("upsert ma_tx_outs: %w", err)
			}
		}
		return nil
	})
}
