package processors

import (
	"fmt"
	"log"
	"nectar/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// EpochParamsProvider provides protocol parameters for epochs
type EpochParamsProvider struct {
	db *gorm.DB
}

// NewEpochParamsProvider creates a new provider
func NewEpochParamsProvider(db *gorm.DB) *EpochParamsProvider {
	return &EpochParamsProvider{db: db}
}

// Helper functions for pointer values
func float64Ptr(v float64) *float64 { return &v }
func uint32Ptr(v uint32) *uint32 { return &v }
func uint64Ptr(v uint64) *uint64 { return &v }
func int64Ptr(v int64) *int64 { return &v }

// MainnetEpochParams contains known mainnet protocol parameters by era
var MainnetEpochParams = map[string]*models.EpochParam{
	"byron": {
		MinFeeA:               44,
		MinFeeB:               155381,
		MaxBlockSize:          65536,
		MaxTxSize:             16384,
		MaxBhSize:             1100,
		KeyDeposit:            0,
		PoolDeposit:           0,
		MinPoolCost:           0,
		MaxEpoch:              18,
		NOpt:                  0,
		A0:                    0,
		Rho:                   0,
		Tau:                   0,
		Decentralisation:      1.0,
		ProtocolMajor:         1,
		ProtocolMinor:         0,
		MinUtxo:               0,
		// Byron doesn't have most parameters
	},
	"shelley": {
		MinFeeA:               44,
		MinFeeB:               155381,
		MaxBlockSize:          65536,
		MaxTxSize:             16384,
		MaxBhSize:             1100,
		KeyDeposit:            2000000,    // 2 ADA
		PoolDeposit:           500000000,  // 500 ADA
		MinPoolCost:           340000000,  // 340 ADA
		PriceMem:              float64Ptr(0.0577),
		PriceStep:             float64Ptr(0.0000721),
		MaxEpoch:              18,
		NOpt:                  150,
		A0:                    0.3,
		Rho:                   0.003,
		Tau:                   0.2,
		Decentralisation:      0.0,
		ProtocolMajor:         2,
		ProtocolMinor:         0,
		MinUtxo:               1000000,
		CollateralPercent:     uint32Ptr(0),
		MaxCollateralInputs:   uint32Ptr(0),
	},
	"allegra": {
		MinFeeA:               44,
		MinFeeB:               155381,
		MaxBlockSize:          65536,
		MaxTxSize:             16384,
		MaxBhSize:             1100,
		KeyDeposit:            2000000,
		PoolDeposit:           500000000,
		MinPoolCost:           340000000,
		PriceMem:              float64Ptr(0.0577),
		PriceStep:             float64Ptr(0.0000721),
		MaxEpoch:              18,
		NOpt:                  500,
		A0:                    0.3,
		Rho:                   0.003,
		Tau:                   0.2,
		Decentralisation:      0.0,
		ProtocolMajor:         3,
		ProtocolMinor:         0,
		MinUtxo:               1000000,
		CollateralPercent:     uint32Ptr(0),
		MaxCollateralInputs:   uint32Ptr(0),
	},
	"mary": {
		MinFeeA:               44,
		MinFeeB:               155381,
		MaxBlockSize:          65536,
		MaxTxSize:             16384,
		MaxBhSize:             1100,
		KeyDeposit:            2000000,
		PoolDeposit:           500000000,
		MinPoolCost:           340000000,
		PriceMem:              float64Ptr(0.0577),
		PriceStep:             float64Ptr(0.0000721),
		MaxEpoch:              18,
		NOpt:                  500,
		A0:                    0.3,
		Rho:                   0.003,
		Tau:                   0.2,
		Decentralisation:      0.0,
		ProtocolMajor:         4,
		ProtocolMinor:         0,
		MinUtxo:               1000000,
		CollateralPercent:     uint32Ptr(0),
		MaxCollateralInputs:   uint32Ptr(0),
	},
	"alonzo": {
		MinFeeA:               44,
		MinFeeB:               155381,
		MaxBlockSize:          90112,      // Increased for scripts
		MaxTxSize:             16384,
		MaxBhSize:             1100,
		KeyDeposit:            2000000,
		PoolDeposit:           500000000,
		MinPoolCost:           340000000,
		PriceMem:              float64Ptr(0.0577),
		PriceStep:             float64Ptr(0.0000721),
		MaxEpoch:              18,
		NOpt:                  500,
		A0:                    0.3,
		Rho:                   0.003,
		Tau:                   0.2,
		Decentralisation:      0.0,
		ProtocolMajor:         5,
		ProtocolMinor:         0,
		MinUtxo:               34482,
		CoinsPerUtxoSize:      int64Ptr(34482),
		CollateralPercent:     uint32Ptr(150),
		MaxCollateralInputs:   uint32Ptr(3),
		MaxTxExMem:            uint64Ptr(14000000),
		MaxTxExSteps:          uint64Ptr(10000000000),
		MaxBlockExMem:         uint64Ptr(62000000),
		MaxBlockExSteps:       uint64Ptr(20000000000),
	},
	"babbage": {
		MinFeeA:               44,
		MinFeeB:               155381,
		MaxBlockSize:          90112,
		MaxTxSize:             16384,
		MaxBhSize:             1100,
		KeyDeposit:            2000000,
		PoolDeposit:           500000000,
		MinPoolCost:           170000000,  // Reduced to 170 ADA
		PriceMem:              float64Ptr(0.0577),
		PriceStep:             float64Ptr(0.0000721),
		MaxEpoch:              18,
		NOpt:                  500,
		A0:                    0.3,
		Rho:                   0.003,
		Tau:                   0.2,
		Decentralisation:      0.0,
		ProtocolMajor:         7,
		ProtocolMinor:         0,
		MinUtxo:               0,
		CoinsPerUtxoSize:      int64Ptr(4310),
		CollateralPercent:     uint32Ptr(150),
		MaxCollateralInputs:   uint32Ptr(3),
		MaxTxExMem:            uint64Ptr(14000000),
		MaxTxExSteps:          uint64Ptr(10000000000),
		MaxBlockExMem:         uint64Ptr(62000000),
		MaxBlockExSteps:       uint64Ptr(20000000000),
		MaxValSize:            uint64Ptr(5000),
	},
	"conway": {
		MinFeeA:               44,
		MinFeeB:               155381,
		MaxBlockSize:          90112,
		MaxTxSize:             16384,
		MaxBhSize:             1100,
		KeyDeposit:            2000000,
		PoolDeposit:           500000000,
		MinPoolCost:           170000000,
		PriceMem:              float64Ptr(0.0577),
		PriceStep:             float64Ptr(0.0000721),
		MaxEpoch:              18,
		NOpt:                  500,
		A0:                    0.3,
		Rho:                   0.003,
		Tau:                   0.2,
		Decentralisation:      0.0,
		ProtocolMajor:         9,
		ProtocolMinor:         0,
		MinUtxo:               0,
		CoinsPerUtxoSize:      int64Ptr(4310),
		CollateralPercent:     uint32Ptr(150),
		MaxCollateralInputs:   uint32Ptr(3),
		MaxTxExMem:            uint64Ptr(14000000),
		MaxTxExSteps:          uint64Ptr(10000000000),
		MaxBlockExMem:         uint64Ptr(62000000),
		MaxBlockExSteps:       uint64Ptr(20000000000),
		MaxValSize:            uint64Ptr(5000),
		// Conway adds governance parameters
		DrepDeposit:           uint64Ptr(500000000),  // 500 ADA
		GovActionDeposit:      uint64Ptr(100000000),  // 100 ADA
		CommitteeMinSize:      uint64Ptr(7),
		CommitteeMaxTermLength: uint64Ptr(146),  // epochs
		GovActionLifetime:     uint64Ptr(6),     // epochs
		DrepActivity:          uint64Ptr(20),     // epochs
	},
}

// GetEraForEpoch returns the era name for a given epoch
func GetEraForEpoch(epochNo uint32) string {
	switch {
	case epochNo < 208:
		return "byron"
	case epochNo < 236:
		return "shelley"
	case epochNo < 251:
		return "allegra"
	case epochNo < 290:
		return "mary"
	case epochNo < 365:
		return "alonzo"
	case epochNo < 491:
		return "babbage"
	default:
		return "conway"
	}
}

// ProvideEpochParams creates epoch parameters for a given epoch
func (epp *EpochParamsProvider) ProvideEpochParams(epochNo uint32) error {
	era := GetEraForEpoch(epochNo)
	params, exists := MainnetEpochParams[era]
	if !exists {
		return fmt.Errorf("no parameters defined for era %s", era)
	}
	
	// Clone the params and set epoch number
	epochParams := *params
	epochParams.EpochNo = epochNo
	
	// Insert or update
	err := epp.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "epoch_no"}},
		UpdateAll: true,
	}).Create(&epochParams).Error
	
	if err != nil {
		return fmt.Errorf("failed to save epoch params: %w", err)
	}
	
	log.Printf("[EPOCH_PARAMS] Set parameters for epoch %d (%s era)", epochNo, era)
	return nil
}

// BackfillEpochParams creates parameters for all past epochs
func (epp *EpochParamsProvider) BackfillEpochParams() error {
	log.Printf("[EPOCH_PARAMS] Starting backfill of epoch parameters")

	// Get max epoch from blocks (using ORDER BY LIMIT 1 for faster index scan)
	var maxEpoch uint32
	err := epp.db.Model(&models.Block{}).
		Select("epoch_no").
		Order("epoch_no DESC").
		Limit(1).
		Scan(&maxEpoch).Error

	if err != nil {
		return fmt.Errorf("failed to get max epoch: %w", err)
	}
	
	log.Printf("[EPOCH_PARAMS] Backfilling parameters for epochs 0-%d", maxEpoch)
	
	// Process each epoch
	for epoch := uint32(0); epoch <= maxEpoch; epoch++ {
		if err := epp.ProvideEpochParams(epoch); err != nil {
			log.Printf("[ERROR] Failed to provide params for epoch %d: %v", epoch, err)
			continue
		}
		
		if epoch%50 == 0 {
			log.Printf("[EPOCH_PARAMS] Progress: %d/%d epochs", epoch, maxEpoch)
		}
	}
	
	log.Printf("[EPOCH_PARAMS] Backfill complete!")
	return nil
}