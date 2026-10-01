package processors

// EraConfig defines performance parameters for each Cardano era
type EraConfig struct {
	WorkerCount    int
	TxBatchSize    int
	TxOutBatchSize int
	TxInBatchSize  int
	AssetBatchSize int
	BlockBatchSize int
	FetchRangeSize int
	QueueSize      int
}

// GetEraConfig returns optimized configuration based on epoch number
func GetEraConfig(epochNo uint64) *EraConfig {
	switch {
	case epochNo < 208: // Byron era
		return &EraConfig{
			WorkerCount:    16,
			TxBatchSize:    1000,
			TxOutBatchSize: 2000,
			TxInBatchSize:  2000,
			AssetBatchSize: 1000,
			BlockBatchSize: 200,
			FetchRangeSize: 5000,
			QueueSize:      100000,
		}

	case epochNo < 236: // Shelley era (staking begins)
		return &EraConfig{
			WorkerCount:    12,
			TxBatchSize:    500,
			TxOutBatchSize: 1000,
			TxInBatchSize:  1000,
			AssetBatchSize: 500,
			BlockBatchSize: 100,
			FetchRangeSize: 2000,
			QueueSize:      50000,
		}

	case epochNo < 290: // Allegra/Mary era (native tokens)
		return &EraConfig{
			WorkerCount:    10,
			TxBatchSize:    400,
			TxOutBatchSize: 800,
			TxInBatchSize:  800,
			AssetBatchSize: 300,
			BlockBatchSize: 80,
			FetchRangeSize: 1500,
			QueueSize:      30000,
		}

	case epochNo < 365: // Alonzo era (smart contracts)
		return &EraConfig{
			WorkerCount:    8,
			TxBatchSize:    300,
			TxOutBatchSize: 500,
			TxInBatchSize:  500,
			AssetBatchSize: 200,
			BlockBatchSize: 50,
			FetchRangeSize: 1000,
			QueueSize:      20000,
		}

	default: // Babbage/Conway era (heavy DeFi)
		return &EraConfig{
			WorkerCount:    6,
			TxBatchSize:    200,
			TxOutBatchSize: 300,
			TxInBatchSize:  300,
			AssetBatchSize: 100,
			BlockBatchSize: 30,
			FetchRangeSize: 500,
			QueueSize:      10000,
		}
	}
}

// GetEraName returns the era name for logging
func GetEraName(epochNo uint64) string {
	switch {
	case epochNo < 208:
		return "Byron"
	case epochNo < 236:
		return "Shelley"
	case epochNo < 290:
		return "Allegra/Mary"
	case epochNo < 365:
		return "Alonzo"
	default:
		return "Babbage/Conway"
	}
}
