package processors_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"nectar/config"
	"nectar/models"
	"nectar/processors"
)

func testBytes(size int, fill byte) []byte {
	return bytes.Repeat([]byte{fill}, size)
}

func testIndexingConfig() *config.IndexingConfig {
	return &config.IndexingConfig{
		Transactions:      true,
		Blocks:            true,
		Metadata:          true,
		Assets:            true,
		Minting:           true,
		UTXOs:             true,
		Inputs:            true,
		Outputs:           true,
		Certificates:      true,
		Governance:        true,
		Withdrawals:       true,
		Scripts:           true,
		Collateral:        true,
		WalletConnections: true,
	}
}

func TestNewBlockProcessorConstructors(t *testing.T) {
	require.NotNil(t, processors.NewBlockProcessor(nil, testIndexingConfig()))

	eventChan := make(chan models.TokenHolderEvent, 1)
	require.NotNil(t, processors.NewBlockProcessorWithEvents(nil, testIndexingConfig(), eventChan))
}

func TestGetEraConfigBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		epoch   uint64
		era     string
		workers int
		queue   int
		fetch   int
	}{
		{"byron", 0, "Byron", 16, 100000, 5000},
		{"shelley", 208, "Shelley", 12, 50000, 2000},
		{"mary", 236, "Allegra/Mary", 10, 30000, 1500},
		{"alonzo", 290, "Alonzo", 8, 20000, 1000},
		{"babbage_conway", 365, "Babbage/Conway", 6, 10000, 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := processors.GetEraConfig(tt.epoch)
			assert.Equal(t, tt.era, processors.GetEraName(tt.epoch))
			assert.Equal(t, tt.workers, cfg.WorkerCount)
			assert.Equal(t, tt.queue, cfg.QueueSize)
			assert.Equal(t, tt.fetch, cfg.FetchRangeSize)
		})
	}
}
