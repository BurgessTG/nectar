package processors_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"nectar/processors"
)

func TestEraConfigIsMonotonicAcrossHeavyEras(t *testing.T) {
	shelley := processors.GetEraConfig(208)
	babbage := processors.GetEraConfig(365)

	assert.Greater(t, shelley.WorkerCount, babbage.WorkerCount)
	assert.Greater(t, shelley.QueueSize, babbage.QueueSize)
	assert.Greater(t, shelley.FetchRangeSize, babbage.FetchRangeSize)
	assert.Equal(t, "Shelley", processors.GetEraName(208))
	assert.Equal(t, "Babbage/Conway", processors.GetEraName(365))
}
