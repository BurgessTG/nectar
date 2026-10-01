package processors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuantityToSignedDeltaRejectsOverflow(t *testing.T) {
	_, err := quantityToSignedDelta(uint64(maxHolderDelta) + 1)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "quantity exceeds")
}

func TestAddHolderDeltaChecksOverflow(t *testing.T) {
	change := &holderChange{delta: maxHolderDelta}

	err := addHolderDelta(change, 1)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflow")
}

func TestAddHolderDeltaAppliesValidNegativeDelta(t *testing.T) {
	change := &holderChange{delta: 10}

	err := addHolderDelta(change, -4)

	require.NoError(t, err)
	assert.Equal(t, int64(6), change.delta)
}
