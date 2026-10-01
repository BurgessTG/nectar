package models

import "gorm.io/gorm"

// TokenHolder represents current token holders with their balances.
// This table is updated incrementally as blocks are processed.
// Primary key is (policy, name, address) - one row per holder per token.
type TokenHolder struct {
	// Composite primary key
	Policy  []byte `gorm:"type:VARBINARY(28);not null;primaryKey;index:idx_token_holder_pk,priority:1"`
	Name    []byte `gorm:"type:VARBINARY(32);not null;primaryKey;index:idx_token_holder_pk,priority:2"`
	Address string `gorm:"type:VARCHAR(256);not null;primaryKey;index:idx_token_holder_pk,priority:3"`

	// Balance and metadata
	Amount          uint64 `gorm:"type:BIGINT UNSIGNED;not null;index:idx_top_holders,priority:3"`
	TxHash          []byte `gorm:"type:VARBINARY(32)"` // Last tx that updated this holder
	OutputIndex     uint32 `gorm:"type:INT UNSIGNED"`
	StakeAddress    string `gorm:"type:VARCHAR(63);index:idx_stake_address"`
	LastUpdatedSlot uint64 `gorm:"type:BIGINT UNSIGNED;index:idx_last_updated"`
}

func (TokenHolder) TableName() string {
	return "token_holders"
}

// BeforeCreate hook - consistent with other models in the codebase
func (t *TokenHolder) BeforeCreate(tx *gorm.DB) error {
	return nil
}

// TokenHolderEvent represents a real-time event for holder changes.
// Used for pushing updates to the backend via event bus.
// JSON tags match the Go struct field names for consistency with backend parsing.
// Policy and TxHash are hex strings (not []byte) for proper JSON serialization.
type TokenHolderEvent struct {
	EventType string `json:"EventType"` // "balance_increased", "balance_decreased"
	Policy    string `json:"Policy"`    // Hex-encoded policy ID
	Name      string `json:"Name"`      // Hex-encoded asset name
	Address   string `json:"Address"`
	OldAmount uint64 `json:"OldAmount"`
	NewAmount uint64 `json:"NewAmount"`
	TxHash    string `json:"TxHash"` // Hex-encoded tx hash
	Slot      uint64 `json:"Slot"`
}
