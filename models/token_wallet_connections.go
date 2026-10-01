package models

import "gorm.io/gorm"

// TokenWalletConnection tracks token transfers between wallets.
// Similar to WalletConnection (ADA transfers) but for native assets.
// This enables fast O(1) lookups for "which holders have sent tokens to each other".
//
// Key insight: A token transfer occurs when:
//   - An input spends a UTxO containing token X from address A (sender)
//   - An output creates a new UTxO with token X at address B (receiver)
//
// This complements WalletConnection (ADA) for full transfer graph visualization.
type TokenWalletConnection struct {
	// Composite primary key - one row per transfer event
	TxHash    []byte `gorm:"type:VARBINARY(32);not null;primaryKey;index:idx_twc_tx"`
	TxIndex   uint32 `gorm:"type:INT UNSIGNED;not null;primaryKey"` // Output index
	PolicyID  []byte `gorm:"type:VARBINARY(28);not null;primaryKey;index:idx_twc_token,priority:1"`
	AssetName []byte `gorm:"type:VARBINARY(32);not null;primaryKey;index:idx_twc_token,priority:2"`

	// Transfer details
	FromAddress      string `gorm:"type:VARCHAR(256);not null;primaryKey;index:idx_twc_from"`
	ToAddress        string `gorm:"type:VARCHAR(256);not null;primaryKey;index:idx_twc_to"`
	FromStakeAddress string `gorm:"type:VARCHAR(63);index:idx_twc_from_stake"`
	ToStakeAddress   string `gorm:"type:VARCHAR(63);index:idx_twc_to_stake"`
	Quantity         uint64 `gorm:"type:BIGINT UNSIGNED;not null"`

	// Block info for ordering/filtering
	Slot        uint64 `gorm:"type:BIGINT UNSIGNED;not null;index:idx_twc_slot"`
	BlockHeight uint64 `gorm:"type:BIGINT UNSIGNED"`
}

func (TokenWalletConnection) TableName() string {
	return "token_wallet_connections"
}

func (t *TokenWalletConnection) BeforeCreate(tx *gorm.DB) error {
	return nil
}

// TokenTransferEvent represents a real-time event for token transfers.
// Used for pushing updates to the backend via event bus.
type TokenTransferEvent struct {
	EventType   string `json:"EventType"` // "token_transfer"
	PolicyID    string `json:"PolicyID"`  // Hex-encoded
	AssetName   string `json:"AssetName"` // Hex-encoded
	FromAddress string `json:"FromAddress"`
	ToAddress   string `json:"ToAddress"`
	Quantity    uint64 `json:"Quantity"`
	TxHash      string `json:"TxHash"` // Hex-encoded
	Slot        uint64 `json:"Slot"`
}
