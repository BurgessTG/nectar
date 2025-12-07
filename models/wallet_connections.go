package models

import "gorm.io/gorm"

// WalletConnection tracks pre-computed wallet-to-wallet transfer relationships.
// This table is populated during block processing to enable fast O(1) lookups
// instead of expensive O(n²) JOIN queries at query time.
//
// Key insight: We track when wallet A sends ADA to wallet B.
// This is determined by: input's spent UTxO address (sender) → output address (receiver)
type WalletConnection struct {
	SenderAddress   string `gorm:"type:VARCHAR(256);primaryKey;index:idx_wc_sender"`
	ReceiverAddress string `gorm:"type:VARCHAR(256);primaryKey;index:idx_wc_receiver"`
	TotalTxCount    uint32 `gorm:"type:INT UNSIGNED;not null;default:1"`
	TotalAdaSent    uint64 `gorm:"type:BIGINT UNSIGNED;not null;default:0"`
	FirstTxSlot     uint64 `gorm:"type:BIGINT UNSIGNED;not null"`
	LastTxSlot      uint64 `gorm:"type:BIGINT UNSIGNED;not null;index:idx_wc_last_slot"`
	LastTxHash      []byte `gorm:"type:VARBINARY(32);not null"`
}

func (WalletConnection) TableName() string {
	return "wallet_connections"
}

// WalletConnectionTx stores individual transaction details for timeline animation.
// This allows the frontend to show the history of transfers between two wallets.
type WalletConnectionTx struct {
	ID              uint64 `gorm:"primaryKey;autoIncrement"`
	SenderAddress   string `gorm:"type:VARCHAR(256);not null;index:idx_wctx_sender"`
	ReceiverAddress string `gorm:"type:VARCHAR(256);not null;index:idx_wctx_receiver"`
	TxHash          []byte `gorm:"type:VARBINARY(32);not null;index:idx_wctx_tx"`
	SlotNo          uint64 `gorm:"type:BIGINT UNSIGNED;not null;index:idx_wctx_slot"`
	AdaAmount       uint64 `gorm:"type:BIGINT UNSIGNED;not null"`
}

func (WalletConnectionTx) TableName() string {
	return "wallet_connection_txs"
}

// BeforeCreate hooks - no-op since we don't need ID management
func (w *WalletConnection) BeforeCreate(tx *gorm.DB) error    { return nil }
func (w *WalletConnectionTx) BeforeCreate(tx *gorm.DB) error { return nil }
