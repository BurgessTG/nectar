package database

import (
	"fmt"
	"log"
	"strings"
	"time"

	"gorm.io/gorm"
)

// RetryConfig holds retry configuration
type RetryConfig struct {
	MaxRetries   int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Factor       float64
}

// DefaultRetryConfig provides sensible defaults
var DefaultRetryConfig = RetryConfig{
	MaxRetries:   3,
	InitialDelay: 100 * time.Millisecond,
	MaxDelay:     5 * time.Second,
	Factor:       2.0,
}

// IsRetryableError checks if an error is retryable
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// Connection errors
	if strings.Contains(errStr, "bad connection") ||
		strings.Contains(errStr, "invalid connection") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "deadlock") ||
		strings.Contains(errStr, "commands out of sync") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection closed") {
		return true
	}

	// TiDB specific retryable errors
	if strings.Contains(errStr, "Write conflict") ||
		strings.Contains(errStr, "Information schema is changed") ||
		strings.Contains(errStr, "Region is unavailable") ||
		strings.Contains(errStr, "tikv is busy") ||
		strings.Contains(errStr, "TiKV server timeout") ||
		strings.Contains(errStr, "txn too large") {
		return true
	}

	// Transaction related errors that might be retryable
	if strings.Contains(errStr, "Error 1213") || // Deadlock
		strings.Contains(errStr, "Error 1205") || // Lock wait timeout
		strings.Contains(errStr, "Error 2013") || // Lost connection
		strings.Contains(errStr, "Error 2006") { // Server has gone away
		return true
	}

	// Context/transaction state errors - need fresh transaction
	if strings.Contains(errStr, "already been committed or rolled back") ||
		strings.Contains(errStr, "context canceled") ||
		strings.Contains(errStr, "context deadline exceeded") {
		return true
	}

	return false
}

// RetryTransaction executes a function within a transaction with retry logic
func RetryTransaction(db *gorm.DB, fn func(*gorm.DB) error) error {
	config := DefaultRetryConfig
	delay := config.InitialDelay

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		err := db.Transaction(fn)

		if err == nil {
			return nil
		}

		if !IsRetryableError(err) {
			return err
		}

		if attempt < config.MaxRetries {
			log.Printf("[RETRY] Attempt %d/%d failed: %v. Retrying in %v...",
				attempt+1, config.MaxRetries, err, delay)
			time.Sleep(delay)

			// Exponential backoff
			delay = time.Duration(float64(delay) * config.Factor)
			if delay > config.MaxDelay {
				delay = config.MaxDelay
			}
		}
	}

	return fmt.Errorf("transaction failed after %d retries", config.MaxRetries)
}

// RetryOperation executes a function with retry logic (no transaction)
func RetryOperation(fn func() error) error {
	config := DefaultRetryConfig
	delay := config.InitialDelay

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		err := fn()

		if err == nil {
			return nil
		}

		if !IsRetryableError(err) {
			return err
		}

		if attempt < config.MaxRetries {
			log.Printf("[RETRY] Attempt %d/%d failed: %v. Retrying in %v...",
				attempt+1, config.MaxRetries, err, delay)
			time.Sleep(delay)

			// Exponential backoff
			delay = time.Duration(float64(delay) * config.Factor)
			if delay > config.MaxDelay {
				delay = config.MaxDelay
			}
		}
	}

	return fmt.Errorf("operation failed after %d retries", config.MaxRetries)
}
