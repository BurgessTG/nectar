package database_test

import (
	"errors"
	"nectar/database"
	"testing"
	"time"
)

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"broken pipe", errors.New("write: broken pipe"), true},
		{"connection reset", errors.New("read: connection reset by peer"), true},
		{"TiDB write conflict", errors.New("Error 9007: Write conflict"), true},
		{"region unavailable", errors.New("Region is unavailable"), true},
		{"TiKV server busy", errors.New("tikv is busy"), true},
		{"deadlock", errors.New("Error 1213: Deadlock found"), true},
		{"lock wait timeout", errors.New("Error 1205: Lock wait timeout exceeded"), true},
		{"non-retryable error", errors.New("syntax error"), false},
		{"permission denied", errors.New("permission denied"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := database.IsRetryableError(tt.err)
			if result != tt.expected {
				t.Errorf("IsRetryableError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

func TestRetryOperation(t *testing.T) {
	t.Run("successful operation", func(t *testing.T) {
		callCount := 0
		err := database.RetryOperation(func() error {
			callCount++
			return nil
		})

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if callCount != 1 {
			t.Errorf("Expected 1 call, got %d", callCount)
		}
	})

	t.Run("retryable error then success", func(t *testing.T) {
		callCount := 0
		err := database.RetryOperation(func() error {
			callCount++
			if callCount < 3 {
				return errors.New("connection reset by peer")
			}
			return nil
		})

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if callCount != 3 {
			t.Errorf("Expected 3 calls, got %d", callCount)
		}
	})

	t.Run("non-retryable error", func(t *testing.T) {
		callCount := 0
		expectedErr := errors.New("syntax error")
		err := database.RetryOperation(func() error {
			callCount++
			return expectedErr
		})

		if err != expectedErr {
			t.Errorf("Expected %v, got %v", expectedErr, err)
		}
		if callCount != 1 {
			t.Errorf("Expected 1 call, got %d", callCount)
		}
	})

	t.Run("max retries exceeded", func(t *testing.T) {
		callCount := 0
		err := database.RetryOperation(func() error {
			callCount++
			return errors.New("connection reset by peer")
		})

		if err == nil {
			t.Error("Expected error after max retries")
		}
		if callCount != 4 {
			t.Errorf("Expected 4 calls, got %d", callCount)
		}
	})
}

func TestRetryTransaction(t *testing.T) {
	t.Skip("Skipping RetryTransaction tests - requires real database connection")
}

func TestExponentialBackoff(t *testing.T) {
	var attempts []time.Time
	err := database.RetryOperation(func() error {
		attempts = append(attempts, time.Now())
		if len(attempts) < 4 {
			return errors.New("connection reset")
		}
		return nil
	})

	if err != nil {
		t.Errorf("Expected operation to eventually succeed, got: %v", err)
	}
	if len(attempts) != 4 {
		t.Errorf("Expected 4 attempts (1 initial + 3 retries), got %d", len(attempts))
	}

	expectedDelays := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
	}

	for i := 1; i < len(attempts); i++ {
		actualDelay := attempts[i].Sub(attempts[i-1])
		expectedDelay := expectedDelays[i-1]
		tolerance := 50 * time.Millisecond
		if actualDelay < expectedDelay-tolerance || actualDelay > expectedDelay+tolerance {
			t.Errorf("Retry %d: expected delay ~%v, got %v", i, expectedDelay, actualDelay)
		}
	}
}

func BenchmarkRetryOperation_Success(b *testing.B) {
	for i := 0; i < b.N; i++ {
		database.RetryOperation(func() error {
			return nil
		})
	}
}

func BenchmarkRetryOperation_OneRetry(b *testing.B) {
	for i := 0; i < b.N; i++ {
		callCount := 0
		database.RetryOperation(func() error {
			callCount++
			if callCount == 1 {
				return errors.New("connection reset")
			}
			return nil
		})
	}
}
