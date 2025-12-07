package database

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	// DefaultTiDBDSN is used only if TIDB_DSN environment variable is not set
	// In production, always set TIDB_DSN with proper credentials
	// Note: tx_isolation removed from DSN - set at session level to avoid mid-transaction conflicts
	// Added interpolateParams=true to reduce round trips and improve performance
	DefaultTiDBDSN = "root:@tcp(127.0.0.1:4000)/nectar?charset=utf8mb4&parseTime=True&loc=Local&timeout=60s&readTimeout=60s&writeTimeout=300s&maxAllowedPacket=67108864&autocommit=true&interpolateParams=true"
	// Deprecated: Use NECTAR_DSN environment variable instead
	NectarDBDSN = "root:@tcp(127.0.0.1:4000)/nectar?charset=utf8mb4&parseTime=True&loc=Local&timeout=60s&readTimeout=60s&writeTimeout=300s&maxAllowedPacket=67108864&autocommit=true&interpolateParams=true"
)

// InitTiDB initializes the TiDB connection with optimizations
func InitTiDB() (*gorm.DB, error) {
	// Configure MySQL driver with custom error handling
	ConfigureMySQLDriver()

	// Get DSN from environment or use default
	baseDSN := os.Getenv("TIDB_DSN")
	if baseDSN == "" {
		// Log warning if using default DSN
		log.Println("WARNING: TIDB_DSN not set, using default DSN without password. Set TIDB_DSN in production!")
		baseDSN = DefaultTiDBDSN
	}

	// First, connect without specifying a database
	baseDB, err := gorm.Open(mysql.Open(baseDSN), &gorm.Config{
		Logger: NewGormLogger(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to TiDB cluster: %w", err)
	}

	// Create the nectar database if it doesn't exist
	if err := baseDB.Exec("CREATE DATABASE IF NOT EXISTS nectar").Error; err != nil {
		return nil, fmt.Errorf("failed to create nectar database: %w", err)
	}

	// Close the base connection
	sqlDB, err := baseDB.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get underlying sql.DB: %w", err)
	}
	sqlDB.Close()

	// Now connect to the nectar database
	nectarDSN := os.Getenv("NECTAR_DSN")
	if nectarDSN == "" {
		// Try TIDB_DSN as fallback
		nectarDSN = os.Getenv("TIDB_DSN")
		if nectarDSN == "" {
			log.Println("WARNING: NECTAR_DSN not set, using default DSN without password. Set NECTAR_DSN in production!")
			nectarDSN = NectarDBDSN
		}
	}

	db, err := gorm.Open(mysql.Open(nectarDSN), &gorm.Config{
		Logger:                                   NewGormLogger(),
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nectar database: %w", err)
	}

	// Get underlying sql.DB to configure connection pool
	sqlDB, err = db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get underlying sql.DB: %w", err)
	}

	// Configure connection pool for high-performance
	// Get pool settings from environment or use defaults
	poolSize := 16 // Default from env
	if envPool := os.Getenv("DB_CONNECTION_POOL"); envPool != "" {
		var size int
		fmt.Sscanf(envPool, "%d", &size)
		if size > 0 {
			poolSize = size
		}
	}

	// Reduced multipliers to prevent connection exhaustion in Shelley era
	sqlDB.SetMaxIdleConns(poolSize)     // Same as pool size
	sqlDB.SetMaxOpenConns(poolSize * 2) // Only 2x pool size for max open
	sqlDB.SetConnMaxLifetime(30 * time.Minute) // Shorter lifetime to prevent stale connections
	sqlDB.SetConnMaxIdleTime(10 * time.Minute) // Shorter idle time

	// Ensure no transaction is active before setting session variables
	if err := db.Exec("ROLLBACK").Error; err != nil {
		// Ignore error - there might not be a transaction
	}
	
	// Enable TiDB-specific optimizations
	if err := enableTiDBOptimizations(db); err != nil {
		log.Printf("Warning: failed to enable some TiDB optimizations: %v", err)
	}

	log.Println("TiDB connection established with performance optimizations")
	return db, nil
}

// AutoMigrate runs GORM auto-migration for all models
func AutoMigrate(db *gorm.DB) error {
	log.Println("Starting database migrations...")

	// Disable foreign key checks during migration
	if err := db.Exec("SET foreign_key_checks = 0").Error; err != nil {
		log.Printf("Warning: could not disable foreign key checks: %v", err)
	}

	// Create all tables without foreign key constraints first
	if err := createAllTablesWithoutForeignKeys(db); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// Re-enable foreign key checks
	if err := db.Exec("SET foreign_key_checks = 1").Error; err != nil {
		log.Printf("Warning: could not re-enable foreign key checks: %v", err)
	}

	// Apply TiDB-specific optimizations after migration
	if err := applyTiDBOptimizations(db); err != nil {
		log.Printf("Warning: failed to apply TiDB optimizations: %v", err)
	}
	
	// Create TiFlash replicas for analytical queries
	if err := CreateTiFlashReplicas(db); err != nil {
		log.Printf("Warning: failed to create TiFlash replicas: %v", err)
	}

	// Create all indexes using unified index manager
	indexManager := NewUnifiedIndexManager(db)
	if err := indexManager.CreateAllIndexes(); err != nil {
		log.Printf("Warning: failed to create indexes: %v", err)
	}
	
	// Analyze tables for query optimization
	if err := indexManager.AnalyzeTables(); err != nil {
		log.Printf("Warning: failed to analyze tables: %v", err)
	}

	log.Println("Database migrations completed successfully")
	return nil
}

// applyTiDBOptimizations applies TiDB-specific optimizations
func applyTiDBOptimizations(db *gorm.DB) error {
	log.Println("Applying TiDB-specific optimizations...")
	// Indexes are now created by unified_indexes.go to avoid duplication
	log.Println("TiDB optimizations applied")
	return nil
}

// enableTiDBOptimizations applies TiDB-specific session optimizations
func enableTiDBOptimizations(db *gorm.DB) error {
	optimizations := []string{
		// CRITICAL: Set transaction isolation at session level (not per-transaction)
		// This avoids "Transaction characteristics can't be changed while a transaction is in progress" errors
		"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",

		// Batch operation optimizations
		"SET SESSION tidb_batch_insert = ON",
		"SET SESSION tidb_batch_delete = ON",
		"SET SESSION tidb_batch_commit = ON",
		"SET SESSION tidb_dml_batch_size = 20000",
		"SET SESSION tidb_max_chunk_size = 1024",

		// Skip unnecessary checks
		"SET SESSION tidb_skip_utf8_check = ON",
		"SET SESSION tidb_constraint_check_in_place = OFF",

		// Enable compression for new tables
		"SET SESSION tidb_enable_table_partition = ON",
		"SET SESSION tidb_row_format_version = 2",

		// Memory optimizations
		"SET SESSION tidb_mem_quota_query = 4294967296", // 4GB per query (reduced from 16GB)
		"SET SESSION tidb_enable_chunk_rpc = ON",

		// Parallel execution
		"SET SESSION tidb_distsql_scan_concurrency = 30",
		"SET SESSION tidb_executor_concurrency = 10",

		// Index optimizations
		"SET SESSION tidb_opt_insubq_to_join_and_agg = ON",
		"SET SESSION tidb_enable_index_merge = ON",

		// Transaction optimizations
		"SET SESSION tidb_enable_async_commit = ON",
		"SET SESSION tidb_enable_1pc = ON",

		// Additional optimizations for high-throughput processing
		"SET SESSION tidb_enable_vectorized_expression = ON",
		"SET SESSION tidb_projection_concurrency = 8",
		"SET SESSION tidb_hash_join_concurrency = 8",
		"SET SESSION tidb_enable_parallel_apply = ON",
		// Note: tidb_enable_batch_dml is a GLOBAL variable, skip it for session
	}

	for _, opt := range optimizations {
		if err := db.Exec(opt).Error; err != nil {
			log.Printf("Warning: TiDB optimization failed (%s): %v", opt, err)
		}
	}

	return nil
}



// CheckDatabaseConnection verifies the database connection is healthy
func CheckDatabaseConnection(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying sql.DB: %w", err)
	}

	// First try a simple ping
	if err := sqlDB.Ping(); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	// Also verify we can execute a simple query
	var result int
	if err := db.Raw("SELECT 1").Scan(&result).Error; err != nil {
		return fmt.Errorf("health check query failed: %w", err)
	}

	// Ensure no transaction is stuck
	if err := db.Exec("ROLLBACK").Error; err != nil {
		// Ignore error - there might not be a transaction
	}

	return nil
}

// ResetConnection resets a database connection to clear any bad state
func ResetConnection(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying sql.DB: %w", err)
	}

	// Close all connections to clear any bad state
	sqlDB.Close()

	// The next query will automatically create new connections
	// Verify the connection is working
	if err := sqlDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping after reset: %w", err)
	}

	return nil
}

// EnsureHealthyConnection ensures the connection is healthy, resetting if needed
func EnsureHealthyConnection(db *gorm.DB) error {
	// First try a simple ping
	if err := CheckDatabaseConnection(db); err == nil {
		return nil
	}

	// Connection is unhealthy, try to reset it
	log.Println("[WARNING] Database connection unhealthy, attempting reset...")
	if err := ResetConnection(db); err != nil {
		return fmt.Errorf("failed to reset connection: %w", err)
	}

	log.Println("[OK] Database connection reset successfully")
	return nil
}

// ConnectionPoolManager manages dedicated database connections for workers
type ConnectionPoolManager struct {
	connections []*gorm.DB
	mutex       sync.RWMutex
	config      *ConnectionPoolConfig
}

// ConnectionPoolConfig holds configuration for the connection pool
type ConnectionPoolConfig struct {
	DSN              string
	WorkerCount      int
	MaxIdleConns     int
	MaxOpenConns     int
	ConnMaxLifetime  time.Duration
	ConnMaxIdleTime  time.Duration
	HealthCheckInterval time.Duration
}

// NewConnectionPoolManager creates a new connection pool manager
func NewConnectionPoolManager(config *ConnectionPoolConfig) (*ConnectionPoolManager, error) {
	if config == nil {
		return nil, fmt.Errorf("connection pool config is required")
	}

	manager := &ConnectionPoolManager{
		connections: make([]*gorm.DB, 0, config.WorkerCount),
		config:      config,
	}

	// Create dedicated connections for each worker
	for i := 0; i < config.WorkerCount; i++ {
		conn, err := manager.createConnection(i)
		if err != nil {
			// Clean up any connections we already created
			manager.Cleanup()
			return nil, fmt.Errorf("failed to create connection %d: %w", i, err)
		}
		manager.connections = append(manager.connections, conn)
	}

	// Start health check routine if interval is specified
	if config.HealthCheckInterval > 0 {
		go manager.startHealthChecker()
	}

	log.Printf("ConnectionPoolManager: Created %d dedicated database connections", config.WorkerCount)
	return manager, nil
}

// GetConnection returns a dedicated connection for a worker
func (cpm *ConnectionPoolManager) GetConnection(workerID int) (*gorm.DB, error) {
	cpm.mutex.RLock()
	defer cpm.mutex.RUnlock()

	if workerID < 0 || workerID >= len(cpm.connections) {
		return nil, fmt.Errorf("invalid worker ID %d (must be 0-%d)", workerID, len(cpm.connections)-1)
	}

	conn := cpm.connections[workerID]
	if conn == nil {
		return nil, fmt.Errorf("connection %d is nil", workerID)
	}

	return conn, nil
}

// createConnection creates a new database connection for a worker
func (cpm *ConnectionPoolManager) createConnection(workerID int) (*gorm.DB, error) {
	// Ensure MySQL driver is configured
	ConfigureMySQLDriver()

	// CRITICAL: Disable connection pooling completely for worker connections
	// Each worker needs a truly dedicated connection to avoid "commands out of sync" errors
	// We add unique connection attributes to ensure MySQL treats each as separate
	dsnWithWorkerID := cpm.config.DSN
	if strings.Contains(dsnWithWorkerID, "?") {
		dsnWithWorkerID += fmt.Sprintf("&connectionAttributes=worker_id:%d,pid:%d", workerID, os.Getpid())
	} else {
		dsnWithWorkerID += fmt.Sprintf("?connectionAttributes=worker_id:%d,pid:%d", workerID, os.Getpid())
	}

	db, err := gorm.Open(mysql.Open(dsnWithWorkerID), &gorm.Config{
		Logger:                                   NewGormLogger(),
		DisableForeignKeyConstraintWhenMigrating: true,
		SkipDefaultTransaction:                   true,  // Performance optimization
		PrepareStmt:                              false, // DISABLE prepared statements to avoid connection state issues
		ConnPool:                                 nil,   // Ensure no connection pooling
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Get underlying sql.DB to configure connection pool
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get underlying sql.DB: %w", err)
	}

	// CRITICAL: Disable ALL connection pooling for worker connections
	// This ensures each worker has exactly ONE dedicated connection
	sqlDB.SetMaxIdleConns(1) // Exactly 1 connection
	sqlDB.SetMaxOpenConns(1) // Exactly 1 connection - no pooling!
	sqlDB.SetConnMaxLifetime(0) // Never expire (we manage lifecycle)
	sqlDB.SetConnMaxIdleTime(0) // Never idle timeout

	// Ensure no transaction is active before setting session variables
	if err := db.Exec("ROLLBACK").Error; err != nil {
		// Ignore error - there might not be a transaction
	}
	
	// Enable TiDB-specific optimizations
	if err := enableTiDBOptimizations(db); err != nil {
		log.Printf("[WARNING] Worker %d: failed to enable some TiDB optimizations: %v", workerID, err)
	}

	// Set a unique connection ID to help with debugging
	if err := db.Exec(fmt.Sprintf("SET @worker_id = %d", workerID)).Error; err != nil {
		log.Printf("[WARNING] Worker %d: failed to set worker_id variable: %v", workerID, err)
	}

	log.Printf("ConnectionPoolManager: Created DEDICATED connection for worker %d (no pooling)", workerID)
	return db, nil
}

// RecoverConnection attempts to recover a failed connection
func (cpm *ConnectionPoolManager) RecoverConnection(workerID int) error {
	cpm.mutex.Lock()
	defer cpm.mutex.Unlock()

	if workerID < 0 || workerID >= len(cpm.connections) {
		return fmt.Errorf("invalid worker ID %d", workerID)
	}

	// Close the old connection if it exists
	if oldConn := cpm.connections[workerID]; oldConn != nil {
		if sqlDB, err := oldConn.DB(); err == nil {
			sqlDB.Close()
		}
	}

	// Create a new connection
	newConn, err := cpm.createConnection(workerID)
	if err != nil {
		return fmt.Errorf("failed to create replacement connection: %w", err)
	}

	cpm.connections[workerID] = newConn
	log.Printf("ConnectionPoolManager: Recovered connection for worker %d", workerID)
	return nil
}

// startHealthChecker runs periodic health checks on all connections
func (cpm *ConnectionPoolManager) startHealthChecker() {
	ticker := time.NewTicker(cpm.config.HealthCheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		cpm.performHealthChecks()
	}
}

// performHealthChecks checks all connections and attempts recovery if needed
func (cpm *ConnectionPoolManager) performHealthChecks() {
	cpm.mutex.RLock()
	connectionCount := len(cpm.connections)
	cpm.mutex.RUnlock()

	for i := 0; i < connectionCount; i++ {
		conn, err := cpm.GetConnection(i)
		if err != nil {
			log.Printf("[WARNING] ConnectionPoolManager: Worker %d has no connection", i)
			cpm.RecoverConnection(i)
			continue
		}

		// Check connection health
		if err := CheckDatabaseConnection(conn); err != nil {
			log.Printf("[WARNING] ConnectionPoolManager: Worker %d connection unhealthy: %v", i, err)
			if err := cpm.RecoverConnection(i); err != nil {
				log.Printf("[ERROR] ConnectionPoolManager: Failed to recover worker %d connection: %v", i, err)
			}
		}
	}
}

// Cleanup closes all connections
func (cpm *ConnectionPoolManager) Cleanup() {
	cpm.mutex.Lock()
	defer cpm.mutex.Unlock()

	for i, conn := range cpm.connections {
		if conn != nil {
			if sqlDB, err := conn.DB(); err == nil {
				sqlDB.Close()
			}
			log.Printf("ConnectionPoolManager: Closed connection for worker %d", i)
		}
	}
	cpm.connections = nil
}

// GetDefaultConnectionPoolConfig returns the default configuration
func GetDefaultConnectionPoolConfig() *ConnectionPoolConfig {
	// Get DSN from environment
	dsn := os.Getenv("NECTAR_DSN")
	if dsn == "" {
		dsn = os.Getenv("TIDB_DSN")
		if dsn == "" {
			dsn = NectarDBDSN
		}
	}

	// Get worker count from environment
	workerCount := 8
	if envWorkers := os.Getenv("WORKER_COUNT"); envWorkers != "" {
		if count, err := strconv.Atoi(envWorkers); err == nil && count > 0 {
			workerCount = count
		}
	}

	return &ConnectionPoolConfig{
		DSN:                 dsn,
		WorkerCount:         workerCount,
		MaxIdleConns:        1, // MUST be 1 for dedicated connections
		MaxOpenConns:        1, // MUST be 1 to prevent pooling
		ConnMaxLifetime:     0, // No expiration - we manage lifecycle
		ConnMaxIdleTime:     0, // No idle timeout
		HealthCheckInterval: 30 * time.Second,
	}
}

// FastCreate performs a create operation optimized for TiDB
// It avoids the slow ON DUPLICATE KEY UPDATE pattern
func FastCreate(tx *gorm.DB, value interface{}) error {
	// Use GORM's Clauses to generate INSERT IGNORE
	if err := tx.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(value).Error; err != nil {
		// Log any real errors (not duplicates)
		if !strings.Contains(err.Error(), "Duplicate entry") {
			return err
		}
	}
	return nil
}

// FastCreateInBatches performs batch create optimized for TiDB
// It avoids the slow ON DUPLICATE KEY UPDATE pattern
func FastCreateInBatches(tx *gorm.DB, value interface{}, batchSize int) error {
	// Set TiDB optimizations for this operation
	optimizedTx := tx.Session(&gorm.Session{
		SkipHooks: true,
		PrepareStmt: false,
	})
	
	// Set batch processing hints
	optimizedTx.Exec("SET SESSION tidb_dml_batch_size = ?", batchSize*2)
	
	// Try batch create with INSERT IGNORE
	if err := optimizedTx.Clauses(clause.Insert{Modifier: "IGNORE"}).CreateInBatches(value, batchSize).Error; err != nil {
		// If it's a duplicate entry error, that's OK
		if strings.Contains(err.Error(), "Duplicate entry") {
			return nil
		}
		return err
	}
	return nil
}
