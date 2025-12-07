// Copyright 2025 The Nectar Authors
// LocalStateQuery service for ledger state data

package statequery

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"nectar/models"
	"nectar/processors"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"gorm.io/gorm"
)

// Config holds configuration for the state query service
type Config struct {
	SocketPath      string
	NetworkMagic    uint32
	QueryInterval   time.Duration
	MaxRetries      int
	RetryBackoff    time.Duration
	WorkerCount     int
	RewardBatchSize int
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	// Get network magic from environment, default to mainnet
	networkMagic := uint32(764824073) // Mainnet default
	if envMagic := os.Getenv("CARDANO_NETWORK_MAGIC"); envMagic != "" {
		if magic, err := strconv.ParseUint(envMagic, 10, 32); err == nil {
			networkMagic = uint32(magic)
		}
	}

	// Get socket path from environment
	socketPath := os.Getenv("CARDANO_NODE_SOCKET")
	if socketPath == "" {
		socketPath = "/opt/cardano/cnode/sockets/node.socket"
	}

	return Config{
		SocketPath:      socketPath,
		NetworkMagic:    networkMagic,
		QueryInterval:   5 * time.Minute,
		MaxRetries:      3,
		RetryBackoff:    30 * time.Second,
		WorkerCount:     1, // Sequential for state queries
		RewardBatchSize: 1000,
	}
}

// Service handles LocalStateQuery operations
type Service struct {
	config           Config
	db               *gorm.DB
	oConn            *ouroboros.Connection
	lsqClient        *localstatequery.Client
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	mu               sync.RWMutex
	lastEpoch        uint32
	stakeCache       *processors.StakeAddressCache
	rewardCalculator *RewardCalculator
}

// New creates a new LocalStateQuery service
func New(db *gorm.DB, config Config) *Service {
	ctx, cancel := context.WithCancel(context.Background())

	s := &Service{
		config:     config,
		db:         db,
		ctx:        ctx,
		cancel:     cancel,
		stakeCache: processors.NewStakeAddressCache(db),
	}
	s.rewardCalculator = NewRewardCalculator(db, s)
	return s
}

// Start begins the state query service
func (s *Service) Start() error {
	log.Println(" Starting LocalStateQuery service...")

	// Connect to node
	if err := s.connect(); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Start periodic queries
	s.wg.Add(1)
	go s.queryLoop()

	log.Println("[OK] LocalStateQuery service started")
	return nil
}

// Stop gracefully shuts down the service
func (s *Service) Stop() error {
	log.Println(" Stopping LocalStateQuery service...")
	s.cancel()
	s.wg.Wait()

	s.mu.Lock()
	if s.oConn != nil {
		s.oConn.Close()
	}
	s.mu.Unlock()

	return nil
}

// connect establishes connection to the node
func (s *Service) connect() error {
	// Create socket connection
	conn, err := net.Dial("unix", s.config.SocketPath)
	if err != nil {
		return fmt.Errorf("failed to connect to socket: %w", err)
	}

	// Create ouroboros connection with LocalStateQuery
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(conn),
		ouroboros.WithNetworkMagic(s.config.NetworkMagic),
		ouroboros.WithNodeToNode(false), // Node-to-Client for LocalStateQuery
		ouroboros.WithLocalStateQueryConfig(
			localstatequery.NewConfig(),
		),
	)
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to create ouroboros connection: %w", err)
	}

	s.mu.Lock()
	s.oConn = oConn
	s.lsqClient = oConn.LocalStateQuery().Client
	s.mu.Unlock()

	return nil
}

// queryLoop runs periodic queries
func (s *Service) queryLoop() {
	defer s.wg.Done()

	// Initial query
	if err := s.runQueries(); err != nil {
		log.Printf("[WARNING] Initial query failed: %v", err)
	}

	ticker := time.NewTicker(s.config.QueryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.runQueries(); err != nil {
				log.Printf("[WARNING] Query failed: %v", err)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// runQueries executes all state queries
func (s *Service) runQueries() error {
	log.Printf(" State query service: Starting queries...")

	s.mu.RLock()
	client := s.lsqClient
	s.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("no client connection")
	}

	// Get current epoch
	epochNoInt, err := client.GetEpochNo()
	if err != nil {
		return fmt.Errorf("failed to get epoch: %w", err)
	}
	epochNo := uint32(epochNoInt)

	log.Printf(" Running state queries for epoch %d", epochNo)

	// Check if we're connected to a live node while doing historical sync
	// The state query socket might be connected to live mainnet (epoch 500+)
	// while the sync socket is processing Byron (epoch 0-207)
	if epochNo > 207 {
		// Get the actual syncing position from database (using ORDER BY LIMIT 1 for speed)
		var currentSlot uint64
		var currentEpoch uint32
		err := s.db.Model(&models.Block{}).Select("slot_no").Order("slot_no DESC").Limit(1).Scan(&currentSlot).Error
		if err == nil && currentSlot > 0 {
			// Calculate epoch from slot (432000 slots per epoch)
			currentEpoch = uint32(currentSlot / 432000)

			if currentEpoch < 208 && epochNo > 400 {
				// We're syncing Byron but state query sees mainnet tip
				log.Printf("[WARNING] Historical sync detected: DB at epoch %d but state query sees epoch %d", currentEpoch, epochNo)
				log.Printf(" Skipping state queries during historical sync to avoid 'Pool not found' errors")
				return nil
			}
		}
	}

	// Only process if epoch has changed
	if epochNo <= s.lastEpoch {
		log.Printf(" Epoch %d already processed, skipping", epochNo)
		return nil
	}

	// Query stake distribution (only for Shelley era and later)
	// Byron (epochs 0-207) has no stake pools or delegation
	if epochNo >= 208 {
		if err := s.queryStakeDistribution(epochNo); err != nil {
			log.Printf("[WARNING] Failed to query stake distribution: %v", err)
		}
	}

	// Query rewards (only for Shelley era and later)
	// Rewards start after epoch 210 in Shelley
	if epochNo >= 210 {
		if err := s.queryRewards(epochNo); err != nil {
			log.Printf("[WARNING] Failed to query rewards: %v", err)
		}
	}

	// Query treasury/reserve from epoch state (only for Shelley era and later)
	if epochNo >= 208 {
		if err := s.queryTreasuryReserve(epochNo); err != nil {
			log.Printf("[WARNING] Failed to query treasury/reserve: %v", err)
		}
	}

	s.lastEpoch = epochNo
	return nil
}

// queryStakeDistribution queries and stores stake distribution
func (s *Service) queryStakeDistribution(epochNo uint32) error {
	// Skip for Byron era (epochs 0-207)
	if epochNo < 208 {
		log.Printf(" Skipping stake distribution for Byron epoch %d", epochNo)
		return nil
	}

	log.Printf(" Querying stake distribution for epoch %d", epochNo)

	s.mu.RLock()
	client := s.lsqClient
	s.mu.RUnlock()

	// Get stake distribution
	stakeDistribution, err := client.GetStakeDistribution()
	if err != nil {
		return fmt.Errorf("failed to get stake distribution: %w", err)
	}

	// Debug: Check if we're getting stake distribution in Byron
	if epochNo < 208 && len(stakeDistribution.Results) > 0 {
		log.Printf("[WARNING] WARNING: Got %d pool entries for Byron epoch %d - this shouldn't happen!",
			len(stakeDistribution.Results), epochNo)
		return nil // Don't process Byron "pools"
	}

	// Start transaction
	tx := s.db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// Process each pool's stake
	count := 0
	for poolId, stakeInfo := range stakeDistribution.Results {
		// Get pool ID bytes directly (poolId is already a [28]byte array)
		poolBytes := poolId[:]

		// Get pool hash ID
		var poolHash models.PoolHash
		if err := tx.Where("hash_raw = ?", poolBytes).First(&poolHash).Error; err != nil {
			// This is expected during Byron sync - pools don't exist yet
			// Don't log as it creates thousands of warnings
			continue
		}

		// Calculate stake amount from fraction
		// Note: This is simplified - real calculation needs total stake
		// Convert cbor.Rat to big.Rat
		var stakeFraction *big.Rat
		if stakeInfo.StakeFraction != nil {
			// Handle the fraction conversion properly
			stakeFraction = big.NewRat(1, 1) // Placeholder for now
		}
		stakeAmount := s.calculateStakeAmount(stakeFraction)

		// For now, create a single epoch_stake entry per pool
		// In reality, we'd need individual delegator amounts
		// Use a placeholder stake address for now
		placeholderAddr := []byte("placeholder_delegator_addr")
		epochStake := &models.EpochStake{
			PoolHash: poolHash.HashRaw,
			Amount:   int64(stakeAmount),
			EpochNo:  epochNo,
			AddrHash: placeholderAddr, // Placeholder - need actual delegator addresses
		}

		if err := tx.Create(epochStake).Error; err != nil {
			log.Printf("[WARNING] Failed to create epoch stake: %v", err)
			continue
		}
		count++
	}

	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("failed to commit stake distribution: %w", err)
	}

	log.Printf("[OK] Stored %d stake distribution entries for epoch %d", count, epochNo)
	return nil
}

// queryRewards queries and stores rewards
func (s *Service) queryRewards(epochNo uint32) error {
	// Skip for Byron era and early Shelley (rewards start epoch 210)
	if epochNo < 210 {
		log.Printf(" Skipping rewards for epoch %d (rewards start at 210)", epochNo)
		return nil
	}

	log.Printf(" Querying rewards for epoch %d", epochNo)

	// Note: The rewards query is incomplete in gouroboros (TODO #858)
	// For now, we'll create placeholder logic

	// In a real implementation, you would:
	// 1. Get all stake addresses
	// 2. Query rewards for each address
	// 3. Store in the reward table

	log.Printf("[WARNING] Rewards query not fully implemented in gouroboros (TODO #858)")

	// TODO: Implement actual rewards querying from ledger state
	// Rewards calculation will be added when we integrate with the actual reward system

	return nil
}

// queryTreasuryReserve queries treasury and reserve balances
func (s *Service) queryTreasuryReserve(epochNo uint32) error {
	// Skip for Byron era (treasury/reserve start in Shelley)
	if epochNo < 208 {
		log.Printf(" Skipping treasury/reserve for Byron epoch %d", epochNo)
		return nil
	}

	log.Printf(" Querying treasury/reserve for epoch %d", epochNo)

	s.mu.RLock()
	client := s.lsqClient
	s.mu.RUnlock()

	// Try to get protocol parameters which might include treasury info
	protocolParams, err := client.GetCurrentProtocolParams()
	if err != nil {
		log.Printf("[WARNING] Failed to get protocol params: %v", err)
	} else {
		s.processProtocolParams(protocolParams)
	}

	// Try debug epoch state (contains treasury/reserve but returns raw CBOR)
	// Note: This is incomplete in gouroboros (TODO #863)
	debugState, err := client.DebugEpochState()
	if err != nil {
		log.Printf("[WARNING] Failed to get debug epoch state: %v", err)
	} else {
		// Would need CBOR parsing here
		log.Printf(" Debug epoch state type: %T", debugState)
	}

	// Try reward provenance (may contain treasury data)
	// Note: This is incomplete in gouroboros (TODO #866)
	rewardProvenance, err := client.GetRewardProvenance()
	if err != nil {
		log.Printf("[WARNING] Failed to get reward provenance: %v", err)
	} else {
		log.Printf(" Reward provenance type: %T", rewardProvenance)
	}

	log.Printf("[WARNING] Treasury/reserve queries not fully implemented in gouroboros")
	return nil
}

// processProtocolParams extracts any useful data from protocol parameters
func (s *Service) processProtocolParams(params interface{}) {
	switch p := params.(type) {
	case *ledger.ConwayProtocolParameters:
		log.Printf(" Conway protocol params: max tx size=%d, min fee A=%d",
			p.MaxTxSize, p.MinFeeA)
	case *ledger.BabbageProtocolParameters:
		log.Printf(" Babbage protocol params: max tx size=%d, min fee A=%d",
			p.MaxTxSize, p.MinFeeA)
	case *ledger.AlonzoProtocolParameters:
		log.Printf(" Alonzo protocol params: max tx size=%d, min fee A=%d",
			p.MaxTxSize, p.MinFeeA)
	default:
		log.Printf(" Unknown protocol params type: %T", params)
	}
}

// calculateStakeAmount calculates stake amount from fraction
func (s *Service) calculateStakeAmount(fraction *big.Rat) uint64 {
	// This is simplified - in reality you'd need the total stake
	// to calculate: amount = fraction * totalStake

	// For now, return a placeholder
	// Total ADA supply is ~45 billion, so total stake is less
	totalStake := uint64(35_000_000_000_000_000) // 35B ADA in lovelace

	if fraction == nil {
		return 0
	}

	// Calculate: amount = fraction * totalStake
	numerator := new(big.Int).Mul(fraction.Num(), big.NewInt(int64(totalStake)))
	amount := new(big.Int).Div(numerator, fraction.Denom())

	return amount.Uint64()
}

// CalculateEpochRewards calculates rewards for an epoch
func (s *Service) CalculateEpochRewards(epochNo uint32) error {
	if s.rewardCalculator == nil {
		return fmt.Errorf("reward calculator not initialized")
	}
	return s.rewardCalculator.CalculateEpochRewards(epochNo)
}

// ProcessRefunds processes refunds for an epoch
func (s *Service) ProcessRefunds(epochNo uint32) error {
	if s.rewardCalculator == nil {
		return fmt.Errorf("reward calculator not initialized")
	}
	return s.rewardCalculator.ProcessRefunds(epochNo)
}

// Helper method to process MIR certificates for treasury/reserve
func (s *Service) ProcessMIRCertificate(tx *gorm.DB, mir *common.MoveInstantaneousRewardsCertificate, txHash []byte, certIndex uint32) error {
	// This would be called from certificate processor when MIR certs are found

	pot := "reserves"
	if mir.Reward.Source == 2 {
		pot = "treasury"
	}

	// Check if this is a pot-to-pot transfer
	if mir.Reward.OtherPot > 0 {
		// This affects treasury/reserve balances
		// Use a system stake address placeholder
		systemAddr := []byte("system_treasury_reserve")

		if pot == "treasury" {
			// Transfer from treasury
			treasury := &models.Treasury{
				TxHash:           txHash,
				CertIndex:        certIndex,
				Amount:           mir.Reward.OtherPot,
				StakeAddressHash: systemAddr,
			}
			if err := tx.Create(treasury).Error; err != nil {
				return err
			}
		} else {
			// Transfer from reserves
			reserve := &models.Reserve{
				TxHash:           txHash,
				CertIndex:        certIndex,
				Amount:           mir.Reward.OtherPot,
				StakeAddressHash: systemAddr,
			}
			if err := tx.Create(reserve).Error; err != nil {
				return err
			}
		}
	}

	return nil
}
