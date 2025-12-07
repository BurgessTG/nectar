// Copyright 2025 The Nectar Authors
// Node-to-Client only connection handler

package connection

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	unifiederrors "nectar/errors"
)

// ConnectionMode represents the connection type
type ConnectionMode int

const (
	ModeNodeToNode ConnectionMode = iota
	ModeNodeToClient
)

// ConnectionResult holds the connection details
type ConnectionResult struct {
	Connection    *ouroboros.Connection
	Mode          ConnectionMode
	NetworkMagic  uint32
	HasBlockFetch bool
	ErrorChannel  chan error
}

// getNetworkMagicFromEnv returns the network magic from environment or default
func getNetworkMagicFromEnv() uint32 {
	if magic := os.Getenv("CARDANO_NETWORK_MAGIC"); magic != "" {
		if val, err := strconv.ParseUint(magic, 10, 32); err == nil {
			return uint32(val)
		}
	}
	return 764824073 // Default to mainnet
}

// NodeToClientConnector handles Node-to-Client connections only
type NodeToClientConnector struct {
	socketPath      string
	chainSyncConfig chainsync.Config
	networkMagic    uint32
}

// NewNodeToClientConnector creates a new Node-to-Client connector
func NewNodeToClientConnector(socketPath string, chainSync chainsync.Config) *NodeToClientConnector {
	return &NodeToClientConnector{
		socketPath:      socketPath,
		chainSyncConfig: chainSync,
		networkMagic:    getNetworkMagicFromEnv(),
	}
}

// Connect establishes a Node-to-Client connection
func (nc *NodeToClientConnector) Connect() (*ConnectionResult, error) {
	var conn net.Conn
	var err error

	if strings.Contains(nc.socketPath, ":") {
		conn, err = net.Dial("tcp", nc.socketPath)
	} else {
		conn, err = net.Dial("unix", nc.socketPath)
	}

	if err != nil {
		return nil, fmt.Errorf("connection failed: %w", err)
	}

	errorChan := make(chan error, 10)

	// Create Node-to-Client connection
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(conn),
		ouroboros.WithNetworkMagic(nc.networkMagic),
		ouroboros.WithErrorChan(errorChan),
		ouroboros.WithNodeToNode(false), // Node-to-Client mode
		ouroboros.WithKeepAlive(false),
		ouroboros.WithChainSyncConfig(nc.chainSyncConfig),
	)

	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("node-to-client connection failed: %w", err)
	}

	log.Printf("[OK] Node-to-Client connection established")
	return &ConnectionResult{
		Connection:    oConn,
		Mode:          ModeNodeToClient,
		NetworkMagic:  nc.networkMagic,
		HasBlockFetch: false, // BlockFetch not available in N2C mode
		ErrorChannel:  errorChan,
	}, nil
}

// StartErrorHandler begins handling connection errors
func StartErrorHandler(ctx context.Context, errorChan chan error, callback func(string, string)) {
	go func() {
		for {
			select {
			case err, ok := <-errorChan:
				if !ok {
					return
				}
				if err != nil {
					// Filter out non-critical warnings
					if !isNonCriticalError(err) {
						unifiederrors.Get().LogError(unifiederrors.ErrorTypeConnection, "NodeToClient", "HandleError", err.Error())
						if callback != nil {
							callback("Connection", err.Error())
						}
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// isNonCriticalError determines if an error should be suppressed
func isNonCriticalError(err error) bool {
	if err == nil {
		return true
	}

	errStr := err.Error()

	// Suppress these warnings in production
	nonCritical := []string{
		"version mismatch",
		"handshake failed",
		"protocol negotiation",
	}

	for _, pattern := range nonCritical {
		if strings.Contains(strings.ToLower(errStr), pattern) {
			return true
		}
	}

	return false
}