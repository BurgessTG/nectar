package events

import (
	"encoding/json"
	"fmt"
	"log"
	"nectar/models"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// EventBus handles publishing events to subscribers (backend services).
// Thread-safe for concurrent use across multiple goroutines.
type EventBus struct {
	subscribers map[string]net.Conn
	eventChan   chan models.TokenHolderEvent
	mu          sync.RWMutex
	listener    net.Listener
	running     atomic.Bool   // Atomic for thread-safe access
	stopChan    chan struct{} // Signal channel for clean shutdown

	// Metrics for monitoring backpressure
	eventsPublished atomic.Uint64 // Total events successfully queued
	eventsDropped   atomic.Uint64 // Events dropped due to full channel
	broadcastErrors atomic.Uint64 // Failed sends to subscribers
}

// NewEventBus creates a new event bus with the specified buffer size.
// The buffer size determines how many events can be queued before blocking.
func NewEventBus(bufferSize int) *EventBus {
	return &EventBus{
		subscribers: make(map[string]net.Conn),
		eventChan:   make(chan models.TokenHolderEvent, bufferSize),
		stopChan:    make(chan struct{}),
	}
}

// GetEventChannel returns the channel for processors to send events
func (eb *EventBus) GetEventChannel() chan<- models.TokenHolderEvent {
	return eb.eventChan
}

// Start begins listening for subscribers and broadcasting events.
// This is non-blocking - it spawns goroutines for accepting connections
// and broadcasting events.
func (eb *EventBus) Start(address string) error {
	var err error
	eb.listener, err = net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to start event bus listener: %w", err)
	}

	eb.running.Store(true)
	log.Printf("[EVENT_BUS] Listening for subscribers on %s", address)

	// Accept subscriber connections
	go eb.acceptSubscribers()

	// Broadcast events to subscribers
	go eb.broadcastEvents()

	// Periodic stats logging for monitoring
	go eb.logStats()

	return nil
}

// logStats periodically logs event bus statistics for monitoring
func (eb *EventBus) logStats() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	var lastPublished, lastDropped uint64

	for {
		select {
		case <-eb.stopChan:
			return
		case <-ticker.C:
			stats := eb.Stats()

			// Calculate rates since last check
			newPublished := stats.EventsPublished - lastPublished
			newDropped := stats.EventsDropped - lastDropped
			lastPublished = stats.EventsPublished
			lastDropped = stats.EventsDropped

			// Only log if there's activity or issues
			if newPublished > 0 || newDropped > 0 || stats.BroadcastErrors > 0 {
				log.Printf("[EVENT_BUS] Stats: subscribers=%d, published=%d/min, dropped=%d/min, errors=%d, queue=%d/%d",
					stats.Subscribers, newPublished, newDropped, stats.BroadcastErrors,
					stats.ChannelSize, stats.ChannelCap)
			}

			// Alert if dropping too many events
			if newDropped > 100 {
				log.Printf("[EVENT_BUS] WARNING: High event drop rate (%d/min). Consider increasing buffer size or reducing event volume.", newDropped)
			}

			// Alert if no subscribers
			if stats.Subscribers == 0 && newPublished > 0 {
				log.Printf("[EVENT_BUS] WARNING: No subscribers connected. Events are being published but not delivered.")
			}
		}
	}
}

// Stop shuts down the event bus gracefully.
// Safe to call multiple times.
func (eb *EventBus) Stop() {
	// Only stop once - atomic swap returns true if this is the first call
	if !eb.running.CompareAndSwap(true, false) {
		return // Already stopped
	}

	// Signal shutdown to goroutines
	close(eb.stopChan)

	// Close listener to unblock Accept()
	if eb.listener != nil {
		eb.listener.Close()
	}

	// Close all subscriber connections
	eb.mu.Lock()
	for _, conn := range eb.subscribers {
		conn.Close()
	}
	eb.subscribers = make(map[string]net.Conn)
	eb.mu.Unlock()

	log.Printf("[EVENT_BUS] Stopped")
}

// acceptSubscribers handles incoming subscriber connections
func (eb *EventBus) acceptSubscribers() {
	for eb.running.Load() {
		conn, err := eb.listener.Accept()
		if err != nil {
			if eb.running.Load() {
				log.Printf("[EVENT_BUS] Accept error: %v", err)
			}
			continue
		}

		id := conn.RemoteAddr().String()
		eb.mu.Lock()
		eb.subscribers[id] = conn
		eb.mu.Unlock()

		log.Printf("[EVENT_BUS] New subscriber connected: %s (total: %d)", id, eb.SubscriberCount())

		// Monitor connection for disconnect
		go eb.monitorConnection(id, conn)
	}
}

// monitorConnection watches for subscriber disconnect
func (eb *EventBus) monitorConnection(id string, conn net.Conn) {
	buf := make([]byte, 1)
	for {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, err := conn.Read(buf)
		if err != nil {
			eb.mu.Lock()
			delete(eb.subscribers, id)
			eb.mu.Unlock()
			conn.Close()
			log.Printf("[EVENT_BUS] Subscriber disconnected: %s", id)
			return
		}
	}
}

// broadcastEvents sends events to all subscribers.
// Uses select to handle both events and shutdown signals.
func (eb *EventBus) broadcastEvents() {
	for {
		select {
		case <-eb.stopChan:
			// Drain remaining events before shutting down
			for len(eb.eventChan) > 0 {
				event := <-eb.eventChan
				eb.sendEventToSubscribers(event)
			}
			return
		case event, ok := <-eb.eventChan:
			if !ok {
				return // Channel closed
			}
			eb.sendEventToSubscribers(event)
		}
	}
}

// sendEventToSubscribers broadcasts a single event to all connected subscribers
func (eb *EventBus) sendEventToSubscribers(event models.TokenHolderEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("[EVENT_BUS] Failed to marshal event: %v", err)
		return
	}

	// Add newline delimiter for easy parsing
	data = append(data, '\n')

	// Send to all subscribers
	eb.mu.RLock()
	for id, conn := range eb.subscribers {
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err := conn.Write(data)
		if err != nil {
			eb.broadcastErrors.Add(1)
			log.Printf("[EVENT_BUS] Failed to send to %s: %v", id, err)
			// Don't remove here, let monitorConnection handle it
		}
	}
	eb.mu.RUnlock()
}

// PublishEvent sends an event to all subscribers.
// Non-blocking - drops the event if the channel is full or the bus is stopped.
func (eb *EventBus) PublishEvent(event models.TokenHolderEvent) {
	if !eb.running.Load() {
		return // Bus is stopped, don't try to send
	}
	select {
	case eb.eventChan <- event:
		eb.eventsPublished.Add(1)
	default:
		eb.eventsDropped.Add(1)
		log.Printf("[EVENT_BUS] Warning: Event channel full, dropping event (total dropped: %d)", eb.eventsDropped.Load())
	}
}

// IsRunning returns whether the event bus is currently running
func (eb *EventBus) IsRunning() bool {
	return eb.running.Load()
}

// SubscriberCount returns the current number of subscribers
func (eb *EventBus) SubscriberCount() int {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return len(eb.subscribers)
}

// EventBusStats holds metrics about event bus performance
type EventBusStats struct {
	Subscribers     int    `json:"subscribers"`
	EventsPublished uint64 `json:"events_published"`
	EventsDropped   uint64 `json:"events_dropped"`
	BroadcastErrors uint64 `json:"broadcast_errors"`
	ChannelSize     int    `json:"channel_size"`
	ChannelCap      int    `json:"channel_capacity"`
	Running         bool   `json:"running"`
}

// Stats returns current event bus metrics
func (eb *EventBus) Stats() EventBusStats {
	return EventBusStats{
		Subscribers:     eb.SubscriberCount(),
		EventsPublished: eb.eventsPublished.Load(),
		EventsDropped:   eb.eventsDropped.Load(),
		BroadcastErrors: eb.broadcastErrors.Load(),
		ChannelSize:     len(eb.eventChan),
		ChannelCap:      cap(eb.eventChan),
		Running:         eb.running.Load(),
	}
}

// BlockEvent represents a block-level event (for broader notifications)
type BlockEvent struct {
	Type       string   `json:"type"` // "block_processed"
	Slot       uint64   `json:"slot"`
	BlockHash  string   `json:"block_hash"`
	TxCount    int      `json:"tx_count"`
	TokensChanged []TokenChange `json:"tokens_changed"`
}

// TokenChange summarizes changes to a specific token in a block
type TokenChange struct {
	PolicyID    string `json:"policy_id"`
	AssetName   string `json:"asset_name"`
	Fingerprint string `json:"fingerprint"`
	HoldersAdded   int `json:"holders_added"`
	HoldersRemoved int `json:"holders_removed"`
	TxCount        int `json:"tx_count"`
}

// PublishBlockEvent publishes a summary event for a processed block
func (eb *EventBus) PublishBlockEvent(event BlockEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("[EVENT_BUS] Failed to marshal block event: %v", err)
		return
	}
	data = append(data, '\n')

	eb.mu.RLock()
	for id, conn := range eb.subscribers {
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err := conn.Write(data)
		if err != nil {
			log.Printf("[EVENT_BUS] Failed to send block event to %s: %v", id, err)
		}
	}
	eb.mu.RUnlock()
}
