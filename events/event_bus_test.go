package events

import (
	"encoding/json"
	"nectar/models"
	"net"
	"strings"
	"testing"
)

func addTestSubscriber(eb *EventBus, id string) (net.Conn, net.Conn) {
	server, client := net.Pipe()
	eb.subscribers[id] = server
	return server, client
}

func TestSendEventToReadOnlySubscriber(t *testing.T) {
	eb := NewEventBus(1)
	server, client := addTestSubscriber(eb, "backend")
	defer server.Close()
	defer client.Close()

	readDone := make(chan string, 1)
	go func() {
		buf := make([]byte, 512)
		n, err := client.Read(buf)
		if err != nil {
			readDone <- err.Error()
			return
		}
		readDone <- string(buf[:n])
	}()

	eb.sendEventToSubscribers(models.TokenHolderEvent{
		EventType: "balance_increased",
		Policy:    "0102",
		Name:      "54455354",
		Address:   "addr1test",
		NewAmount: 100,
		TxHash:    "abcd",
		Slot:      42,
	})

	msg := <-readDone
	if !strings.Contains(msg, `"Policy":"0102"`) {
		t.Fatalf("expected serialized event, got %q", msg)
	}
	if eb.SubscriberCount() != 1 {
		t.Fatalf("read-only subscriber should remain connected, got %d subscribers", eb.SubscriberCount())
	}
}

func TestSendEventRemovesSubscriberOnWriteFailure(t *testing.T) {
	eb := NewEventBus(1)
	server, client := addTestSubscriber(eb, "backend")
	defer server.Close()
	client.Close()

	eb.sendEventToSubscribers(models.TokenHolderEvent{
		EventType: "balance_increased",
		Policy:    "0102",
		Name:      "54455354",
		Address:   "addr1test",
		NewAmount: 100,
		TxHash:    "abcd",
		Slot:      42,
	})

	if eb.SubscriberCount() != 0 {
		t.Fatalf("failed subscriber should be removed, got %d subscribers", eb.SubscriberCount())
	}
	if eb.Stats().BroadcastErrors == 0 {
		t.Fatal("expected broadcast error metric to increment")
	}
}

func TestPublishBlockEventUsesLowercaseBlockPayload(t *testing.T) {
	eb := NewEventBus(1)
	server, client := addTestSubscriber(eb, "backend")
	defer server.Close()
	defer client.Close()

	readDone := make(chan string, 1)
	go func() {
		buf := make([]byte, 512)
		n, err := client.Read(buf)
		if err != nil {
			readDone <- err.Error()
			return
		}
		readDone <- string(buf[:n])
	}()

	eb.PublishBlockEvent(BlockEvent{
		Type:      "block_processed",
		Slot:      42,
		BlockHash: "abcd",
		TxCount:   1,
		TokensChanged: []TokenChange{{
			PolicyID:  "0102",
			AssetName: "54455354",
		}},
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(<-readDone)), &payload); err != nil {
		t.Fatalf("failed to unmarshal block event: %v", err)
	}
	if payload["type"] != "block_processed" {
		t.Fatalf("expected block_processed event, got %#v", payload)
	}
	if eb.SubscriberCount() != 1 {
		t.Fatalf("subscriber should remain connected, got %d subscribers", eb.SubscriberCount())
	}
}
