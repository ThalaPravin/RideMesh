package kafka

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTripRequestedEventSerialization(t *testing.T) {
	now := time.Now()
	event := TripRequestedEvent{
		BaseEvent: BaseEvent{
			EventID:   "evt_12345",
			EventType: "TRIP_REQUESTED",
			Timestamp: now,
		},
		Payload: TripRequestedPayload{
			TripID:      "trip_999",
			UserID:      "usr_888",
			PickupLat:   12.9716,
			PickupLon:   77.5946,
			DestLat:     12.9279,
			DestLon:     77.6271,
			VehicleType: "SUV",
			Amount:      450.00,
		},
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal TripRequestedEvent: %v", err)
	}

	var decoded TripRequestedEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal TripRequestedEvent: %v", err)
	}

	if decoded.EventID != event.EventID {
		t.Errorf("Expected EventID %s, got %s", event.EventID, decoded.EventID)
	}

	if decoded.Payload.TripID != event.Payload.TripID {
		t.Errorf("Expected TripID %s, got %s", event.Payload.TripID, decoded.Payload.TripID)
	}

	if decoded.Payload.Amount != event.Payload.Amount {
		t.Errorf("Expected Amount %f, got %f", event.Payload.Amount, decoded.Payload.Amount)
	}
}

func TestPaymentProcessedEventSerialization(t *testing.T) {
	event := PaymentProcessedEvent{
		BaseEvent: BaseEvent{
			EventID:   "evt_pay_1",
			EventType: "PAYMENT_PROCESSED",
			Timestamp: time.Now(),
		},
		Payload: PaymentProcessedPayload{
			TransactionID: "txn_abc123",
			TripID:        "trip_999",
			UserID:        "usr_888",
			Amount:        450.00,
			Status:        "SUCCESS",
		},
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal PaymentProcessedEvent: %v", err)
	}

	var decoded PaymentProcessedEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal PaymentProcessedEvent: %v", err)
	}

	if decoded.Payload.Status != "SUCCESS" {
		t.Errorf("Expected Status SUCCESS, got %s", decoded.Payload.Status)
	}

	if decoded.Payload.TransactionID != "txn_abc123" {
		t.Errorf("Expected TransactionID txn_abc123, got %s", decoded.Payload.TransactionID)
	}
}
