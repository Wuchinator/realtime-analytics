package event

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID          uuid.UUID       `db:"id" json:"id"`
	EventType   string          `db:"event_type" json:"event_type"`
	UserID      uuid.UUID       `db:"user_id" json:"user_id"`
	SessionID   uuid.UUID       `db:"session_id" json:"session_id"`
	ProductID   *uuid.UUID      `db:"product_id" json:"product_id"`
	Data        json.RawMessage `db:"data" json:"data"`
	CreatedAt   time.Time       `db:"created_at" json:"created_at"`
	ProcessedAt *time.Time      `db:"processed_at" json:"processed_at"`
}

const (
	EventTypePageView       = "page_view"
	EventTypeProductView    = "product_view"
	EventTypeAddToCart      = "add_to_cart"
	EventTypeRemoveFromCart = "remove_from_cart"
	EventTypePurchase       = "purchase"
	EventTypeSearch         = "search"
)

func NewEvent(
	eventType string,
	userId, sessionId uuid.UUID,
	productId *uuid.UUID,
	data map[string]any) (*Event, error) {

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return &Event{
		ID:        uuid.New(),
		EventType: eventType,
		UserID:    userId,
		SessionID: sessionId,
		ProductID: productId,
		Data:      dataBytes,
		CreatedAt: time.Now().UTC(),
	}, nil
}

func (e *Event) Validate() error {
	if e.EventType == "" {
		return ErrInvalidEventType
	}
	if e.UserID == uuid.Nil {
		return ErrInvalidUserID
	}
	if e.SessionID == uuid.Nil {
		return ErrInvalidSessionID
	}
	return nil
}

func (e *Event) MarkAsProcessed() {
	now := time.Now().UTC()
	e.ProcessedAt = &now
}
