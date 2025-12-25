package query

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

type EventStat struct {
	Timestamp   time.Time      `json:"timestamp"`
	EventType   string         `json:"event_type"`
	TotalEvents int64          `json:"total_events"`
	UniqueUsers int64          `json:"unique_users"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

type ProductStat struct {
	ProductID      string  `json:"product_id"`
	EventCount     int64   `json:"event_count"`
	UniqueUsers    int64   `json:"unique_users"`
	ConversionRate float64 `json:"conversion_rate"`
}
