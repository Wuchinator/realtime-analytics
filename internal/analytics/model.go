package analytics

import (
	"encoding/json"
	"time"
)

type Summary struct {
	ID          int             `db:"id" json:"id"`
	Date        time.Time       `db:"date" json:"date"`
	Hour        int             `db:"hour" json:"hour"`
	EventType   string          `db:"event_type" json:"event_type"`
	TotalEvents int64           `db:"total_events" json:"total_events"`
	UniqueUsers int64           `db:"unique_users" json:"unique_users"`
	Metadata    json.RawMessage `db:"metadata" json:"metadata,omitempty"`
	UpdatedAt   time.Time       `db:"updated_at" json:"updated_at"`
}

func NewSummary(date time.Time, hour int, eventType string) *Summary {
	return &Summary{
		Date:        date.Truncate(24 * time.Hour),
		Hour:        hour,
		EventType:   eventType,
		TotalEvents: 0,
		UniqueUsers: 0,
		UpdatedAt:   time.Now().UTC(),
	}
}

func (s *Summary) IncrementEvents(count int64) {
	s.TotalEvents += count
	s.UpdatedAt = time.Now().UTC()
}

func (s *Summary) SetUniqueUsers(count int64) {
	s.UniqueUsers = count
	s.UpdatedAt = time.Now().UTC()
}

type EventData struct {
	ID        string                 `json:"id"`
	EventType string                 `json:"event_type"`
	UserID    string                 `json:"user_id"`
	SessionID string                 `json:"session_id"`
	ProductID *string                `json:"product_id,omitempty"`
	Data      map[string]interface{} `json:"data"`
	CreatedAt time.Time              `json:"created_at"`
}
