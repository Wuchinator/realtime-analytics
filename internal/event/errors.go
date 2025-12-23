package event

import "errors"

var (
	ErrDuplicateEvent = errors.New("duplicate event")

	ErrInvalidEventType = errors.New("invalid event type")

	ErrInvalidUserID = errors.New("invalid user id")

	ErrInvalidSessionID = errors.New("invalid session id")

	ErrEventAlreadyProcessed = errors.New("event already processed")

	ErrEventNotFound = errors.New("event not found")
)
