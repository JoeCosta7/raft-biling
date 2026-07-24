package command

import (
	"encoding/json"
	"time"
)

type LogEntry struct {
	Type       string          `json:"type"`
	ProposedAt time.Time       `json:"proposed_at"`
	Payload    json.RawMessage `json:"payload"`
}

type CommandError struct {
	Kind    string
	Message string
	Field   string
	Details map[string]interface{}
}

func (e *CommandError) Error() string { return e.Message }

const (
	KindValidation = "validation"
	KindConflict   = "conflict"
	KindNotFound   = "not_found"
	KindStorage    = "storage"
)
