package command

import (
	"encoding/json"
	"time"

	"raft-biling/internal/apperr"
)

type LogEntry struct {
	Type       string          `json:"type"`
	ProposedAt time.Time       `json:"proposed_at"`
	Payload    json.RawMessage `json:"payload"`
}

type CommandError = apperr.CommandError

const (
	KindValidation = apperr.KindValidation
	KindConflict   = apperr.KindConflict
	KindNotFound   = apperr.KindNotFound
	KindStorage    = apperr.KindStorage
)
