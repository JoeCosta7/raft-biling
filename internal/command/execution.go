package command

import "raft-biling/internal/model"

func IsTerminal(s model.ExecutionStatus) bool {
	switch s {
	case model.ExecutionStatusSucceeded, model.ExecutionStatusFailedTerminal, model.ExecutionStatusFailedMaxRetries:
		return true
	default:
		return false
	}
}
