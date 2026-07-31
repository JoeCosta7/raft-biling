package statemachine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"raft-biling/internal/command"
	"raft-biling/internal/config"
	"raft-biling/storage"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

type StateMachine struct {
	storage storage.Storage
	db      *bolt.DB
	dataDir string
	//Apply(*Log) interface{} where log entries become state
	//Snapshot() (FSMSnapshot, error) //log compaction
	//Restore(io.ReadCloser) error //helps catchup
}

type FSMSnapshot struct {
	tx *bolt.Tx
}

func (sm *StateMachine) Apply(log *raft.Log) any {
	var envelope command.LogEntry
	if err := json.Unmarshal(log.Data, &envelope); err != nil {
		panic(err)
	}
	switch envelope.Type {
	case "create_schedule":
		var cmd command.CreateScheduleCommand
		if err := json.Unmarshal(envelope.Payload, &cmd); err != nil {
			panic(err)
		}
		var result any
		err := sm.storage.Update(func(tx storage.Tx) error {
			res, herr := command.ApplyCreateSchedule(tx, cmd, envelope.ProposedAt)
			result = res
			return herr
		})
		var cmdErr *command.CommandError
		if errors.As(err, &cmdErr) {
			return cmdErr
		}
		if err != nil {
			panic(err)
		}
		return result
	case "update_schedule":
		var cmd command.UpdateScheduleCommand
		if err := json.Unmarshal(envelope.Payload, &cmd); err != nil {
			panic(err)
		}
		var result any
		err := sm.storage.Update(func(tx storage.Tx) error {
			res, herr := command.ApplyUpdateSchedule(tx, cmd, envelope.ProposedAt)
			result = res
			return herr
		})
		var cmdErr *command.CommandError
		if errors.As(err, &cmdErr) {
			return cmdErr
		}
		if err != nil {
			panic(err)
		}
		return result
	case "pause_schedule":
		var cmd command.PauseScheduleCommand
		if err := json.Unmarshal(envelope.Payload, &cmd); err != nil {
			panic(err)
		}
		var result any
		err := sm.storage.Update(func(tx storage.Tx) error {
			res, herr := command.ApplyPauseSchedule(tx, cmd, envelope.ProposedAt)
			result = res
			return herr
		})
		var cmdErr *command.CommandError
		if errors.As(err, &cmdErr) {
			return cmdErr
		}
		if err != nil {
			panic(err)
		}
		return result
	case "cancel_schedule":
		var cmd command.CancelScheduleCommand
		if err := json.Unmarshal(envelope.Payload, &cmd); err != nil {
			panic(err)
		}
		var result any
		err := sm.storage.Update(func(tx storage.Tx) error {
			res, herr := command.ApplyCancelSchedule(tx, cmd, envelope.ProposedAt)
			result = res
			return herr
		})
		var cmdErr *command.CommandError
		if errors.As(err, &cmdErr) {
			return cmdErr
		}
		if err != nil {
			panic(err)
		}
		return result
	case "resume_schedule":
		var cmd command.ResumeScheduleCommand
		if err := json.Unmarshal(envelope.Payload, &cmd); err != nil {
			panic(err)
		}
		var result any
		err := sm.storage.Update(func(tx storage.Tx) error {
			res, herr := command.ApplyResumeSchedule(tx, cmd, envelope.ProposedAt)
			result = res
			return herr
		})
		var cmdErr *command.CommandError
		if errors.As(err, &cmdErr) {
			return cmdErr
		}
		if err != nil {
			panic(err)
		}
		return result
	case "claim_execution":
		var cmd command.ClaimExecutionCommand
		if err := json.Unmarshal(envelope.Payload, &cmd); err != nil {
			panic(err)
		}
		var result any
		err := sm.storage.Update(func(tx storage.Tx) error {
			res, herr := command.ApplyClaimExecution(tx, cmd, envelope.ProposedAt)
			result = res
			return herr
		})
		var cmdErr *command.CommandError
		if errors.As(err, &cmdErr) {
			return cmdErr
		}
		if err != nil {
			panic(err)
		}
		return result
	case "transfer_execution":
		var cmd command.TransferExecutionCommand
		if err := json.Unmarshal(envelope.Payload, &cmd); err != nil {
			panic(err)
		}
		var result any
		err := sm.storage.Update(func(tx storage.Tx) error {
			res, herr := command.ApplyTransferExecution(tx, cmd, envelope.ProposedAt)
			result = res
			return herr
		})
		var cmdErr *command.CommandError
		if errors.As(err, &cmdErr) {
			return cmdErr
		}
		if err != nil {
			panic(err)
		}
		return result
	case "record_attempt":
		var cmd command.RecordAttemptCommand
		if err := json.Unmarshal(envelope.Payload, &cmd); err != nil {
			panic(err)
		}
		var result any
		err := sm.storage.Update(func(tx storage.Tx) error {
			res, herr := command.ApplyRecordAttempt(tx, cmd, envelope.ProposedAt)
			result = res
			return herr
		})
		var cmdErr *command.CommandError
		if errors.As(err, &cmdErr) {
			return cmdErr
		}
		if err != nil {
			panic(err)
		}
		return result
	case "complete_execution":
		var cmd command.CompleteExecutionCommand
		if err := json.Unmarshal(envelope.Payload, &cmd); err != nil {
			panic(err)
		}
		var result any
		err := sm.storage.Update(func(tx storage.Tx) error {
			res, herr := command.ApplyCompleteExecution(tx, cmd, envelope.ProposedAt)
			result = res
			return herr
		})
		var cmdErr *command.CommandError
		if errors.As(err, &cmdErr) {
			return cmdErr
		}
		if err != nil {
			panic(err)
		}
		return result
	case "fail_execution_timeout":
		var cmd command.FailExecutionTimeoutCommand
		if err := json.Unmarshal(envelope.Payload, &cmd); err != nil {
			panic(err)
		}
		var result any
		err := sm.storage.Update(func(tx storage.Tx) error {
			res, herr := command.ApplyFailExecutionTimeout(tx, cmd, envelope.ProposedAt)
			result = res
			return herr
		})
		var cmdErr *command.CommandError
		if errors.As(err, &cmdErr) {
			return cmdErr
		}
		if err != nil {
			panic(err)
		}
		return result
	default:
		panic(fmt.Sprintf("unknown log entry type: %q", envelope.Type))
	}
}

func (sm *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	tx, err := sm.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &FSMSnapshot{tx: tx}, nil
}

func (sm *StateMachine) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	err := sm.db.Close()
	if err != nil {
		return err
	}
	path := filepath.Join(sm.dataDir, "state.db")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, rc)
	if err != nil {
		return err
	}
	file.Close()
	newDB, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return err
	}
	sm.db = newDB

	return nil

}

func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := s.tx.WriteTo(sink); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *FSMSnapshot) Release() {
	s.tx.Rollback()
}

func New(cfg *config.Config) (*StateMachine, error) {
	store, err := storage.New(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	return &StateMachine{storage: store, db: store.DB(), dataDir: cfg.DataDir}, nil
}

func (statemachine *StateMachine) Start(ctx context.Context) error    { return nil }
func (statemachine *StateMachine) Shutdown(ctx context.Context) error { return nil }
