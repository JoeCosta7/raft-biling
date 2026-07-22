package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"raft-biling/internal/model"
	"time"

	bolt "go.etcd.io/bbolt"
)

// persistent backend
type Storage interface {
	Update(func(tx Tx) error) error
	View(func(tx Tx) error) error
	Close() error
}

type Tx interface {
	GetSchedule(tenantID, id string) (*model.Schedule, error)
	PutSchedule(s *model.Schedule) error

	GetExecution(tenantID, id string) (*model.Execution, error)
	PutExecution(ex *model.Execution) error

	GetAttempt(tenantID, id string) (*model.Attempt, error)
	PutAttempt(a *model.Attempt) error

	ListExecutionsBySchedule(tenantID, scheduleID string, fn func(*model.Execution) error) error
	ListExecutionsByStatus(tenantID string, status model.ExecutionStatus, fn func(*model.Execution) error) error
	ListAttemptsByExecution(tenantID, executionID string, fn func(*model.Attempt) error) error
}

const (
	bucketSchedules            = "schedules"
	bucketExecutions           = "executions"
	bucketAttempts             = "attempts"
	bucketExecutionsBySchedule = "executions_by_schedule"
	bucketExecutionsByStatus   = "executions_by_status"
	bucketAttemptsByExecution  = "attempts_by_execution"
)

// bbolt implementation
type BoltStorage struct {
	db *bolt.DB
}

func (s *BoltStorage) Update(fn func(tx Tx) error) error {
	return s.db.Update(func(btx *bolt.Tx) error {
		return fn(&boltTx{tx: btx})
	})
}

func (s *BoltStorage) View(fn func(tx Tx) error) error {
	return s.db.View(func(btx *bolt.Tx) error {
		return fn(&boltTx{tx: btx})
	})
}

func (s *BoltStorage) Close() error {
	return s.db.Close()
}

func New(dataDir string) (*BoltStorage, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dataDir, "scheduler.db")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		buckets := [][]byte{
			[]byte(bucketSchedules),
			[]byte(bucketExecutions),
			[]byte(bucketAttempts),
			[]byte(bucketExecutionsBySchedule),
			[]byte(bucketExecutionsByStatus),
			[]byte(bucketAttemptsByExecution),
		}
		for _, b := range buckets {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &BoltStorage{db: db}, nil

}

type boltTx struct {
	tx *bolt.Tx
}

func scheduleKey(tenantID, id string) []byte {
	return []byte(tenantID + ":" + id)
}

func (t *boltTx) PutSchedule(s *model.Schedule) error {
	jsonData, err := json.Marshal(s)
	if err != nil {
		return err
	}
	key := scheduleKey(s.TenantID, s.ID)
	return t.tx.Bucket([]byte(bucketSchedules)).Put(key, jsonData)
}

func (t *boltTx) GetSchedule(tenantID, id string) (*model.Schedule, error) {
	key := scheduleKey(tenantID, id)
	raw := t.tx.Bucket([]byte(bucketSchedules)).Get(key)
	if raw == nil {
		return nil, nil
	}
	var s model.Schedule
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func executionKey(tenantID, id string) []byte {
	return []byte(tenantID + ":" + id)
}

func (t *boltTx) PutExecution(ex *model.Execution) error {
	jsonData, err := json.Marshal(ex)
	if err != nil {
		return err
	}
	key := executionKey(ex.TenantID, ex.ID)
	return t.tx.Bucket([]byte(bucketExecutions)).Put(key, jsonData)
}

func (t *boltTx) GetExecution(tenantID, id string) (*model.Execution, error) {
	key := executionKey(tenantID, id)
	raw := t.tx.Bucket([]byte(bucketExecutions)).Get(key)
	if raw == nil {
		return nil, nil
	}
	var ex model.Execution
	if err := json.Unmarshal(raw, &ex); err != nil {
		return nil, err
	}
	return &ex, nil
}

func attemptKey(tenantID, id string) []byte {
	return []byte(tenantID + ":" + id)
}

func (t *boltTx) PutAttempt(at *model.Attempt) error {
	jsonData, err := json.Marshal(at)
	if err != nil {
		return err
	}
	key := attemptKey(at.TenantID, at.ID)
	return t.tx.Bucket([]byte(bucketAttempts)).Put(key, jsonData)

}

func (t *boltTx) GetAttempt(tenantID, id string) (*model.Attempt, error) {
	key := attemptKey(tenantID, id)
	raw := t.tx.Bucket([]byte(bucketAttempts)).Get(key)
	if raw == nil {
		return nil, nil
	}
	var at model.Attempt
	if err := json.Unmarshal(raw, &at); err != nil {
		return nil, err
	}
	return &at, nil
}

func (t *boltTx) ListExecutionsBySchedule(tenantID, scheduleID string, fn func(*model.Execution) error) error {
	return nil // TODO: implement in next chunk
}

func (t *boltTx) ListExecutionsByStatus(tenantID string, status model.ExecutionStatus, fn func(*model.Execution) error) error {
	return nil
}

func (t *boltTx) ListAttemptsByExecution(tenantID, executionID string, fn func(*model.Attempt) error) error {
	return nil
}

//User facing
//Create Schedule, check whether a schedule with the tenant_id, & id already exists, read a schedule by tenant_id, id and write a schedule
//Update Schedule, check whether the schedule acutally exists, but get covers that and then update it, which put covers
//Pause Schedule, get to check if schedule exists, then use Put to set status to paused
//Resume Schedule, get to check if schedule exists, then use Put to set status to active
//Cancel Schedule, get to check if schedule exists, then use Put to set status to canceled
//Internal Commands
//Claim Exectuion, check whether exectuion already exists, create if not already existing, set owner
//Transfer Execuiotn, use put to set new owner and update claimed_at
//RecordAttempt appends Attempt row, calls put execution to updates parent's attempt count and last_attempt_id. Maybe need another function for proposer verification
//CompleteExecution, use put to update Execution & clear owner node id. Also put to schedule to advance next_run_at.
//FailExecutionTimeout, use put to update execution failed with timeout reasoning
