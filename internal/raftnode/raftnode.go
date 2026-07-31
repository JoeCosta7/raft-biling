package raftnode

import (
	"context"
	"fmt"
	"raft-biling/internal/config"
	"raft-biling/internal/model"
	"raft-biling/storage"

	"github.com/hashicorp/raft"
)

type RaftNode struct {
	raft    *raft.Raft
	storage storage.Storage
}

func New(cfg *config.Config, st storage.Storage) (*RaftNode, error) {
	return &RaftNode{
		//raft:raft
		storage: st,
	}, nil
}

func (rn *RaftNode) GetSchedule(tenantID, id string) (*model.Schedule, error) {
	if rn.raft.State() != raft.Leader {
		return nil, fmt.Errorf("failed to GetSchedule on node %v", string(rn.raft.Leader()))
	}
	var schedule *model.Schedule
	err := rn.storage.View(func(tx storage.Tx) error {
		s, err := tx.GetSchedule(tenantID, id)
		if err != nil {
			return err
		}
		schedule = s
		return nil
	})
	if err != nil {
		return nil, err
	}
	return schedule, nil
}

func (raftnode *RaftNode) Start(ctx context.Context) error    { return nil }
func (raftnode *RaftNode) Shutdown(ctx context.Context) error { return nil }
