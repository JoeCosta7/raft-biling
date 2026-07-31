package command

import (
	"fmt"
	"raft-biling/internal/model"
	"raft-biling/internal/storage"
	"time"
)

type CreateTenantCommand struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func ApplyCreateTenant(tx storage.Tx, cmd CreateTenantCommand, proposedAt time.Time) (*model.Tenant, error) {
	if cmd.ID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "id", Message: "id is missing"}
	}
	existing, err := tx.GetTenant(cmd.ID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("read failed: %v", err)}
	}
	if existing != nil {
		return nil, &CommandError{Kind: KindConflict, Field: "id", Message: "tenant with this ID already exists"}
	}
	tenant := model.Tenant{
		ID:        cmd.ID,
		Name:      cmd.Name,
		CreatedAt: proposedAt,
	}
	if err := tx.PutTenant(&tenant); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("write failed: %v", err)}
	}
	return &tenant, nil
}
