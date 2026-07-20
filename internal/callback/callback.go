package callback

import "raft-biling/internal/config"

type Callback struct {
}

func New(cfg *config.Config) *Callback {
	return &Callback{}
}
