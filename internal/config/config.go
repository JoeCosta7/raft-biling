package config

import (
	"flag"
	"fmt"
	"strings"
)

// Config stuff -- upcase names get imported don't forget
type Config struct {
	NodeID    string
	RaftAddr  string
	HTTPAddr  string
	DataDir   string
	Peers     map[string]string
	Bootstrap bool

	peersRaw string
}

func New(args []string) (*Config, error) {
	cfg := &Config{}
	fs := flag.NewFlagSet("scheduler", flag.ContinueOnError)
	fs.StringVar(&cfg.NodeID, "node-id", "", "Id for a node")
	fs.StringVar(&cfg.RaftAddr, "raft-addr", "", "raft listen address")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", "", "http listen address")
	fs.StringVar(&cfg.DataDir, "data-dir", "", "path to data directory")
	fs.StringVar(&cfg.peersRaw, "peers", "", "comma-separated list of node-id=addr peers")
	fs.BoolVar(&cfg.Bootstrap, "bootstrap", false, "bootstraps as the first node in the cluster")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (cfg *Config) validate() error {
	if cfg.NodeID == "" {
		return fmt.Errorf("node-id is required")
	}
	if cfg.RaftAddr == "" {
		return fmt.Errorf("raft-addr is required")
	}
	if cfg.HTTPAddr == "" {
		return fmt.Errorf("http-addr is required")
	}
	if cfg.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}
	if err := cfg.parsePeers(); err != nil {
		return err
	}
	if _, found := cfg.Peers[cfg.NodeID]; !found {
		return fmt.Errorf("node-id %q not found in peers", cfg.NodeID)
	}
	return nil
}

func (cfg *Config) parsePeers() error {
	if cfg.peersRaw == "" {
		return fmt.Errorf("peers are required")
	}
	peers := make(map[string]string)
	for entry := range strings.SplitSeq(cfg.peersRaw, ",") {
		entry = strings.TrimSpace(entry)
		id, addr, ok := strings.Cut(entry, "=")
		if !ok {
			return fmt.Errorf("malformed peer entry %q, want node-id=addr", entry)
		}
		id = strings.TrimSpace(id)
		addr = strings.TrimSpace(addr)
		if id == "" {
			return fmt.Errorf("malformed peer entry %q, empty node-id", entry)
		}
		if addr == "" {
			return fmt.Errorf("malformed peer entry %q, empty addr", entry)
		}
		if _, dup := peers[id]; dup {
			return fmt.Errorf("duplicate peer node-id %q", id)
		}
		peers[id] = addr
	}
	cfg.Peers = peers
	return nil
}
