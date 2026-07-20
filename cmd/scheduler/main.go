package main

import (
	"fmt"
	"os"
	"raft-biling/internal/config"
	"raft-biling/internal/node"
)

//All config flags
//based on a cli start command of
/*scheduler start \
  --node-id=node1 \
  --raft-addr=127.0.0.1:7001 \
  --http-addr=127.0.0.1:8001 \
  --data-dir=/var/lib/scheduler/node1 \
  --peers=node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003 \
  --bootstrap
*/

// Returns a channel on which to recieved specified signals
// func HandleSignals(sigs ...os.Signal) <-chan os.Signal {
// 	//register interest in SIGINT & SIGTERM to keep track of those evil evil commands and get back a channel
// 	sigCh := make(chan os.Signal, 2)
// 	signal.Nofify(sigCh, sigs...)
// 	for {
// 		sig := <-sigCh
// 		log.Printf(`received signal "%s"`, sig.String())
// 		ch <- sig
// 	}
// 	return ch
// }

// // CreateContext creates a context which is canceled if signals are recieved on the given channel
// func CreateContext(ch <-chan os.Signal) (context.Context, context.CancelFunc) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	go func() {
// 		<-ch
// 		cancel()
// 	}()
// 	return ctx, cancel
// }

func main() {
	// sigCh := HandleSignals(syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	// mainContext, _ := CreateContext(sigCh)

	cfg, err := config.New(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("config: %+v\n", cfg)
	n, err := node.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("node constructed: %+v\n", n)
	// stMach, err := statemachine.New(cfg*config.Config)(*StateMachine, error)
	// if err != nil {
	// 	log.Fatalf("failed to create state machine: %s", err.Error())
	// }
	// raftNode, err := raftnode.New(cfg*config.Config, sm*statemachine.StateMachine)(*RaftNode, error)
	// if err != nil {
	// 	log.Fatalf("failed to create raft node: %s", err.Error())
	// }
	// callback, err := callback.New(cfg*config.Config) * Callback
	// if err != nil {
	// 	log.Fatalf("failed to create callback: %s", err.Error())
	// }
	// scheduler, err := scheduler.New(rn*raftnode.RaftNode, sm*statemachine.StateMachine, cb*callback.Callback) * Scheduler
	// if err != nil {
	// 	log.Fatalf("failed to create store: %s", err.Error())
	// }
	// api, err := api.New(cfg*config.Config, rn*raftnode.RaftNode, sm*statemachine.StateMachine) * API
	// if err != nil {
	// 	log.Fatalf("failed to create api: %s", err.Error())
	// }
	// node := Node{
	// 	cfg:          cfg,
	// 	raftNode:     raftNode,
	// 	stateMachine: stMach,
	// 	api:          api,
	// 	scheduler:    scheduler,
	// 	callback:     callback,
	// }

}
