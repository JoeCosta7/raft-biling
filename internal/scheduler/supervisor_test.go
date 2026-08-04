package scheduler

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

const testTimeout = time.Second

type fakeRunner struct {
	started   chan struct{} // closed when Run begins
	finished  chan struct{} // closed when Run returns
	returnErr error         // what Run returns; test sets before Run is called
}

func (f *fakeRunner) Run(ctx context.Context) error {
	close(f.started)
	<-ctx.Done()
	close(f.finished)
	return f.returnErr
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		started:  make(chan struct{}),
		finished: make(chan struct{}),
	}
}

type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r)
	h.mu.Unlock()
	return nil
}
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func (h *captureHandler) hasWarn(msgContains string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, msgContains) {
			return true
		}
	}
	return false
}

func TestAcquire(t *testing.T) {
	// Arrange
	runnerCh := make(chan *fakeRunner, 8)
	newWorker := func() Runner {
		r := newFakeRunner()
		runnerCh <- r
		return r
	}
	notifyCh := make(chan bool, 1)
	handler := &captureHandler{}
	logger := slog.New(handler)
	sv := NewSupervisor(newWorker, notifyCh, logger)

	parentCtx, cancel := context.WithCancel(context.Background())
	svDone := make(chan error, 1)
	go func() { svDone <- sv.Run(parentCtx) }()
	notifyCh <- true

	var r0 *fakeRunner
	select {
	case r0 = <-runnerCh:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not construct a worker")
	}
	select {
	case <-r0.started:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not start the worker")
	}

	notifyCh <- false

	// Cleanup: shut down supervisor.
	cancel()
	select {
	case err := <-svDone:
		if err != nil {
			t.Fatalf("supervisor returned error: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not shut down")
	}
}

func TestLose(t *testing.T) {
	// Arrange
	runnerCh := make(chan *fakeRunner, 8)
	newWorker := func() Runner {
		r := newFakeRunner()
		runnerCh <- r
		return r
	}
	notifyCh := make(chan bool, 1)
	handler := &captureHandler{}
	logger := slog.New(handler)
	sv := NewSupervisor(newWorker, notifyCh, logger)

	parentCtx, cancel := context.WithCancel(context.Background())
	svDone := make(chan error, 1)
	go func() { svDone <- sv.Run(parentCtx) }()
	notifyCh <- true

	var r0 *fakeRunner
	select {
	case r0 = <-runnerCh:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not construct a worker")
	}
	select {
	case <-r0.started:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not start the worker")
	}

	notifyCh <- false

	select {
	case <-r0.finished:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not stop the worker")
	}
	cancel()
	select {
	case err := <-svDone:
		if err != nil {
			t.Fatalf("supervisor returned error: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not shut down")
	}
}

func TestRedundantTrue(t *testing.T) {
	// Arrange
	runnerCh := make(chan *fakeRunner, 8)
	newWorker := func() Runner {
		r := newFakeRunner()
		runnerCh <- r
		return r
	}
	notifyCh := make(chan bool, 4)
	handler := &captureHandler{}
	logger := slog.New(handler)
	sv := NewSupervisor(newWorker, notifyCh, logger)

	parentCtx, cancel := context.WithCancel(context.Background())
	svDone := make(chan error, 1)
	go func() { svDone <- sv.Run(parentCtx) }()
	notifyCh <- true

	var r0 *fakeRunner
	select {
	case r0 = <-runnerCh:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not construct a worker")
	}
	select {
	case <-r0.started:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not start the worker")
	}

	notifyCh <- true
	notifyCh <- false

	select {
	case <-r0.finished:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not stop the worker")
	}

	if len(runnerCh) != 0 {
		t.Fatalf("redundant true spawned an extra worker: %d left in channel", len(runnerCh))
	}

	// Cleanup: shut down supervisor.
	cancel()
	select {
	case err := <-svDone:
		if err != nil {
			t.Fatalf("supervisor returned error: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not shut down")
	}
}

func TestRedundantFalse(t *testing.T) {
	// Arrange
	runnerCh := make(chan *fakeRunner, 8)
	newWorker := func() Runner {
		r := newFakeRunner()
		runnerCh <- r
		return r
	}
	notifyCh := make(chan bool, 4)
	handler := &captureHandler{}
	logger := slog.New(handler)
	sv := NewSupervisor(newWorker, notifyCh, logger)

	parentCtx, cancel := context.WithCancel(context.Background())
	svDone := make(chan error, 1)
	go func() { svDone <- sv.Run(parentCtx) }()

	notifyCh <- false
	notifyCh <- true

	// Causal barrier: the supervisor drains notifyCh in order, so a worker
	// only shows up here once the redundant false has been processed as a
	// no-op and the true has been acted on.
	select {
	case <-runnerCh:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not construct a worker")
	}

	if !handler.hasWarn("leader loop is not running") {
		t.Fatal("expected a warning for the redundant false, got none")
	}

	// Cleanup: shut down supervisor.
	cancel()
	select {
	case err := <-svDone:
		if err != nil {
			t.Fatalf("supervisor returned error: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not shut down")
	}
}

func TestReacquire(t *testing.T) {
	// Arrange
	runnerCh := make(chan *fakeRunner, 8)
	newWorker := func() Runner {
		r := newFakeRunner()
		runnerCh <- r
		return r
	}
	notifyCh := make(chan bool, 1)
	handler := &captureHandler{}
	logger := slog.New(handler)
	sv := NewSupervisor(newWorker, notifyCh, logger)

	parentCtx, cancel := context.WithCancel(context.Background())
	svDone := make(chan error, 1)
	go func() { svDone <- sv.Run(parentCtx) }()
	notifyCh <- true

	var r0 *fakeRunner
	select {
	case r0 = <-runnerCh:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not construct a worker")
	}
	select {
	case <-r0.started:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not start the worker")
	}

	notifyCh <- false

	select {
	case <-r0.finished:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not stop the worker")
	}

	notifyCh <- true

	var r1 *fakeRunner
	select {
	case r1 = <-runnerCh:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not construct a second worker")
	}
	select {
	case <-r1.started:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not start the second worker")
	}

	if len(runnerCh) != 0 {
		t.Fatalf("factory constructed more than two workers: %d left in channel", len(runnerCh))
	}

	// Cleanup: shut down supervisor.
	cancel()
	select {
	case err := <-svDone:
		if err != nil {
			t.Fatalf("supervisor returned error: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not shut down")
	}
}

func TestShutdownWhileLeading(t *testing.T) {
	// Arrange
	runnerCh := make(chan *fakeRunner, 8)
	newWorker := func() Runner {
		r := newFakeRunner()
		runnerCh <- r
		return r
	}
	notifyCh := make(chan bool, 1)
	handler := &captureHandler{}
	logger := slog.New(handler)
	sv := NewSupervisor(newWorker, notifyCh, logger)

	parentCtx, cancel := context.WithCancel(context.Background())
	svDone := make(chan error, 1)
	go func() { svDone <- sv.Run(parentCtx) }()
	notifyCh <- true

	var r0 *fakeRunner
	select {
	case r0 = <-runnerCh:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not construct a worker")
	}
	select {
	case <-r0.started:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not start the worker")
	}

	// Act: shut down via ctx cancellation, not notifyCh <- false.
	cancel()

	select {
	case <-r0.finished:
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not stop the worker on shutdown")
	}

	select {
	case err := <-svDone:
		if err != nil {
			t.Fatalf("supervisor returned error: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not shut down")
	}
}

func TestShutdownWhileFollower(t *testing.T) {
	// Arrange
	runnerCh := make(chan *fakeRunner, 8)
	newWorker := func() Runner {
		r := newFakeRunner()
		runnerCh <- r
		return r
	}
	notifyCh := make(chan bool, 1)
	handler := &captureHandler{}
	logger := slog.New(handler)
	sv := NewSupervisor(newWorker, notifyCh, logger)

	parentCtx, cancel := context.WithCancel(context.Background())
	svDone := make(chan error, 1)
	go func() { svDone <- sv.Run(parentCtx) }()

	// Act: never sent anything on notifyCh — supervisor stays a follower.
	cancel()

	select {
	case err := <-svDone:
		if err != nil {
			t.Fatalf("supervisor returned error: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("supervisor did not shut down")
	}

	if handler.hasWarn("leader loop is not running") {
		t.Fatal("supervisor logged a stop-while-not-running warning on a follower shutdown, but it never tried to stop anything")
	}
}
