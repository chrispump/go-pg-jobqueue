package worker

import (
	"context"
	"fmt"
	"sync"
)

// managed represents a worker along with a channel to signal when it has fully stopped.
type managed struct {
	worker Worker
	done   chan struct{}
}

func newManaged(worker Worker) managed {
	return managed{
		worker: worker,
		done:   make(chan struct{}),
	}
}

// Factory produces workers based on the provided configuration.
type Factory func(ctx context.Context, label string, id uint) Worker

// Manager owns worker lifecycles (start/stop/wait).
// It exists so the UI can add/remove workers safely without leaking goroutines.
type Manager struct {
	mu      sync.Mutex
	ctx     context.Context
	prefix  string
	factory Factory

	workers []managed
}

func NewManager(ctx context.Context, prefix string, factory Factory) (*Manager, error) {
	if factory == nil {
		return nil, fmt.Errorf("no worker factory provided")
	}

	return &Manager{
		ctx:     ctx,
		prefix:  prefix,
		factory: factory,
	}, nil
}

// StartWorker creates and starts a new worker using the factory.
func (m *Manager) StartWorker() error {
	// Protect access to worker count, factory reference, and reserving
	// a slot in the m.workers slice from concurrent access.
	m.mu.Lock()
	workerIndex := len(m.workers)
	factory := m.factory
	m.workers = append(m.workers, managed{})
	m.mu.Unlock()

	// Call factory outside of lock to avoid potential deadlocks
	worker := factory(m.ctx, m.prefix, uint(workerIndex))
	if worker == nil {
		return fmt.Errorf("worker factory returned nil worker")
	}

	mw := newManaged(worker)

	// Protect inserting the initialized worker at the reserved
	// position in the m.workers slice from concurrent access.
	m.mu.Lock()
	m.workers[workerIndex] = mw
	m.mu.Unlock()

	go func() {
		defer close(mw.done)

		worker.Start()
	}()

	return nil
}

// StopAll stops and removes all managed workers.
// It returns the number of workers that were stopped.
func (m *Manager) StopAll() int {
	m.mu.Lock()
	workers := m.workers
	m.workers = nil
	m.mu.Unlock()

	stopped := 0

	for i := len(workers) - 1; i >= 0; i-- {
		mw := workers[i]
		if mw.worker == nil {
			continue
		}

		mw.worker.Stop()
		<-mw.done

		stopped++
	}

	return stopped
}

func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.workers)
}

// WorkerStateSnapshot represents the aggregated state of all workers at a point in time.
type WorkerStateSnapshot struct {
	Idle       uint
	Fetching   uint
	Processing uint
	Total      uint
}

// GetWorkerStates returns a snapshot of all worker states for metrics collection.
func (m *Manager) GetWorkerStates() WorkerStateSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshot := WorkerStateSnapshot{
		Total: uint(len(m.workers)),
	}

	for _, mw := range m.workers {
		if mw.worker == nil {
			continue
		}

		state := mw.worker.GetState()
		switch state {
		case StateIdle:
			snapshot.Idle++
		case StateFetching:
			snapshot.Fetching++
		case StateProcessing:
			snapshot.Processing++
		}
	}

	return snapshot
}
