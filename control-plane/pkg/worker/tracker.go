package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"control-plane/pkg/raft"
)

// Tracker monitors active workers and triggers rescheduling upon failure.
type Tracker struct {
	store            *raft.MetadataStore
	heartbeatTimeout time.Duration
	onWorkerFailure  func(workerID string)
	mu               sync.Mutex
	stopChan         chan struct{}
}

// NewTracker creates a new worker tracker.
func NewTracker(store *raft.MetadataStore, timeout time.Duration, onFailure func(workerID string)) *Tracker {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Tracker{
		store:            store,
		heartbeatTimeout: timeout,
		onWorkerFailure:  onFailure,
		stopChan:         make(chan struct{}),
	}
}

// Start runs the periodic check loop.
func (t *Tracker) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Printf("Worker tracker started with heartbeat timeout %v", t.heartbeatTimeout)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.stopChan:
			return
		case <-ticker.C:
			t.checkWorkers()
		}
	}
}

// Stop stops the tracker loop.
func (t *Tracker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	select {
	case <-t.stopChan:
		// already closed
	default:
		close(t.stopChan)
	}
}

func (t *Tracker) checkWorkers() {
	workers := t.store.GetAllWorkers()
	now := time.Now()

	for _, w := range workers {
		if !w.Active {
			continue
		}

		if now.Sub(w.LastActive) > t.heartbeatTimeout {
			log.Printf("Worker %s failed heartbeat check (last active: %v, limit: %v ago). Marking inactive.",
				w.WorkerID, w.LastActive.Format(time.RFC3339), t.heartbeatTimeout)
			
			// Mark inactive in store
			t.store.SetWorkerActiveStatus(w.WorkerID, false)

			// Trigger rescheduling
			if t.onWorkerFailure != nil {
				go t.onWorkerFailure(w.WorkerID)
			}
		}
	}
}
