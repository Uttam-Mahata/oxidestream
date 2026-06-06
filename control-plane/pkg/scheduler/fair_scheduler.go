package scheduler

import (
	"sync"
)

// FairScheduler dynamically shares execution slots across concurrent active jobs.
type FairScheduler struct {
	mu         sync.Mutex
	activeJobs map[string]*Job
	maxSlots   int
}

// NewFairScheduler creates a new FairScheduler instance.
func NewFairScheduler(maxSlots int) *FairScheduler {
	if maxSlots <= 0 {
		maxSlots = 8 // Default to 8 concurrent tasks across the cluster
	}
	return &FairScheduler{
		activeJobs: make(map[string]*Job),
		maxSlots:   maxSlots,
	}
}

// RegisterJob marks a job as active and eligible for slots.
func (fs *FairScheduler) RegisterJob(jobID string, job *Job) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.activeJobs[jobID] = job
}

// UnregisterJob removes a job from active slot allocation.
func (fs *FairScheduler) UnregisterJob(jobID string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.activeJobs, jobID)
}

// GetAllowedTasks returns the number of concurrent tasks a job is allowed to execute.
// It shares maxSlots equally among all currently active jobs.
func (fs *FairScheduler) GetAllowedTasks(jobID string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	numJobs := len(fs.activeJobs)
	if numJobs == 0 {
		return fs.maxSlots
	}

	share := fs.maxSlots / numJobs
	if share < 1 {
		share = 1
	}
	return share
}
