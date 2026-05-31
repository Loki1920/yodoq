package solution

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
)

// Sentinel errors allow callers to distinguish failure modes with errors.Is.
var (
	ErrJobNotFound  = errors.New("job not found")
	ErrNotLeased    = errors.New("job is not currently leased")
	ErrWrongWorker  = errors.New("lease is held by a different worker")
	ErrLeaseExpired = errors.New("lease has expired")
)

// LeasedJob is what a worker receives when it successfully leases a job.
type LeasedJob struct {
	ID             string
	Payload        []byte
	LeaseExpiresAt int64 // milliseconds, in the same frame as Clock.Now()
	Attempt        int   // 1-based; how many times this job has been leased
}

// Clock is the queue's view of time. The test harness supplies a fake clock
// so it can fast-forward past lease expiries without sleeping.
type Clock interface {
	Now() int64 // milliseconds
}

// Stats reports the current size of each job state.
type Stats struct {
	Pending   int // never leased, or failed and waiting
	Leased    int // currently held by a worker (may have expired)
	Completed int // acked successfully
}

type jobState int

const (
	statePending   jobState = iota
	stateLeased
	stateCompleted
)

type entry struct {
	id             string
	payload        []byte
	state          jobState
	leasedTo       string
	leaseExpiresAt int64
	attempts       int // incremented each time the job is leased
}

// JobQueue is what you implement.
type JobQueue struct {
	clock Clock
	mu    sync.RWMutex // RWMutex: Stats reads don't block each other
	jobs  map[string]*entry
	order []string // FIFO insertion order for lease priority
}

// New constructs an empty queue using the given clock.
func New(clock Clock) *JobQueue {
	return &JobQueue{
		clock: clock,
		jobs:  make(map[string]*entry),
	}
}

// newID returns a cryptographically random 16-byte hex string.
// Sequential counters are predictable and unsafe across restarts.
func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Enqueue stores a new job for processing and returns its unique ID.
func (q *JobQueue) Enqueue(payload []byte) (string, error) {
	// Defensive copy before acquiring the lock — caller's buffer stays independent.
	p := make([]byte, len(payload))
	copy(p, payload)
	id := newID()

	q.mu.Lock()
	defer q.mu.Unlock()

	q.jobs[id] = &entry{id: id, payload: p, state: statePending}
	q.order = append(q.order, id)
	return id, nil
}

// Lease atomically reserves the next available job for workerID for leaseMS
// milliseconds. Returns (nil, nil) when no job is available.
// Jobs with expired leases are treated as pending and re-leased.
func (q *JobQueue) Lease(workerID string, leaseMS int64) (*LeasedJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.clock.Now()
	for _, id := range q.order {
		e := q.jobs[id]
		if e.state == stateCompleted {
			continue
		}
		if e.state == statePending || (e.state == stateLeased && now >= e.leaseExpiresAt) {
			e.state = stateLeased
			e.leasedTo = workerID
			e.leaseExpiresAt = now + leaseMS
			e.attempts++

			// Defensive copy so the worker cannot mutate queue-internal state.
			payload := make([]byte, len(e.payload))
			copy(payload, e.payload)

			return &LeasedJob{
				ID:             e.id,
				Payload:        payload,
				LeaseExpiresAt: e.leaseExpiresAt,
				Attempt:        e.attempts,
			}, nil
		}
	}
	return nil, nil
}

// Ack marks a job as completed. Returns a sentinel error if workerID is not
// the current valid lease holder (wrong worker or expired lease).
func (q *JobQueue) Ack(workerID, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	e, ok := q.jobs[jobID]
	if !ok {
		return ErrJobNotFound
	}
	if e.state != stateLeased {
		return ErrNotLeased
	}
	if e.leasedTo != workerID {
		return ErrWrongWorker
	}
	if q.clock.Now() >= e.leaseExpiresAt {
		return ErrLeaseExpired
	}
	e.state = stateCompleted
	e.leasedTo = ""
	return nil
}

// Fail releases the lease without completing. The job becomes immediately
// re-leasable.
func (q *JobQueue) Fail(workerID, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	e, ok := q.jobs[jobID]
	if !ok {
		return ErrJobNotFound
	}
	if e.state != stateLeased {
		return ErrNotLeased
	}
	if e.leasedTo != workerID {
		return ErrWrongWorker
	}
	e.state = statePending
	e.leasedTo = ""
	e.leaseExpiresAt = 0
	return nil
}

// Stats returns counts of jobs by state. Uses a read lock since no state
// is modified — concurrent Stats calls do not block each other.
func (q *JobQueue) Stats() Stats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var s Stats
	for _, e := range q.jobs {
		switch e.state {
		case statePending:
			s.Pending++
		case stateLeased:
			s.Leased++
		case stateCompleted:
			s.Completed++
		}
	}
	return s
}
