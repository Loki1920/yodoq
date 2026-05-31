package solution

import (
	"errors"
	"fmt"
	"sync"
)

// LeasedJob is what a worker receives when it successfully leases a job.
type LeasedJob struct {
	ID             string
	Payload        []byte
	LeaseExpiresAt int64 // milliseconds, in the same frame as Clock.Now()
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
}

// JobQueue is what you implement.
type JobQueue struct {
	clock   Clock
	mu      sync.Mutex
	jobs    map[string]*entry
	order   []string // FIFO insertion order
	counter uint64
}

// New constructs an empty queue using the given clock.
func New(clock Clock) *JobQueue {
	return &JobQueue{
		clock: clock,
		jobs:  make(map[string]*entry),
	}
}

// Enqueue stores a new job for processing and returns its unique ID.
func (q *JobQueue) Enqueue(payload []byte) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.counter++
	id := fmt.Sprintf("job-%d", q.counter)
	p := make([]byte, len(payload))
	copy(p, payload)
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
		// Available if pending, or if leased but the lease has expired.
		if e.state == statePending || (e.state == stateLeased && now >= e.leaseExpiresAt) {
			e.state = stateLeased
			e.leasedTo = workerID
			e.leaseExpiresAt = now + leaseMS
			return &LeasedJob{
				ID:             e.id,
				Payload:        e.payload,
				LeaseExpiresAt: e.leaseExpiresAt,
			}, nil
		}
	}
	return nil, nil
}

// Ack marks a job as completed. Returns an error if workerID is not the
// current valid lease holder (wrong worker or expired lease).
func (q *JobQueue) Ack(workerID, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	e, ok := q.jobs[jobID]
	if !ok {
		return errors.New("job not found")
	}
	if e.state != stateLeased {
		return errors.New("job is not currently leased")
	}
	if e.leasedTo != workerID {
		return errors.New("stale ack: lease is held by a different worker")
	}
	if q.clock.Now() >= e.leaseExpiresAt {
		return errors.New("stale ack: lease has expired")
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
		return errors.New("job not found")
	}
	if e.state != stateLeased {
		return errors.New("job is not currently leased")
	}
	if e.leasedTo != workerID {
		return errors.New("fail from wrong worker")
	}
	e.state = statePending
	e.leasedTo = ""
	e.leaseExpiresAt = 0
	return nil
}

// Stats returns counts of jobs by state. Used for harness progress checks.
func (q *JobQueue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()

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
