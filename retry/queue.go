package retry

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Cpotenzone/sentinel-v3/models"
)

// Executor is the interface for re-running a failed job.
type Executor interface {
	ExecuteRetry(ctx context.Context, job models.RetryJob) (*models.AdapterResult, error)
}

// Queue manages failed source queries with exponential backoff retry.
type Queue struct {
	mu   sync.Mutex
	jobs map[string]*models.RetryJob
}

// NewQueue creates an empty retry queue.
func NewQueue() *Queue {
	return &Queue{
		jobs: make(map[string]*models.RetryJob),
	}
}

// Enqueue adds a job to the retry queue.
func (q *Queue) Enqueue(job models.RetryJob) {
	q.mu.Lock()
	defer q.mu.Unlock()

	existing, ok := q.jobs[job.ID]
	if ok {
		// Update existing job
		existing.Attempts++
		existing.LastError = job.LastError
		existing.NextRetry = nextRetryTime(existing.Attempts)
		return
	}

	job.Attempts = 0
	q.jobs[job.ID] = &job
}

// Size returns the number of pending retry jobs.
func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}

// StartScheduler runs a background loop that processes due retry jobs.
// It runs every 30 seconds, checking for jobs whose NextRetry has passed.
func (q *Queue) StartScheduler(ctx context.Context, executor Executor) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	log.Println("Retry scheduler started (30s interval)")

	for {
		select {
		case <-ctx.Done():
			log.Println("Retry scheduler stopped")
			return
		case <-ticker.C:
			q.processDueJobs(ctx, executor)
		}
	}
}

func (q *Queue) processDueJobs(ctx context.Context, executor Executor) {
	q.mu.Lock()
	now := time.Now()
	var dueJobs []*models.RetryJob
	for _, job := range q.jobs {
		if now.After(job.NextRetry) {
			dueJobs = append(dueJobs, job)
		}
	}
	q.mu.Unlock()

	if len(dueJobs) == 0 {
		return
	}

	log.Printf("Retry scheduler: processing %d due jobs", len(dueJobs))

	for _, job := range dueJobs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result, err := executor.ExecuteRetry(ctx, *job)

		q.mu.Lock()
		if err != nil || (result != nil && result.Error != nil) {
			// Still failing
			job.Attempts++
			errMsg := ""
			if err != nil {
				errMsg = err.Error()
			} else if result != nil {
				errMsg = result.ErrorMsg
			}
			job.LastError = errMsg

			if job.Attempts >= job.MaxAttempts {
				// Give up — remove from queue
				log.Printf("Retry queue: giving up on %s after %d attempts: %s", job.ID, job.Attempts, errMsg)
				delete(q.jobs, job.ID)
			} else {
				job.NextRetry = nextRetryTime(job.Attempts)
				log.Printf("Retry queue: %s failed (attempt %d/%d), next retry at %s",
					job.ID, job.Attempts, job.MaxAttempts, job.NextRetry.Format(time.RFC3339))
			}
		} else {
			// Success! Remove from queue
			log.Printf("Retry queue: %s succeeded on attempt %d (%d records)",
				job.ID, job.Attempts+1, len(result.Records))
			delete(q.jobs, job.ID)
			// TODO: Store results to BigQuery/GCS for the original report
		}
		q.mu.Unlock()
	}
}

// nextRetryTime calculates exponential backoff: 2min, 5min, 15min, 30min, 60min
func nextRetryTime(attempts int) time.Time {
	delays := []time.Duration{
		2 * time.Minute,
		5 * time.Minute,
		15 * time.Minute,
		30 * time.Minute,
		60 * time.Minute,
	}
	idx := attempts
	if idx >= len(delays) {
		idx = len(delays) - 1
	}
	return time.Now().Add(delays[idx])
}
