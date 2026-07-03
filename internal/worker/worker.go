package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Tharun-bot/goq/internal/model"
	"github.com/Tharun-bot/goq/internal/queue"
)

// Handler processes a single job's payload. Returning an error means the job failed.
type Handler func(ctx context.Context, job *model.Job) error

type Pool struct {
	q             *queue.Queue
	queueName     string
	concurrency   int
	handler       Handler
	pollTimeout   time.Duration
	leaseDuration time.Duration
	maxRetries    int

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewPool(q *queue.Queue, queueName string, concurrency int, handler Handler) *Pool {
	return &Pool{
		q:             q,
		queueName:     queueName,
		concurrency:   concurrency,
		handler:       handler,
		pollTimeout:   2 * time.Second,
		leaseDuration: 30 * time.Second,
		maxRetries:    3,
	}
}

// Start launches `concurrency` goroutines, each looping: dequeue -> handle -> ack.
// Returns immediately; call Stop() to shut down gracefully.
func (p *Pool) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	for i := 0; i < p.concurrency; i++ {
		p.wg.Add(1)
		workerID := i
		go p.runWorker(ctx, workerID)
	}
}

func (p *Pool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := p.q.Dequeue(ctx, p.queueName, p.pollTimeout, p.leaseDuration)
		if err != nil {
			log.Printf("worker %d: dequeue error: %v", id, err)
			continue
		}
		if job == nil {
			continue // timeout, no job available — loop and check ctx.Done() again
		}

		p.process(ctx, id, job)
	}
}

func (p *Pool) process(ctx context.Context, workerID int, job *model.Job) {
	err := p.handler(ctx, job)

	// Use a fresh context for the Ack/Fail write — the job's outcome must be
	// recorded even if the pool's shutdown context has already been cancelled.
	// Using the cancellable ctx here caused a real bug: a job finishing right as
	// Stop() fires would complete successfully but fail to Ack, making the
	// reaper wrongly reprocess an already-completed job 30s later.
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err != nil {
		log.Printf("worker %d: job %s failed: %v", workerID, job.ID, err)
		if failErr := p.q.Fail(cleanupCtx, p.queueName, job.ID, p.maxRetries); failErr != nil {
			log.Printf("worker %d: failed to record failure for job %s: %v", workerID, job.ID, failErr)
		}
		return
	}

	if ackErr := p.q.Ack(cleanupCtx, p.queueName, job.ID); ackErr != nil {
		log.Printf("worker %d: ack failed for job %s: %v", workerID, job.ID, ackErr)
		return
	}
	log.Printf("worker %d: job %s completed", workerID, job.ID)
}

// Stop cancels all workers and blocks until they finish their current job.
func (p *Pool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}
