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
	if err != nil {
		log.Printf("worker %d: job %s failed: %v", workerID, job.ID, err)
		// Phase 4 will add retry/DLQ logic here. For now, just log and leave
		// it in the processing list — Phase 3's reaper will pick it up.
		return
	}

	if ackErr := p.q.Ack(ctx, p.queueName, job.ID); ackErr != nil {
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
