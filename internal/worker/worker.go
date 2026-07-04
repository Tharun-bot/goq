package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Tharun-bot/goq/internal/metrics"
	"github.com/Tharun-bot/goq/internal/model"
	"github.com/Tharun-bot/goq/internal/queue"
	"github.com/Tharun-bot/goq/internal/store"
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
	auditWriter   *store.AuditWriter
	metrics       *metrics.Metrics // nil is fine — metrics are optional, same pattern as auditWriter

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// WithMetrics attaches a metrics recorder. Optional — omit for tests that
// don't need a Prometheus registry.
func (p *Pool) WithMetrics(m *metrics.Metrics) *Pool {
	p.metrics = m
	return p
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

// WithAuditWriter attaches an audit writer so job completions/failures get
// recorded to Postgres asynchronously. Optional — omit for tests that don't
// need Postgres running.
func (p *Pool) WithAuditWriter(w *store.AuditWriter) *Pool {
	p.auditWriter = w
	return p
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
	if p.metrics != nil {
		p.metrics.ActiveWorkers.Inc()
		defer p.metrics.ActiveWorkers.Dec()
	}

	start := time.Now()
	err := p.handler(ctx, job)
	duration := time.Since(start)

	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err != nil {
		log.Printf("worker %d: job %s failed: %v", workerID, job.ID, err)
		if failErr := p.q.Fail(cleanupCtx, p.queueName, job.ID, p.maxRetries); failErr != nil {
			log.Printf("worker %d: failed to record failure for job %s: %v", workerID, job.ID, failErr)
		}
		if p.auditWriter != nil {
			p.auditWriter.Record(store.AuditEvent{Job: job, ErrorMessage: err.Error()})
		}
		if p.metrics != nil {
			status := "failed"
			if job.RetryCount+1 > p.maxRetries {
				status = "dead_letter"
			}
			p.metrics.RecordCompletion(p.queueName, status, duration)
		}
		return
	}

	if ackErr := p.q.Ack(cleanupCtx, p.queueName, job.ID); ackErr != nil {
		log.Printf("worker %d: ack failed for job %s: %v", workerID, job.ID, ackErr)
		return
	}
	log.Printf("worker %d: job %s completed", workerID, job.ID)
	if p.auditWriter != nil {
		p.auditWriter.Record(store.AuditEvent{Job: job})
	}
	if p.metrics != nil {
		p.metrics.RecordCompletion(p.queueName, "completed", duration)
	}
}

// Stop cancels all workers and blocks until they finish their current job.
func (p *Pool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}
