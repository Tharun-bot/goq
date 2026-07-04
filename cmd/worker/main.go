package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Tharun-bot/goq/internal/config"
	"github.com/Tharun-bot/goq/internal/metrics"
	"github.com/Tharun-bot/goq/internal/model"
	"github.com/Tharun-bot/goq/internal/queue"
	"github.com/Tharun-bot/goq/internal/store"
	"github.com/Tharun-bot/goq/internal/worker"
)

func main() {
	queueName := flag.String("queue", "default", "queue name to consume from")
	concurrency := flag.Int("concurrency", 10, "number of worker goroutines")
	flag.Parse()

	cfg := config.Load()

	q, err := queue.New(cfg.RedisURL)
	if err != nil {
		log.Fatalf("connecting to redis: %v", err)
	}
	defer q.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Postgres audit trail — optional; log and continue without it rather
	// than refusing to start, since a missing audit DB shouldn't block job processing.
	var auditWriter *store.AuditWriter
	pgStore, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Printf("warning: postgres unavailable, audit trail disabled: %v", err)
	} else {
		defer pgStore.Close()
		auditWriter = store.NewAuditWriter(pgStore, 1000, 50, 2*time.Second)
		auditWriter.Start(ctx)
		defer auditWriter.Stop()
	}

	m := metrics.New(prometheus.DefaultRegisterer, q, *queueName)

	handler := func(ctx context.Context, job *model.Job) error {
		// Placeholder handler — this is where your actual job logic goes
		// (send email, resize image, whatever GoQ is queueing for you).
		log.Printf("processing job %s: %s", job.ID, job.Payload)
		return nil
	}

	pool := worker.NewPool(q, *queueName, *concurrency, handler)
	pool.WithAuditWriter(auditWriter).WithMetrics(m)
	pool.Start(ctx)

	reaper := queue.NewReaper(q, *queueName, 10*time.Second)
	reaper.Start(ctx)

	retryPoller := queue.NewRetryPoller(q, *queueName, 2*time.Second)
	retryPoller.Start(ctx)

	schedPoller := queue.NewSchedulerPoller(q, *queueName, 2*time.Second)
	schedPoller.Start(ctx)

	log.Printf("worker started: queue=%s concurrency=%d", *queueName, *concurrency)

	<-ctx.Done()
	log.Println("shutting down worker...")

	reaper.Stop()
	retryPoller.Stop()
	schedPoller.Stop()
	pool.Stop()

	log.Println("worker shutdown complete")
}
