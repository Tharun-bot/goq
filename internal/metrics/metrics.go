package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Tharun-bot/goq/internal/queue"
)

// Metrics holds the event-driven counters/histograms that workers update
// directly, plus registers gauge-funcs that poll queue state on scrape.
type Metrics struct {
	JobsProcessed  *prometheus.CounterVec   // labels: queue, status (completed/failed/dead_letter)
	ProcessingTime *prometheus.HistogramVec // labels: queue
	ActiveWorkers  prometheus.Gauge         // currently-busy worker count (in-process, not per-queue)
}

func New(reg prometheus.Registerer, q *queue.Queue, queueName string) *Metrics {
	m := &Metrics{
		JobsProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "goq",
			Name:      "jobs_processed_total",
			Help:      "Total number of jobs processed, by queue and outcome status.",
		}, []string{"queue", "status"}),

		ProcessingTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "goq",
			Name:      "job_processing_seconds",
			Help:      "Job handler execution time in seconds.",
			Buckets:   prometheus.DefBuckets, // includes p99-relevant buckets by default
		}, []string{"queue"}),

		ActiveWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "goq",
			Name:      "active_workers",
			Help:      "Number of workers currently executing a job (not idle/polling).",
		}),
	}

	reg.MustRegister(m.JobsProcessed, m.ProcessingTime, m.ActiveWorkers)

	// Gauges that reflect live Redis state — polled fresh on every scrape,
	// never manually incremented/decremented, so they can't drift from reality.
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "goq", Name: "queue_pending", Help: "Jobs currently waiting in the main queue.",
	}, func() float64 { return float64(safeStat(q, queueName, statPending)) }))

	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "goq", Name: "queue_processing", Help: "Jobs currently checked out by a worker.",
	}, func() float64 { return float64(safeStat(q, queueName, statProcessing)) }))

	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "goq", Name: "queue_dlq_size", Help: "Jobs currently in the dead letter queue.",
	}, func() float64 { return float64(safeStat(q, queueName, statDLQ)) }))

	return m
}

type statField int

const (
	statPending statField = iota
	statProcessing
	statDLQ
)

// safeStat queries live stats with a short timeout — a slow/unreachable Redis
// must never hang a Prometheus scrape. Returns 0 on error rather than
// blocking or panicking; a scrape-time hiccup shouldn't take down monitoring.
func safeStat(q *queue.Queue, queueName string, field statField) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stats, err := q.Stats(ctx, queueName)
	if err != nil {
		return 0
	}
	switch field {
	case statPending:
		return stats.Pending
	case statProcessing:
		return stats.Processing
	case statDLQ:
		return stats.DeadLetter
	default:
		return 0
	}
}

// RecordCompletion is called by the worker pool after a job finishes,
// successfully or not.
func (m *Metrics) RecordCompletion(queueName, status string, duration time.Duration) {
	m.JobsProcessed.WithLabelValues(queueName, status).Inc()
	m.ProcessingTime.WithLabelValues(queueName).Observe(duration.Seconds())
}
