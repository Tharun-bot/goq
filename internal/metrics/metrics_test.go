package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Tharun-bot/goq/internal/queue"
)

func TestMetricsExposedOnScrape(t *testing.T) {
	q, err := queue.New("redis://localhost:6379")
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	queueName := "metrics-test-" + time.Now().Format("150405.000000")
	reg := prometheus.NewRegistry() // isolated registry so this test doesn't collide with other tests' global state
	m := New(reg, q, queueName)

	m.RecordCompletion(queueName, "completed", 120*time.Millisecond)
	m.RecordCompletion(queueName, "failed", 50*time.Millisecond)

	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	wantSubstrings := []string{
		"goq_jobs_processed_total",
		"goq_job_processing_seconds",
		"goq_active_workers",
		"goq_queue_pending",
		"goq_queue_dlq_size",
	}
	for _, w := range wantSubstrings {
		if !strings.Contains(body, w) {
			t.Fatalf("expected metrics output to contain %q, got:\n%s", w, body)
		}
	}
}

func TestQueuePendingGaugeReflectsLiveState(t *testing.T) {
	q, err := queue.New("redis://localhost:6379")
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	queueName := "gauge-test-" + time.Now().Format("150405.000000")
	reg := prometheus.NewRegistry()
	New(reg, q, queueName)

	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if _, _, err := q.Enqueue(ctx, queueName, "payload", 1); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `goq_queue_pending{`) && !strings.Contains(body, "goq_queue_pending 4") {
		t.Fatalf("expected goq_queue_pending to reflect 4 pending jobs, got:\n%s", body)
	}
}
