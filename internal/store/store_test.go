package store

import (
	"context"
	"testing"
	"time"

	"github.com/Tharun-bot/goq/internal/model"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(context.Background(), "postgres://goq:goq@localhost:5432/goq?sslmode=disable")
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestInsertBatch(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	job := &model.Job{
		ID: "test-job-" + time.Now().Format("150405.000000"), Queue: "test-queue",
		Status: model.StatusCompleted, RetryCount: 0,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}

	if err := s.InsertBatch(ctx, []AuditEvent{{Job: job}}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
}

func TestAuditWriterFlushesOnTimer(t *testing.T) {
	s := testStore(t)
	writer := NewAuditWriter(s, 100, 50, 300*time.Millisecond) // batchSize=50 so timer, not size, triggers flush
	ctx, cancel := context.WithCancel(context.Background())
	writer.Start(ctx)
	t.Cleanup(func() { cancel(); writer.Stop() })

	job := &model.Job{
		ID: "timer-test-" + time.Now().Format("150405.000000"), Queue: "test-queue",
		Status: model.StatusCompleted, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	writer.Record(AuditEvent{Job: job})

	time.Sleep(500 * time.Millisecond) // wait past the 300ms flush interval

	var count int
	row := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM job_audit WHERE job_id = $1", job.ID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 audit row after timer flush, got %d", count)
	}
}

func TestAuditWriterFlushesOnBatchSize(t *testing.T) {
	s := testStore(t)
	writer := NewAuditWriter(s, 100, 3, 10*time.Second) // long interval so batchSize triggers flush, not timer
	ctx, cancel := context.WithCancel(context.Background())
	writer.Start(ctx)
	t.Cleanup(func() { cancel(); writer.Stop() })

	prefix := "batch-test-" + time.Now().Format("150405.000000")
	for i := 0; i < 3; i++ {
		job := &model.Job{
			ID: prefix + "-" + time.Now().Format("000000000"), Queue: "test-queue",
			Status: model.StatusCompleted, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		}
		writer.Record(AuditEvent{Job: job})
	}

	time.Sleep(300 * time.Millisecond) // give the flush a moment to land

	var count int
	row := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM job_audit WHERE job_id LIKE $1", prefix+"%")
	if err := row.Scan(&count); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 audit rows after batch-size flush, got %d", count)
	}
}
