package store

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Tharun-bot/goq/internal/model"
)

const schema = `
CREATE TABLE IF NOT EXISTS job_audit (
	id           BIGSERIAL PRIMARY KEY,
	job_id       TEXT NOT NULL,
	queue        TEXT NOT NULL,
	status       TEXT NOT NULL,
	retry_count  INT NOT NULL,
	error_message TEXT,
	created_at   TIMESTAMPTZ NOT NULL,
	updated_at   TIMESTAMPTZ NOT NULL,
	recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_job_audit_job_id ON job_audit(job_id);
`

type AuditEvent struct {
	Job          *model.Job
	ErrorMessage string
}

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}

	if _, err := pool.Exec(ctx, schema); err != nil {
		return nil, fmt.Errorf("applying schema: %w", err)
	}

	return &Store{pool: pool}, nil
}

// InsertBatch writes a batch of audit events in a single round trip.
// Errors here are logged by the caller (AuditWriter), never propagated to
// the job-processing hot path.
func (s *Store) InsertBatch(ctx context.Context, events []AuditEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch := &pgxBatchWrapper{}
	for _, e := range events {
		batch.Queue(
			`INSERT INTO job_audit (job_id, queue, status, retry_count, error_message, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			e.Job.ID, e.Job.Queue, string(e.Job.Status), e.Job.RetryCount, e.ErrorMessage, e.Job.CreatedAt, e.Job.UpdatedAt,
		)
	}

	br := s.pool.SendBatch(ctx, batch.b)
	defer br.Close()

	for i := 0; i < len(events); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert event %d: %w", i, err)
		}
	}
	return nil
}

func (s *Store) Close() {
	s.pool.Close()
}

// small wrapper so callers don't need to import pgx directly just to build a batch
type pgxBatchWrapper struct {
	b pgxBatch
}

func (w *pgxBatchWrapper) Queue(sql string, args ...interface{}) {
	if w.b == nil {
		w.b = newPgxBatch()
	}
	w.b.Queue(sql, args...)
}

func logDropped(event AuditEvent, reason error) {
	log.Printf("audit: dropping event for job %s (%s): %v", event.Job.ID, event.Job.Status, reason)
}
