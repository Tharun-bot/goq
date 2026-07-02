package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/Tharun-bot/goq/internal/model"
)

type Queue struct {
	client *redis.Client
}

func New(redisURL string) (*Queue, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parsing redis url: %w", err)
	}
	client := redis.NewClient(opt)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connecting to redis: %w", err)
	}

	return &Queue{client: client}, nil
}

func (q *Queue) queueKey(name string) string      { return fmt.Sprintf("goq:queue:%s", name) }
func (q *Queue) processingKey(name string) string { return fmt.Sprintf("goq:processing:%s", name) }
func (q *Queue) dedupKey(jobID string) string     { return fmt.Sprintf("goq:dedup:%s", jobID) }
func (q *Queue) jobKey(jobID string) string       { return fmt.Sprintf("goq:job:%s", jobID) }

// Enqueue adds a job to the named queue. Returns (false, nil) if the job ID
// was already enqueued before (idempotency), without error — caller decides
// whether that's worth logging.
func (q *Queue) Enqueue(ctx context.Context, queueName, payload string, priority int) (*model.Job, bool, error) {
	job := &model.Job{
		ID:        uuid.NewString(),
		Queue:     queueName,
		Payload:   payload,
		Priority:  priority,
		Status:    model.StatusPending,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	ok, err := q.client.SetNX(ctx, q.dedupKey(job.ID), 1, 24*time.Hour).Result()
	if err != nil {
		return nil, false, fmt.Errorf("dedup check: %w", err)
	}
	if !ok {
		return nil, false, nil
	}

	data, err := json.Marshal(job)
	if err != nil {
		return nil, false, fmt.Errorf("marshal job: %w", err)
	}

	pipe := q.client.TxPipeline()
	pipe.Set(ctx, q.jobKey(job.ID), data, 0)
	pipe.LPush(ctx, q.queueKey(queueName), job.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, false, fmt.Errorf("enqueue pipeline: %w", err)
	}

	return job, true, nil
}

// Dequeue blocks up to timeout waiting for a job, atomically moving it from
// the queue list to the processing list. Returns nil, nil on timeout (no job available).
func (q *Queue) Dequeue(ctx context.Context, queueName string, timeout time.Duration) (*model.Job, error) {
	jobID, err := q.client.BRPopLPush(ctx, q.queueKey(queueName), q.processingKey(queueName), timeout).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("brpoplpush: %w", err)
	}

	job, err := q.getJob(ctx, jobID)
	if err != nil {
		return nil, err
	}

	job.Status = model.StatusProcessing
	job.UpdatedAt = time.Now().UTC()
	if err := q.saveJob(ctx, job); err != nil {
		return nil, err
	}

	return job, nil
}

// Ack removes a completed job from the processing list.
func (q *Queue) Ack(ctx context.Context, queueName, jobID string) error {
	if err := q.client.LRem(ctx, q.processingKey(queueName), 1, jobID).Err(); err != nil {
		return fmt.Errorf("ack lrem: %w", err)
	}
	return nil
}

func (q *Queue) getJob(ctx context.Context, jobID string) (*model.Job, error) {
	data, err := q.client.Get(ctx, q.jobKey(jobID)).Result()
	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", jobID, err)
	}
	var job model.Job
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}
	return &job, nil
}

func (q *Queue) saveJob(ctx context.Context, job *model.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	return q.client.Set(ctx, q.jobKey(job.ID), data, 0).Err()
}

func (q *Queue) Close() error {
	return q.client.Close()
}
