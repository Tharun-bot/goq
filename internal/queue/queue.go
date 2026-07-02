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
func (q *Queue) leaseKey(name string) string      { return fmt.Sprintf("goq:lease:%s", name) }

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
// the queue list to the processing list, and registers a lease that expires
// after leaseDuration — if not Ack'd by then, the reaper will requeue it.
func (q *Queue) Dequeue(ctx context.Context, queueName string, timeout, leaseDuration time.Duration) (*model.Job, error) {
	jobID, err := q.client.BRPopLPush(ctx, q.queueKey(queueName), q.processingKey(queueName), timeout).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("brpoplpush: %w", err)
	}

	expiresAt := float64(time.Now().Add(leaseDuration).Unix())
	if err := q.client.ZAdd(ctx, q.leaseKey(queueName), redis.Z{Score: expiresAt, Member: jobID}).Err(); err != nil {
		return nil, fmt.Errorf("registering lease: %w", err)
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

// Ack removes a completed job from the processing list and clears its lease.
func (q *Queue) Ack(ctx context.Context, queueName, jobID string) error {
	pipe := q.client.TxPipeline()
	pipe.LRem(ctx, q.processingKey(queueName), 1, jobID)
	pipe.ZRem(ctx, q.leaseKey(queueName), jobID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("ack: %w", err)
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

var requeueScript = redis.NewScript(`
	local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
	if removed > 0 then
		redis.call('LPUSH', KEYS[2], ARGV[1])
	end
	redis.call('ZREM', KEYS[3], ARGV[1])
	return removed
`)

// RequeueStale scans for jobs whose lease has expired and atomically moves
// them from processing back onto the main queue. Returns the number of jobs requeued.
func (q *Queue) RequeueStale(ctx context.Context, queueName string) (int, error) {
	now := float64(time.Now().Unix())
	expired, err := q.client.ZRangeByScore(ctx, q.leaseKey(queueName), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", now),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("scanning expired leases: %w", err)
	}

	requeued := 0
	for _, jobID := range expired {
		res, err := requeueScript.Run(ctx, q.client,
			[]string{q.processingKey(queueName), q.queueKey(queueName), q.leaseKey(queueName)},
			jobID,
		).Result()
		if err != nil {
			return requeued, fmt.Errorf("requeue script for job %s: %w", jobID, err)
		}
		if n, ok := res.(int64); ok && n > 0 {
			requeued++
			if job, err := q.getJob(ctx, jobID); err == nil {
				job.Status = model.StatusPending
				job.UpdatedAt = time.Now().UTC()
				_ = q.saveJob(ctx, job) // best-effort metadata update, not critical path
			}
		}
	}
	return requeued, nil
}
