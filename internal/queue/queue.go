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
func (q *Queue) retryKey(name string) string      { return fmt.Sprintf("goq:retry:%s", name) }
func (q *Queue) dlqKey(name string) string        { return fmt.Sprintf("goq:dlq:%s", name) }
func (q *Queue) scheduledKey(name string) string  { return fmt.Sprintf("goq:scheduled:%s", name) }

// Enqueue adds a job to the named queue. Returns (false, nil) if the job ID
// was already enqueued before (idempotency), without error — caller decides
// whether that's worth logging.
// func (q *Queue) Enqueue(ctx context.Context, queueName, payload string, priority int) (*model.Job, bool, error) {
// 	job := &model.Job{
// 		ID:        uuid.NewString(),
// 		Queue:     queueName,
// 		Payload:   payload,
// 		Priority:  priority,
// 		Status:    model.StatusPending,
// 		CreatedAt: time.Now().UTC(),
// 		UpdatedAt: time.Now().UTC(),
// 	}

// 	ok, err := q.client.SetNX(ctx, q.dedupKey(job.ID), 1, 24*time.Hour).Result()
// 	if err != nil {
// 		return nil, false, fmt.Errorf("dedup check: %w", err)
// 	}
// 	if !ok {
// 		return nil, false, nil
// 	}

// 	data, err := json.Marshal(job)
// 	if err != nil {
// 		return nil, false, fmt.Errorf("marshal job: %w", err)
// 	}

// 	pipe := q.client.TxPipeline()
// 	pipe.Set(ctx, q.jobKey(job.ID), data, 0)
// 	pipe.LPush(ctx, q.queueKey(queueName), job.ID)
// 	if _, err := pipe.Exec(ctx); err != nil {
// 		return nil, false, fmt.Errorf("enqueue pipeline: %w", err)
// 	}

// 	return job, true, nil
// }

func (q *Queue) Enqueue(ctx context.Context, queueName, payload string, priority int) (*model.Job, bool, error) {
	return q.EnqueueWithID(ctx, uuid.NewString(), queueName, payload, priority)
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

// Fail records a job failure. It clears the job from processing/lease, and
// either schedules a backoff retry or moves the job to the DLQ if maxRetries
// has been exhausted.
func (q *Queue) Fail(ctx context.Context, queueName, jobID string, maxRetries int) error {
	job, err := q.getJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("fail: loading job: %w", err)
	}

	pipe := q.client.TxPipeline()
	pipe.LRem(ctx, q.processingKey(queueName), 1, jobID)
	pipe.ZRem(ctx, q.leaseKey(queueName), jobID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("fail: clearing processing/lease: %w", err)
	}

	job.RetryCount++
	job.UpdatedAt = time.Now().UTC()

	if job.RetryCount > maxRetries {
		job.Status = model.StatusDead
		if err := q.saveJob(ctx, job); err != nil {
			return fmt.Errorf("fail: saving dead job: %w", err)
		}
		if err := q.client.LPush(ctx, q.dlqKey(queueName), jobID).Err(); err != nil {
			return fmt.Errorf("fail: pushing to dlq: %w", err)
		}
		return nil
	}

	job.Status = model.StatusFailed
	if err := q.saveJob(ctx, job); err != nil {
		return fmt.Errorf("fail: saving failed job: %w", err)
	}

	backoff := time.Duration(1<<uint(job.RetryCount)) * time.Second // retry 1->2s, 2->4s, 3->8s
	nextAttempt := float64(time.Now().Add(backoff).Unix())
	if err := q.client.ZAdd(ctx, q.retryKey(queueName), redis.Z{Score: nextAttempt, Member: jobID}).Err(); err != nil {
		return fmt.Errorf("fail: scheduling retry: %w", err)
	}

	return nil
}

// PromoteDueRetries moves jobs whose backoff window has elapsed from the
// retry set back onto the main queue. Returns the number promoted.
func (q *Queue) PromoteDueRetries(ctx context.Context, queueName string) (int, error) {
	now := float64(time.Now().Unix())
	due, err := q.client.ZRangeByScore(ctx, q.retryKey(queueName), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", now),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("scanning due retries: %w", err)
	}

	promoted := 0
	for _, jobID := range due {
		res, err := promoteScript.Run(ctx, q.client,
			[]string{q.retryKey(queueName), q.queueKey(queueName)},
			jobID,
		).Result()
		if err != nil {
			return promoted, fmt.Errorf("promote script for job %s: %w", jobID, err)
		}
		if n, ok := res.(int64); ok && n > 0 {
			promoted++
			if job, err := q.getJob(ctx, jobID); err == nil {
				job.Status = model.StatusPending
				job.UpdatedAt = time.Now().UTC()
				_ = q.saveJob(ctx, job)
			}
		}
	}
	return promoted, nil
}

var promoteScript = redis.NewScript(`
	local removed = redis.call('ZREM', KEYS[1], ARGV[1])
	if removed > 0 then
		redis.call('LPUSH', KEYS[2], ARGV[1])
	end
	return removed
`)

// DLQLen returns the current size of a queue's dead letter list.
func (q *Queue) DLQLen(ctx context.Context, queueName string) (int64, error) {
	return q.client.LLen(ctx, q.dlqKey(queueName)).Result()
}

// EnqueueDelayed schedules a job to become available on the main queue at
// runAt. Uses the same idempotency dedup as Enqueue.
func (q *Queue) EnqueueDelayed(ctx context.Context, queueName, payload string, priority int, runAt time.Time) (*model.Job, bool, error) {
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
	pipe.ZAdd(ctx, q.scheduledKey(queueName), redis.Z{Score: float64(runAt.Unix()), Member: job.ID})
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, false, fmt.Errorf("enqueue delayed pipeline: %w", err)
	}

	return job, true, nil
}

// PromoteDueScheduled moves scheduled jobs whose runAt has passed onto the
// main queue. Returns the number promoted. Reuses the same atomic
// remove-then-push script as retry promotion.
func (q *Queue) PromoteDueScheduled(ctx context.Context, queueName string) (int, error) {
	now := float64(time.Now().Unix())
	due, err := q.client.ZRangeByScore(ctx, q.scheduledKey(queueName), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", now),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("scanning due scheduled jobs: %w", err)
	}

	promoted := 0
	for _, jobID := range due {
		res, err := promoteScript.Run(ctx, q.client,
			[]string{q.scheduledKey(queueName), q.queueKey(queueName)},
			jobID,
		).Result()
		if err != nil {
			return promoted, fmt.Errorf("promote script for job %s: %w", jobID, err)
		}
		if n, ok := res.(int64); ok && n > 0 {
			promoted++
		}
	}
	return promoted, nil
}

// EnqueueWithID is like Enqueue but lets the caller supply the job ID instead
// of generating one. Used by the API layer so an idempotency key can be
// pre-associated with a specific job ID before the job is created.
func (q *Queue) EnqueueWithID(ctx context.Context, id, queueName, payload string, priority int) (*model.Job, bool, error) {
	job := &model.Job{
		ID:        id,
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

func (q *Queue) apiIdemKey(key string) string { return fmt.Sprintf("goq:api-idem:%s", key) }

// ClaimIdempotencyKey attempts to atomically associate key with jobID.
// claimed=true means this call won the race and the caller should proceed to
// create the job. claimed=false means someone already claimed this key —
// existingJobID tells the caller which job to return instead.
func (q *Queue) ClaimIdempotencyKey(ctx context.Context, key, jobID string) (existingJobID string, claimed bool, err error) {
	ok, err := q.client.SetNX(ctx, q.apiIdemKey(key), jobID, 24*time.Hour).Result()
	if err != nil {
		return "", false, fmt.Errorf("claim idempotency key: %w", err)
	}
	if ok {
		return jobID, true, nil
	}
	existing, err := q.client.Get(ctx, q.apiIdemKey(key)).Result()
	if err != nil {
		return "", false, fmt.Errorf("fetch existing idempotency mapping: %w", err)
	}
	return existing, false, nil
}

// GetJob retrieves a job by ID. Returns nil, nil (not an error) if not found —
// callers translate that into a 404 rather than a 500.
func (q *Queue) GetJob(ctx context.Context, jobID string) (*model.Job, error) {
	data, err := q.client.Get(ctx, q.jobKey(jobID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", jobID, err)
	}
	var job model.Job
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}
	return &job, nil
}

type QueueStats struct {
	Queue      string `json:"queue"`
	Pending    int64  `json:"pending"`
	Processing int64  `json:"processing"`
	Scheduled  int64  `json:"scheduled"`
	Retrying   int64  `json:"retrying"`
	DeadLetter int64  `json:"dead_letter"`
}

// Stats returns a snapshot of a queue's current state across all its
// internal Redis structures in a single round trip.
func (q *Queue) Stats(ctx context.Context, queueName string) (*QueueStats, error) {
	pipe := q.client.TxPipeline()
	pendingCmd := pipe.LLen(ctx, q.queueKey(queueName))
	processingCmd := pipe.LLen(ctx, q.processingKey(queueName))
	scheduledCmd := pipe.ZCard(ctx, q.scheduledKey(queueName))
	retryingCmd := pipe.ZCard(ctx, q.retryKey(queueName))
	dlqCmd := pipe.LLen(ctx, q.dlqKey(queueName))
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("stats pipeline: %w", err)
	}
	return &QueueStats{
		Queue:      queueName,
		Pending:    pendingCmd.Val(),
		Processing: processingCmd.Val(),
		Scheduled:  scheduledCmd.Val(),
		Retrying:   retryingCmd.Val(),
		DeadLetter: dlqCmd.Val(),
	}, nil
}
