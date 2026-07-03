package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestFailSchedulesRetryThenSucceeds(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	queueName := "retry-test-" + time.Now().Format("150405.000000")

	job, _, err := q.Enqueue(ctx, queueName, "payload", 1)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := q.Dequeue(ctx, queueName, 2*time.Second, 30*time.Second)
	if err != nil || got == nil {
		t.Fatalf("dequeue: %v", err)
	}

	// First failure -> should schedule retry ~2s out, not go to DLQ (maxRetries=3)
	if err := q.Fail(ctx, queueName, job.ID, 3); err != nil {
		t.Fatalf("fail: %v", err)
	}

	dlqLen, err := q.DLQLen(ctx, queueName)
	if err != nil || dlqLen != 0 {
		t.Fatalf("expected 0 in dlq after 1st failure, got %d (err: %v)", dlqLen, err)
	}

	time.Sleep(2100 * time.Millisecond) // wait past 2s backoff

	promoted, err := q.PromoteDueRetries(ctx, queueName)
	if err != nil {
		t.Fatalf("promote due retries: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("expected 1 promoted, got %d", promoted)
	}

	// Job should now be back on the main queue, dequeue-able again.
	got2, err := q.Dequeue(ctx, queueName, 2*time.Second, 30*time.Second)
	if err != nil || got2 == nil {
		t.Fatalf("re-dequeue after retry: %v", err)
	}
	if got2.RetryCount != 1 {
		t.Fatalf("expected retry_count=1, got %d", got2.RetryCount)
	}

	// This time succeed.
	if err := q.Ack(ctx, queueName, got2.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

func TestFailExhaustsToDeadLetter(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	queueName := "dlq-test-" + time.Now().Format("150405.000000")

	job, _, err := q.Enqueue(ctx, queueName, "payload", 1)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	const maxRetries = 3
	currentJobID := job.ID

	for attempt := 1; attempt <= maxRetries+1; attempt++ {
		got, err := q.Dequeue(ctx, queueName, 2*time.Second, 30*time.Second)
		if err != nil {
			t.Fatalf("attempt %d: dequeue: %v", attempt, err)
		}
		if got == nil {
			t.Fatalf("attempt %d: expected a job, got nil", attempt)
		}
		currentJobID = got.ID

		if err := q.Fail(ctx, queueName, got.ID, maxRetries); err != nil {
			t.Fatalf("attempt %d: fail: %v", attempt, err)
		}

		if attempt <= maxRetries {
			// force retry to be immediately due for test speed: manually
			// re-score it to "now" instead of waiting out real backoff
			if err := q.client.ZAdd(ctx, q.retryKey(queueName), redis_ZParam(q, got.ID)).Err(); err != nil {
				t.Fatalf("attempt %d: re-scoring retry: %v", attempt, err)
			}
			promoted, err := q.PromoteDueRetries(ctx, queueName)
			if err != nil || promoted != 1 {
				t.Fatalf("attempt %d: promote: promoted=%d err=%v", attempt, promoted, err)
			}
		}
	}

	dlqLen, err := q.DLQLen(ctx, queueName)
	if err != nil || dlqLen != 1 {
		t.Fatalf("expected 1 job in dlq after exhausting retries, got %d (err: %v)", dlqLen, err)
	}
	_ = errors.New // placeholder if unused import trimmed
	_ = currentJobID
}

func redis_ZParam(q *Queue, jobID string) redisZWrapper {
	return redisZWrapper{Score: 0, Member: jobID}
}

type redisZWrapper = redis.Z
