package queue

import (
	"context"
	"testing"
	"time"
)

func TestReaperRequeuesExpiredJob(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	queueName := "reaper-test-" + time.Now().Format("150405.000000")

	job, _, err := q.Enqueue(ctx, queueName, "payload", 1)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Simulate a worker picking up the job with a very short lease, then crashing
	// (never calling Ack).
	got, err := q.Dequeue(ctx, queueName, 2*time.Second, 500*time.Millisecond)
	if err != nil || got == nil {
		t.Fatalf("dequeue: %v", err)
	}
	if got.ID != job.ID {
		t.Fatalf("expected job %s, got %s", job.ID, got.ID)
	}

	// At this point job is in `processing`, not `queue`. Confirm that.
	processingLen, err := q.client.LLen(ctx, q.processingKey(queueName)).Result()
	if err != nil || processingLen != 1 {
		t.Fatalf("expected 1 job in processing, got %d (err: %v)", processingLen, err)
	}

	time.Sleep(700 * time.Millisecond) // wait past the 500ms lease

	requeued, err := q.RequeueStale(ctx, queueName)
	if err != nil {
		t.Fatalf("requeue stale: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("expected 1 job requeued, got %d", requeued)
	}

	queueLen, err := q.client.LLen(ctx, q.queueKey(queueName)).Result()
	if err != nil || queueLen != 1 {
		t.Fatalf("expected 1 job back in queue, got %d (err: %v)", queueLen, err)
	}

	processingLen, err = q.client.LLen(ctx, q.processingKey(queueName)).Result()
	if err != nil || processingLen != 0 {
		t.Fatalf("expected 0 jobs left in processing, got %d (err: %v)", processingLen, err)
	}
}

func TestReaperDoesNotRequeueAckedJob(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	queueName := "reaper-ack-test-" + time.Now().Format("150405.000000")

	if _, _, err := q.Enqueue(ctx, queueName, "payload", 1); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := q.Dequeue(ctx, queueName, 2*time.Second, 500*time.Millisecond)
	if err != nil || got == nil {
		t.Fatalf("dequeue: %v", err)
	}

	if err := q.Ack(ctx, queueName, got.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	time.Sleep(700 * time.Millisecond)

	requeued, err := q.RequeueStale(ctx, queueName)
	if err != nil {
		t.Fatalf("requeue stale: %v", err)
	}
	if requeued != 0 {
		t.Fatalf("expected 0 jobs requeued (already acked), got %d", requeued)
	}
}

// TestReaperStress simulates 50 worker crashes in a row and confirms every
// single job survives — this is the core reliability guarantee of the whole project.
func TestReaperStress(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	queueName := "reaper-stress-" + time.Now().Format("150405.000000")

	const iterations = 50
	for i := 0; i < iterations; i++ {
		if _, _, err := q.Enqueue(ctx, queueName, "payload", 1); err != nil {
			t.Fatalf("iter %d: enqueue: %v", i, err)
		}

		got, err := q.Dequeue(ctx, queueName, 2*time.Second, 300*time.Millisecond)
		if err != nil || got == nil {
			t.Fatalf("iter %d: dequeue: %v", i, err)
		}
		// simulate crash: never Ack

		time.Sleep(400 * time.Millisecond)

		requeued, err := q.RequeueStale(ctx, queueName)
		if err != nil {
			t.Fatalf("iter %d: requeue stale: %v", i, err)
		}
		if requeued != 1 {
			t.Fatalf("iter %d: expected 1 requeued, got %d", i, requeued)
		}

		// Now actually complete it properly so the queue is clean for the next iteration.
		got2, err := q.Dequeue(ctx, queueName, 2*time.Second, 10*time.Second)
		if err != nil || got2 == nil {
			t.Fatalf("iter %d: re-dequeue after requeue: %v", i, err)
		}
		if err := q.Ack(ctx, queueName, got2.ID); err != nil {
			t.Fatalf("iter %d: ack: %v", i, err)
		}
	}
}
