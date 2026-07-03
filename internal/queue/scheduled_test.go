package queue

import (
	"context"
	"testing"
	"time"
)

func TestScheduledJobNotAvailableImmediately(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	queueName := "sched-test-" + time.Now().Format("150405.000000")

	_, created, err := q.EnqueueDelayed(ctx, queueName, "payload", 1, time.Now().Add(3*time.Second))
	if err != nil {
		t.Fatalf("enqueue delayed: %v", err)
	}
	if !created {
		t.Fatal("expected job to be created")
	}

	// Should NOT be dequeue-able yet — it's not on the main queue.
	got, err := q.Dequeue(ctx, queueName, 500*time.Millisecond, 30*time.Second)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if got != nil {
		t.Fatal("expected no job available yet, but got one")
	}
}

func TestScheduledJobAvailableAfterRunAt(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	queueName := "sched-test-" + time.Now().Format("150405.000000")

	job, _, err := q.EnqueueDelayed(ctx, queueName, "payload", 1, time.Now().Add(1*time.Second))
	if err != nil {
		t.Fatalf("enqueue delayed: %v", err)
	}

	time.Sleep(1200 * time.Millisecond)

	promoted, err := q.PromoteDueScheduled(ctx, queueName)
	if err != nil {
		t.Fatalf("promote due scheduled: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("expected 1 promoted, got %d", promoted)
	}

	got, err := q.Dequeue(ctx, queueName, 2*time.Second, 30*time.Second)
	if err != nil || got == nil {
		t.Fatalf("dequeue after promotion: %v", err)
	}
	if got.ID != job.ID {
		t.Fatalf("expected job %s, got %s", job.ID, got.ID)
	}
}

func TestSchedulerPollerNoDoublePromoteUnderConcurrency(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	queueName := "sched-race-test-" + time.Now().Format("150405.000000")

	if _, _, err := q.EnqueueDelayed(ctx, queueName, "payload", 1, time.Now().Add(500*time.Millisecond)); err != nil {
		t.Fatalf("enqueue delayed: %v", err)
	}

	time.Sleep(700 * time.Millisecond)

	// Simulate two concurrent poller instances racing on the same due job.
	results := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() {
			n, err := q.PromoteDueScheduled(ctx, queueName)
			if err != nil {
				t.Errorf("promote: %v", err)
				results <- 0
				return
			}
			results <- n
		}()
	}

	total := 0
	total += <-results
	total += <-results

	if total != 1 {
		t.Fatalf("expected exactly 1 total promotion across both racers (atomicity check), got %d", total)
	}
}
