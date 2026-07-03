package queue

import (
	"context"
	"testing"
	"time"
)

func testQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := New("redis://localhost:6379")
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

func TestEnqueueDequeue(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()

	job, created, err := q.Enqueue(ctx, "test-queue", "hello world", 1)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if !created {
		t.Fatal("expected job to be newly created")
	}

	got, err := q.Dequeue(ctx, "test-queue", 2*time.Second, 30*time.Second)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if got == nil {
		t.Fatal("expected a job, got nil")
	}
	if got.ID != job.ID {
		t.Fatalf("expected job ID %s, got %s", job.ID, got.ID)
	}
	if got.Payload != "hello world" {
		t.Fatalf("expected payload 'hello world', got %s", got.Payload)
	}
}

func TestDequeueTimeout(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()

	job, err := q.Dequeue(ctx, "empty-queue-"+time.Now().String(), 1*time.Second, 30*time.Second)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if job != nil {
		t.Fatal("expected nil job on timeout, got a job")
	}
}

func TestAckRemovesFromProcessing(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()

	job, _, err := q.Enqueue(ctx, "ack-queue", "payload", 1)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := q.Dequeue(ctx, "ack-queue", 2*time.Second, 30*time.Second)
	if err != nil || got == nil {
		t.Fatalf("dequeue failed: %v", err)
	}

	if err := q.Ack(ctx, "ack-queue", job.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
}
