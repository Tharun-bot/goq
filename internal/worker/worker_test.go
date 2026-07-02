package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tharun-bot/goq/internal/model"
	"github.com/Tharun-bot/goq/internal/queue"
)

func testQueue(t *testing.T) *queue.Queue {
	t.Helper()
	q, err := queue.New("redis://localhost:6379")
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

func TestPoolProcessesAllJobs(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	queueName := "pool-test-" + time.Now().Format("150405.000000")

	const numJobs = 100
	for i := 0; i < numJobs; i++ {
		if _, _, err := q.Enqueue(ctx, queueName, "payload", 1); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	var processed int64
	handler := func(ctx context.Context, job *model.Job) error {
		atomic.AddInt64(&processed, 1)
		return nil
	}

	pool := NewPool(q, queueName, 5, handler)
	runCtx, cancel := context.WithCancel(context.Background())
	pool.Start(runCtx)

	deadline := time.Now().Add(10 * time.Second)
	for atomic.LoadInt64(&processed) < numJobs && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	pool.Stop()

	if got := atomic.LoadInt64(&processed); got != numJobs {
		t.Fatalf("expected %d jobs processed, got %d", numJobs, got)
	}
}

func TestPoolGracefulShutdown(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	queueName := "shutdown-test-" + time.Now().Format("150405.000000")

	started := make(chan struct{})
	release := make(chan struct{})

	handler := func(ctx context.Context, job *model.Job) error {
		close(started)
		<-release // block until test says go ahead, simulating a slow job
		return nil
	}

	if _, _, err := q.Enqueue(ctx, queueName, "slow-payload", 1); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	pool := NewPool(q, queueName, 1, handler)
	runCtx := context.Background()
	pool.Start(runCtx)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("job never started")
	}

	stopDone := make(chan struct{})
	go func() {
		pool.Stop() // should block until the in-flight job finishes
		close(stopDone)
	}()

	select {
	case <-stopDone:
		t.Fatal("Stop() returned before in-flight job finished")
	case <-time.After(200 * time.Millisecond):
		// expected: Stop() is still blocked because the job hasn't released yet
	}

	close(release) // let the job finish

	select {
	case <-stopDone:
		// good — Stop() unblocked after job completed
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() never returned after job completed")
	}
}
