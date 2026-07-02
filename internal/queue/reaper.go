package queue

import (
	"context"
	"log"
	"time"
)

type Reaper struct {
	q         *Queue
	queueName string
	interval  time.Duration
	cancel    context.CancelFunc
	done      chan struct{}
}

func NewReaper(q *Queue, queueName string, interval time.Duration) *Reaper {
	return &Reaper{
		q:         q,
		queueName: queueName,
		interval:  interval,
		done:      make(chan struct{}),
	}
}

func (r *Reaper) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	go func() {
		defer close(r.done)
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n, err := r.q.RequeueStale(ctx, r.queueName)
				if err != nil {
					log.Printf("reaper: error requeuing stale jobs: %v", err)
					continue
				}
				if n > 0 {
					log.Printf("reaper: requeued %d stale job(s)", n)
				}
			}
		}
	}()
}

func (r *Reaper) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	<-r.done
}
