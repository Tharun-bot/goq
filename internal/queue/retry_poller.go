package queue

import (
	"context"
	"log"
	"time"
)

type RetryPoller struct {
	q         *Queue
	queueName string
	interval  time.Duration
	cancel    context.CancelFunc
	done      chan struct{}
}

func NewRetryPoller(q *Queue, queueName string, interval time.Duration) *RetryPoller {
	return &RetryPoller{
		q:         q,
		queueName: queueName,
		interval:  interval,
		done:      make(chan struct{}),
	}
}

func (p *RetryPoller) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	go func() {
		defer close(p.done)
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n, err := p.q.PromoteDueRetries(ctx, p.queueName)
				if err != nil {
					log.Printf("retry poller: error promoting due retries: %v", err)
					continue
				}
				if n > 0 {
					log.Printf("retry poller: promoted %d retry job(s)", n)
				}
			}
		}
	}()
}

func (p *RetryPoller) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	<-p.done
}
