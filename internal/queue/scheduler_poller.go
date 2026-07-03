package queue

import (
	"context"
	"log"
	"time"
)

type SchedulerPoller struct {
	q         *Queue
	queueName string
	interval  time.Duration
	cancel    context.CancelFunc
	done      chan struct{}
}

func NewSchedulerPoller(q *Queue, queueName string, interval time.Duration) *SchedulerPoller {
	return &SchedulerPoller{
		q:         q,
		queueName: queueName,
		interval:  interval,
		done:      make(chan struct{}),
	}
}

func (p *SchedulerPoller) Start(ctx context.Context) {
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
				n, err := p.q.PromoteDueScheduled(ctx, p.queueName)
				if err != nil {
					log.Printf("scheduler poller: error promoting due jobs: %v", err)
					continue
				}
				if n > 0 {
					log.Printf("scheduler poller: promoted %d scheduled job(s)", n)
				}
			}
		}
	}()
}

func (p *SchedulerPoller) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	<-p.done
}
