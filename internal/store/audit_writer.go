package store

import (
	"context"
	"log"
	"time"
)

// AuditWriter buffers audit events and flushes them to Postgres in batches
// on a timer, off the hot path. If the buffer is full, events are dropped
// (logged, not blocked) — job processing must never wait on this.
type AuditWriter struct {
	store         *Store
	events        chan AuditEvent
	flushInterval time.Duration
	batchSize     int
	done          chan struct{}
	cancel        context.CancelFunc
}

func NewAuditWriter(store *Store, bufferSize, batchSize int, flushInterval time.Duration) *AuditWriter {
	return &AuditWriter{
		store:         store,
		events:        make(chan AuditEvent, bufferSize),
		flushInterval: flushInterval,
		batchSize:     batchSize,
		done:          make(chan struct{}),
	}
}

// Record enqueues an audit event without blocking. If the internal buffer is
// full, the event is dropped and logged — this is a deliberate tradeoff:
// losing an audit row is recoverable, stalling a worker is not.
func (w *AuditWriter) Record(event AuditEvent) {
	select {
	case w.events <- event:
	default:
		logDropped(event, errBufferFull)
	}
}

var errBufferFull = errBuffer{}

type errBuffer struct{}

func (errBuffer) Error() string { return "audit buffer full" }

func (w *AuditWriter) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	go func() {
		defer close(w.done)
		ticker := time.NewTicker(w.flushInterval)
		defer ticker.Stop()

		batch := make([]AuditEvent, 0, w.batchSize)

		flush := func() {
			if len(batch) == 0 {
				return
			}
			writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := w.store.InsertBatch(writeCtx, batch); err != nil {
				log.Printf("audit writer: batch insert failed, dropping %d event(s): %v", len(batch), err)
			}
			cancel()
			batch = batch[:0]
		}

		for {
			select {
			case <-ctx.Done():
				flush() // best-effort final flush on shutdown
				return
			case e := <-w.events:
				batch = append(batch, e)
				if len(batch) >= w.batchSize {
					flush()
				}
			case <-ticker.C:
				flush()
			}
		}
	}()
}

func (w *AuditWriter) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	<-w.done
}
