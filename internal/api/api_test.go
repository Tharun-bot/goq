package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Tharun-bot/goq/internal/model"
	"github.com/Tharun-bot/goq/internal/queue"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	q, err := queue.New("redis://localhost:6379")
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return NewServer(q)
}

func postJob(s *Server, queueName, payload string, priority int, idemKey string) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]interface{}{
		"queue": queueName, "payload": payload, "priority": priority,
	})
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestCreateAndGetJob(t *testing.T) {
	s := testServer(t)
	queueName := "api-test-" + time.Now().Format("150405.000000")

	rec := postJob(s, queueName, "send-email", 1, "")
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var created model.Job
	json.Unmarshal(rec.Body.Bytes(), &created)
	if created.Queue != queueName || created.Payload != "send-email" {
		t.Fatalf("unexpected job in response: %+v", created)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/jobs/"+created.ID, nil)
	getRec := httptest.NewRecorder()
	s.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getRec.Code, getRec.Body.String())
	}
	var fetched model.Job
	json.Unmarshal(getRec.Body.Bytes(), &fetched)
	if fetched.ID != created.ID {
		t.Fatalf("expected job id %s, got %s", created.ID, fetched.ID)
	}
}

func TestGetJobNotFound(t *testing.T) {
	s := testServer(t)
	req := httptest.NewRequest(http.MethodGet, "/jobs/does-not-exist", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestCreateJobMissingFields(t *testing.T) {
	s := testServer(t)
	body, _ := json.Marshal(map[string]interface{}{"queue": "some-queue"}) // missing payload
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestCreateJobMalformedJSON(t *testing.T) {
	s := testServer(t)
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader([]byte("{not json")))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestIdempotencyKeyPreventsDuplicateJob(t *testing.T) {
	s := testServer(t)
	queueName := "api-idem-test-" + time.Now().Format("150405.000000")
	idemKey := "client-retry-key-" + time.Now().Format("150405.000000")

	rec1 := postJob(s, queueName, "charge-card", 1, idemKey)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("expected 201 on first request, got %d: %s", rec1.Code, rec1.Body.String())
	}
	var job1 model.Job
	json.Unmarshal(rec1.Body.Bytes(), &job1)

	// Simulated retry: same Idempotency-Key, as if the client never saw the first response.
	rec2 := postJob(s, queueName, "charge-card", 1, idemKey)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 (existing job) on retry, got %d: %s", rec2.Code, rec2.Body.String())
	}
	var job2 model.Job
	json.Unmarshal(rec2.Body.Bytes(), &job2)

	if job1.ID != job2.ID {
		t.Fatalf("expected same job ID on retried request, got %s and %s", job1.ID, job2.ID)
	}

	// stats, err := s.q.Stats(nil, queueName)
	// _ = err
	// _ = stats
}

func TestQueueStats(t *testing.T) {
	s := testServer(t)
	queueName := "stats-test-" + time.Now().Format("150405.000000")

	for i := 0; i < 3; i++ {
		rec := postJob(s, queueName, "job", 1, "")
		if rec.Code != http.StatusCreated {
			t.Fatalf("setup enqueue failed: %d", rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/queues/"+queueName+"/stats", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var stats queue.QueueStats
	json.Unmarshal(rec.Body.Bytes(), &stats)
	if stats.Pending != 3 {
		t.Fatalf("expected pending=3, got %d", stats.Pending)
	}
}

func TestConcurrentJobCreationNoIDCollisions(t *testing.T) {
	s := testServer(t)
	queueName := "concurrent-test-" + time.Now().Format("150405.000000")

	const n = 50
	ids := make(chan string, n)
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		go func() {
			rec := postJob(s, queueName, "job", 1, "")
			if rec.Code != http.StatusCreated {
				errs <- fmt.Errorf("unexpected status %d", rec.Code)
				return
			}
			var j model.Job
			if err := json.Unmarshal(rec.Body.Bytes(), &j); err != nil {
				errs <- err
				return
			}
			ids <- j.ID
			errs <- nil
		}()
	}

	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
	}
	close(ids)

	seen := make(map[string]bool)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate job ID detected: %s", id)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("expected %d unique IDs, got %d", n, len(seen))
	}
}
