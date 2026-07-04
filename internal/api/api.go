package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Tharun-bot/goq/internal/queue"
)

type Server struct {
	q   *queue.Queue
	mux *http.ServeMux
}

func NewServer(q *queue.Queue) *Server {
	s := &Server{q: q, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /jobs", s.handleCreateJob)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("GET /queues/{name}/stats", s.handleQueueStats)
	s.mux.Handle("GET /metrics", promhttp.Handler())
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

type createJobRequest struct {
	Queue    string `json:"queue"`
	Payload  string `json:"payload"`
	Priority int    `json:"priority"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write json response: %v", err)
	}
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body: " + err.Error()})
		return
	}
	if req.Queue == "" || req.Payload == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "queue and payload are required"})
		return
	}

	ctx := r.Context()
	idemKey := r.Header.Get("Idempotency-Key")

	if idemKey != "" {
		candidateID := uuid.NewString()
		existingID, claimed, err := s.q.ClaimIdempotencyKey(ctx, idemKey, candidateID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to check idempotency key"})
			return
		}
		if !claimed {
			existingJob, err := s.q.GetJob(ctx, existingID)
			if err != nil || existingJob == nil {
				writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to fetch existing job for idempotency key"})
				return
			}
			writeJSON(w, http.StatusOK, existingJob) // 200, not 201: this is a retry, nothing new was created
			return
		}
		job, _, err := s.q.EnqueueWithID(ctx, candidateID, req.Queue, req.Payload, req.Priority)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to enqueue job"})
			return
		}
		writeJSON(w, http.StatusCreated, job)
		return
	}

	job, _, err := s.q.Enqueue(ctx, req.Queue, req.Payload, req.Priority)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to enqueue job"})
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.q.GetJob(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to fetch job"})
		return
	}
	if job == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "job not found"})
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleQueueStats(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	stats, err := s.q.Stats(r.Context(), name)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to fetch stats"})
		return
	}
	writeJSON(w, http.StatusOK, stats)
}
