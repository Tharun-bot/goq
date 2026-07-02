package model

import "time"

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusDead       JobStatus = "dead_letter"
)

type Job struct {
	ID         string    `json:"id"`
	Queue      string    `json:"queue"`
	Payload    string    `json:"payload"`
	Priority   int       `json:"priority"`
	Status     JobStatus `json:"status"`
	RetryCount int       `json:"retry_count"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}
