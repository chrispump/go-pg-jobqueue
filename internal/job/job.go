package job

import (
	"encoding/json"
	"time"
)

// Status describes the current lifecycle state of a job as stored in the queue.
type Status string

const (
	// stressScaleFactor converts a float32 stress level (0.0–1.0) to an integer microsecond value.
	stressScaleFactor = 1_000
)

const (
	// StatusPending means the job is enqueued and ready (or scheduled) to be picked up.
	StatusPending Status = "pending"
	// StatusProcessing means a worker currently holds the lease/lock and is executing the job.
	StatusProcessing Status = "processing"
	// StatusCompleted means the job finished successfully.
	StatusCompleted Status = "completed"
	// StatusFailed means the job has no more retries left and has failed permanently.
	StatusFailed Status = "failed"
)

// Type identifies the handler/payload kind of a job.
type Type string

const (
	// TypeNoop represents a no-op job that only simulates processing time and failure rate.
	TypeNoop Type = "noop"
)

// PayloadOption modifies PayloadOptions (functional options pattern).
type PayloadOption func(*PayloadOptions)

// PayloadOptions are additional handler parameters persisted as part of the payload.
type PayloadOptions struct {
	// Type selects the handler to execute. Defaults to TypeNoop.
	Type Type
	// Stress is an artificial processing duration in milliseconds.
	Stress uint64
	// ErrorRate is the probability of a handler returning an error (0.0 to 1.0).
	ErrorRate float32
}

// NewDefaultPayloadOptions creates PayloadOptions with defaults and applies any modifiers.
func NewDefaultPayloadOptions(opts ...PayloadOption) *PayloadOptions {
	o := &PayloadOptions{
		Type:      TypeNoop,
		Stress:    0,
		ErrorRate: 0.0,
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

func WithJobType(t Type) PayloadOption {
	return func(o *PayloadOptions) {
		o.Type = t
	}
}

func WithStressLevel(s float32) PayloadOption {
	return func(o *PayloadOptions) {
		if s <= 0 {
			o.Stress = 0

			return
		}

		o.Stress = uint64(float64(s) * stressScaleFactor)
	}
}

func WithErrorRate(er float32) PayloadOption {
	return func(o *PayloadOptions) {
		o.ErrorRate = min(max(0.0, er), 1.0)
	}
}

// Payload is the persisted job payload.
type Payload struct {
	// Data is arbitrary job data
	Data string
	// Opts contains handler/scenario parameters
	Opts *PayloadOptions
}

// payloadJSON is an internal struct used for custom JSON serialization of Payload.
type payloadJSON struct {
	Type      Type    `json:"type"`
	Stress    uint64  `json:"stress"`
	ErrorRate float32 `json:"error_rate"`
	Data      string  `json:"data"`
}

// NewPayload creates a payload with default options and applies the given modifiers.
func NewPayload(data string, opts ...PayloadOption) Payload {
	return Payload{
		Data: data,
		Opts: NewDefaultPayloadOptions(opts...),
	}
}

// MarshalJSON implements custom JSON serialization for Payload.
func (p Payload) MarshalJSON() ([]byte, error) {
	opts := p.Opts
	if opts == nil {
		opts = NewDefaultPayloadOptions()
	}

	return json.Marshal(payloadJSON{
		Type:      opts.Type,
		Stress:    opts.Stress,
		ErrorRate: opts.ErrorRate,
		Data:      p.Data,
	})
}

// UnmarshalJSON implements custom JSON deserialization for Payload.
func (p *Payload) UnmarshalJSON(data []byte) error {
	var pj payloadJSON
	if err := json.Unmarshal(data, &pj); err != nil {
		return err
	}

	p.Data = pj.Data

	opts := NewDefaultPayloadOptions()
	if pj.Type != "" {
		opts.Type = pj.Type
	}

	opts.Stress = pj.Stress
	opts.ErrorRate = min(max(0.0, pj.ErrorRate), 1.0)

	p.Opts = opts

	return nil
}

// Job represents a unit of work to be processed by a worker as it is stored in the queue.
type Job struct {
	// ID is the database primary key.
	ID int64
	// Payload contains handler type and scenario parameters.
	Payload Payload
	// Status is the current lifecycle state.
	Status Status
	// Attempts counts how many processing attempts were made so far.
	Attempts uint
	// NotBefore schedules a job for later execution.
	NotBefore time.Time
	// LockUntil is the lease/lock expiration timestamp while a worker processes the job.
	LockUntil time.Time
	// LockedBy identifies the worker holding the lock.
	LockedBy string
	// LockToken disambiguates locks (useful to avoid stale unlocks).
	LockToken string
}

// New creates a new pending job from a payload.
func New(payload Payload) Job {
	return Job{
		Payload: payload,
		Status:  StatusPending,
	}
}

// NewFromData builds a Job from persisted fields (typically loaded from the database).
func NewFromData(
	id int64,
	payload Payload,
	status Status,
	attempts uint,
	notBefore time.Time,
	lockUntil time.Time,
	lockedBy string,
	lockToken string,
) *Job {
	return &Job{
		ID:        id,
		Payload:   payload,
		Status:    status,
		Attempts:  attempts,
		NotBefore: notBefore,
		LockUntil: lockUntil,
		LockedBy:  lockedBy,
		LockToken: lockToken,
	}
}
