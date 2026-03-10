package job_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/job"
)

// --- Payload JSON Serialization Tests ---

func TestPayload_MarshalJSON_WithAllFields(t *testing.T) {
	payload := job.NewPayload("dummy",
		job.WithJobType(job.TypeNoop),
		job.WithStressLevel(1.5),
		job.WithErrorRate(0.75),
	)

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Verify JSON
	expected := `{"type":"noop","stress":1500,"error_rate":0.75,"data":"dummy"}`
	if string(data) != expected {
		t.Errorf("Expected JSON:\n%s\nGot:\n%s", expected, string(data))
	}
}

func TestPayload_MarshalJSON_WithNilOpts(t *testing.T) {
	payload := job.Payload{
		Data: "dummy",
		Opts: nil,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Should use default values
	expected := `{"type":"noop","stress":0,"error_rate":0,"data":"dummy"}`
	if string(data) != expected {
		t.Errorf("Expected JSON:\n%s\nGot:\n%s", expected, string(data))
	}
}

func TestPayload_UnmarshalJSON_ValidData(t *testing.T) {
	jsonData := `{"type":"noop","stress":1500,"error_rate":0.75,"data":"dummy"}`

	var payload job.Payload

	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if payload.Data != "dummy" {
		t.Errorf("Expected Data='dummy', got '%s'", payload.Data)
	}

	if payload.Opts.Type != job.TypeNoop {
		t.Errorf("Expected Type=%s, got %s", job.TypeNoop, payload.Opts.Type)
	}

	if payload.Opts.Stress != 1500 {
		t.Errorf("Expected Stress=1500, got %d", payload.Opts.Stress)
	}

	if payload.Opts.ErrorRate != 0.75 {
		t.Errorf("Expected ErrorRate=0.75, got %f", payload.Opts.ErrorRate)
	}
}

func TestPayload_MarshalUnmarshal_Full(t *testing.T) {
	tests := []struct {
		name     string
		payload  job.Payload
		wantOpts *job.PayloadOptions
		wantData string
	}{
		{
			name: "non_zero_values",
			payload: job.NewPayload("dummy-1",
				job.WithStressLevel(2.5),
				job.WithErrorRate(0.8),
			),
			wantOpts: &job.PayloadOptions{
				Type:      job.TypeNoop,
				Stress:    2500,
				ErrorRate: 0.8,
			},
			wantData: "dummy-1",
		},
		{
			name: "zero_values",
			payload: job.NewPayload("",
				job.WithStressLevel(0),
				job.WithErrorRate(0),
			),
			wantOpts: &job.PayloadOptions{
				Type:      job.TypeNoop,
				Stress:    0,
				ErrorRate: 0,
			},
			wantData: "",
		},
		{
			name: "max_error_rate",
			payload: job.NewPayload("dummy-2",
				job.WithErrorRate(1.0),
			),
			wantOpts: &job.PayloadOptions{
				Type:      job.TypeNoop,
				Stress:    0,
				ErrorRate: 1.0,
			},
			wantData: "dummy-2",
		},
		{
			name: "nil_opts",
			payload: job.Payload{
				Data: "dummy-3",
				Opts: nil,
			},
			wantOpts: &job.PayloadOptions{
				Type:      job.TypeNoop,
				Stress:    0,
				ErrorRate: 0,
			},
			wantData: "dummy-3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			data, err := json.Marshal(tt.payload)
			if err != nil {
				t.Fatalf("Marshal failed: %v", err)
			}

			// Unmarshal
			var result job.Payload

			err = json.Unmarshal(data, &result)
			if err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			// Verify
			if result.Data != tt.wantData {
				t.Errorf("Data mismatch: want=%s, got=%s", tt.wantData, result.Data)
			}

			if result.Opts.Type != tt.wantOpts.Type {
				t.Errorf("Type mismatch: want=%s, got=%s", tt.wantOpts.Type, result.Opts.Type)
			}

			if result.Opts.Stress != tt.wantOpts.Stress {
				t.Errorf("Stress mismatch: want=%d, got=%d", tt.wantOpts.Stress, result.Opts.Stress)
			}

			if result.Opts.ErrorRate != tt.wantOpts.ErrorRate {
				t.Errorf(
					"ErrorRate mismatch: want=%f, got=%f",
					tt.wantOpts.ErrorRate,
					result.Opts.ErrorRate,
				)
			}
		})
	}
}

func TestPayload_UnmarshalJSON_ClampsErrorRate(t *testing.T) {
	tests := []struct {
		name          string
		jsonData      string
		wantErrorRate float32
	}{
		{
			name:          "lower_zero_clamped_to_zero",
			jsonData:      `{"type":"noop","stress":0,"error_rate":-0.5,"data":"dummy"}`,
			wantErrorRate: 0.0,
		},
		{
			name:          "greater_one_clamped_to_one",
			jsonData:      `{"type":"noop","stress":0,"error_rate":1.5,"data":"dummy"}`,
			wantErrorRate: 1.0,
		},
		{
			name:          "valid_value_no_change",
			jsonData:      `{"type":"noop","stress":0,"error_rate":0.42,"data":"dummy"}`,
			wantErrorRate: 0.42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var payload job.Payload

			err := json.Unmarshal([]byte(tt.jsonData), &payload)
			if err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			if payload.Opts.ErrorRate != tt.wantErrorRate {
				t.Errorf("Expected ErrorRate=%f, got %f", tt.wantErrorRate, payload.Opts.ErrorRate)
			}
		})
	}
}

// --- PayloadOption Tests ---

func TestWithErrorRate_Clamping(t *testing.T) {
	tests := []struct {
		name     string
		input    float32
		expected float32
	}{
		{"lower_zero_clamped", -0.5, 0.0},
		{"zero_unchanged", 0.0, 0.0},
		{"valid_value", 0.42, 0.42},
		{"one_unchanged", 1.0, 1.0},
		{"greater_one_clamped", 1.5, 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := job.NewDefaultPayloadOptions(job.WithErrorRate(tt.input))
			if opts.ErrorRate != tt.expected {
				t.Errorf("Expected ErrorRate=%f, got %f", tt.expected, opts.ErrorRate)
			}
		})
	}
}

func TestWithStressLevel_NonNegative(t *testing.T) {
	tests := []struct {
		name     string
		input    float32 // in seconds
		expected uint64  // in milliseconds
	}{
		{"lower_zero_clamped", -1.5, 0},
		{"zero_unchanged", 0.0, 0},
		{"valid_small_value", 0.001, 1},
		{"valid_large_value", 10.0, 10_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := job.NewDefaultPayloadOptions(job.WithStressLevel(tt.input))
			if opts.Stress != tt.expected {
				t.Errorf("Expected Stress=%d, got %d", tt.expected, opts.Stress)
			}
		})
	}
}

func TestWithJobType(t *testing.T) {
	opts := job.NewDefaultPayloadOptions(job.WithJobType(job.TypeNoop))
	if opts.Type != job.TypeNoop {
		t.Errorf("Expected Type=%s, got %s", job.TypeNoop, opts.Type)
	}
}

// --- Job Construction Tests ---

func TestNew_DefaultStatus(t *testing.T) {
	payload := job.NewPayload("dummy")
	j := job.New(payload)

	if j.Status != job.StatusPending {
		t.Errorf("Expected Status=%s, got %s", job.StatusPending, j.Status)
	}

	if j.Payload.Data != "dummy" {
		t.Errorf("Expected Payload.Data='dummy', got '%s'", j.Payload.Data)
	}

	if j.ID != 0 {
		t.Errorf("Expected ID=0 for new job, got %d", j.ID)
	}
}

func TestNewFromData_AllFields(t *testing.T) {
	now := time.Now()
	lockUntil := now.Add(5 * time.Minute)
	payload := job.NewPayload("test-data")

	j := job.NewFromData(
		42,
		payload,
		job.StatusProcessing,
		3,
		now,
		lockUntil,
		"worker-1",
		"random-token",
	)

	if j.ID != 42 {
		t.Errorf("Expected ID=42, got %d", j.ID)
	}

	if j.Status != job.StatusProcessing {
		t.Errorf("Expected Status=%s, got %s", job.StatusProcessing, j.Status)
	}

	if j.Attempts != 3 {
		t.Errorf("Expected Attempts=3, got %d", j.Attempts)
	}

	if j.LockedBy != "worker-1" {
		t.Errorf("Expected LockedBy='worker-1', got '%s'", j.LockedBy)
	}

	if j.LockToken != "random-token" {
		t.Errorf("Expected LockToken='random-token', got '%s'", j.LockToken)
	}

	if !j.NotBefore.Equal(now) {
		t.Errorf("NotBefore mismatch")
	}

	if !j.LockUntil.Equal(lockUntil) {
		t.Errorf("LockUntil mismatch")
	}
}

// --- Default Values Tests ---

func TestNewDefaultPayloadOptions_Defaults(t *testing.T) {
	opts := job.NewDefaultPayloadOptions()

	if opts.Type != job.TypeNoop {
		t.Errorf("Expected default Type=%s, got %s", job.TypeNoop, opts.Type)
	}

	if opts.Stress != 0 {
		t.Errorf("Expected default Stress=0, got %d", opts.Stress)
	}

	if opts.ErrorRate != 0.0 {
		t.Errorf("Expected default ErrorRate=0.0, got %f", opts.ErrorRate)
	}
}

func TestNewPayload_ChainingOptions(t *testing.T) {
	payload := job.NewPayload("data",
		job.WithJobType(job.TypeNoop),
		job.WithStressLevel(1.0),
		job.WithErrorRate(0.5),
	)

	if payload.Data != "data" {
		t.Errorf("Expected Data='data', got '%s'", payload.Data)
	}

	if payload.Opts.Type != job.TypeNoop {
		t.Errorf("Expected Type=%s, got %s", job.TypeNoop, payload.Opts.Type)
	}

	if payload.Opts.Stress != 1000 {
		t.Errorf("Expected Stress=1000, got %d", payload.Opts.Stress)
	}

	if payload.Opts.ErrorRate != 0.5 {
		t.Errorf("Expected ErrorRate=0.5, got %f", payload.Opts.ErrorRate)
	}
}
