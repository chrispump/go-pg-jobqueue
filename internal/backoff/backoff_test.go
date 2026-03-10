package backoff_test

import (
	"testing"
	"time"

	"github.com/chrispump/go-pg-jobqueue/internal/backoff"
)

const exponentialFactor = 2.0

// TestConstantBackoff verifies that constant backoff returns the same delay for all attempts.
func TestConstantBackoff(t *testing.T) {
	baseDelay := 2 * time.Second
	b := backoff.NewConstant(baseDelay)

	var attempt uint
	for attempt = 1; attempt <= 5; attempt++ {
		delay := b.CalculateDelay(attempt)
		if delay != baseDelay {
			t.Errorf("attempt %d: expected delay %v, got %v", attempt, baseDelay, delay)
		}
	}
}

// TestLinearBackoff verifies that linear backoff increases linearly with each attempt.
func TestLinearBackoff(t *testing.T) {
	baseDelay := 2 * time.Second
	step := 3 * time.Second
	b := backoff.NewLinear(baseDelay, step)

	expected := []time.Duration{
		2 * time.Second,  // attempt 1: base
		5 * time.Second,  // attempt 2: base + step
		8 * time.Second,  // attempt 3: base + 2*step
		11 * time.Second, // attempt 4: base + 3*step
		14 * time.Second, // attempt 5: base + 4*step
	}

	var attempt uint
	for attempt = 1; attempt <= 5; attempt++ {
		delay := b.CalculateDelay(attempt)
		if delay != expected[attempt-1] {
			t.Errorf("attempt %d: expected delay %v, got %v", attempt, expected[attempt-1], delay)
		}
	}
}

// TestExponentialBackoff verifies that exponential backoff grows exponentially.
func TestExponentialBackoff(t *testing.T) {
	baseDelay := 2 * time.Second
	factor := float32(exponentialFactor)
	b := backoff.NewExponential(baseDelay, factor)

	expected := []time.Duration{
		2 * time.Second,  // attempt 1: base * 2^0
		4 * time.Second,  // attempt 2: base * 2^1
		8 * time.Second,  // attempt 3: base * 2^2
		16 * time.Second, // attempt 4: base * 2^3
		32 * time.Second, // attempt 5: base * 2^4
	}

	var attempt uint
	for attempt = 1; attempt <= 5; attempt++ {
		delay := b.CalculateDelay(attempt)
		if delay != expected[attempt-1] {
			t.Errorf("attempt %d: expected delay %v, got %v", attempt, expected[attempt-1], delay)
		}
	}
}

// TestBackoffEdgeCases verifies that backoff strategies handle edge cases correctly.
func TestBackoffEdgeCases(t *testing.T) {
	baseDelay := 2 * time.Second

	tests := []struct {
		name     string
		backoff  backoff.Backoff
		attempt  uint
		expected time.Duration
	}{
		{"constant_zero", backoff.NewConstant(baseDelay), 0, baseDelay},
		{"linear_zero", backoff.NewLinear(baseDelay, time.Second), 0, baseDelay},
		{"exponential_zero", backoff.NewExponential(baseDelay, exponentialFactor), 0, baseDelay},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay := tt.backoff.CalculateDelay(tt.attempt)
			if delay != tt.expected {
				t.Errorf("expected delay %v, got %v", tt.expected, delay)
			}
		})
	}
}

// TestBackoffWithMaxDelay verifies that max delay option works correctly.
func TestBackoffWithMaxDelay(t *testing.T) {
	baseDelay := 1 * time.Second
	maxDelay := 5 * time.Second
	b := backoff.NewExponential(baseDelay, 2.0, backoff.WithMaxDelay(maxDelay))

	// Exponential would be: 1s, 2s, 4s, 8s, 16s...
	// With max 5s: 1s, 2s, 4s, 5s, 5s...
	expected := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		5 * time.Second, // capped
		5 * time.Second, // capped
	}

	var attempt uint
	for attempt = 1; attempt <= 5; attempt++ {
		delay := b.CalculateDelay(attempt)
		if delay != expected[attempt-1] {
			t.Errorf("attempt %d: expected delay %v, got %v", attempt, expected[attempt-1], delay)
		}
	}
}

// TestGetJitter verifies that GetJitter returns nil when no jitter is configured
// and a non-nil value when jitter is configured.
func TestGetJitter(t *testing.T) {
	// Without jitter
	b := backoff.NewConstant(time.Second)
	if j := b.GetJitter(); j != nil {
		t.Errorf("expected nil jitter, got %v", j)
	}

	// With full jitter
	bFull := backoff.NewConstant(time.Second, backoff.WithJitter(backoff.NewFullJitter))
	if j := bFull.GetJitter(); j == nil {
		t.Error("expected non-nil jitter for full jitter")
	}

	// With equal jitter
	bEqual := backoff.NewConstant(time.Second, backoff.WithJitter(backoff.NewEqualJitter))
	if j := bEqual.GetJitter(); j == nil {
		t.Error("expected non-nil jitter for equal jitter")
	}
}

// TestBackoffString verifies that String() method returns meaningful output.
func TestBackoffString(t *testing.T) {
	tests := []struct {
		name     string
		backoff  backoff.Backoff
		contains string
	}{
		{"constant_no_jitter", backoff.NewConstant(time.Second), "constant"},
		{
			"constant_full_jitter",
			backoff.NewConstant(time.Second, backoff.WithJitter(backoff.NewFullJitter)),
			"full jitter",
		},
		{"linear_no_jitter", backoff.NewLinear(time.Second, time.Second), "linear"},
		{
			"exponential_equal_jitter",
			backoff.NewExponential(
				time.Second,
				exponentialFactor,
				backoff.WithJitter(backoff.NewEqualJitter),
			),
			"equal jitter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.backoff.String()
			if s == "" {
				t.Error("expected non-empty string")
			}

			if len(tt.contains) > 0 && !contains(s, tt.contains) {
				t.Errorf("expected string to contain %q, got %q", tt.contains, s)
			}
		})
	}
}

// Helper function for string contains check.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) &&
			(s[:len(substr)] == substr ||
				s[len(s)-len(substr):] == substr ||
				containsInMiddle(s, substr))))
}

func containsInMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}
