package backoff

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

// --- Jitter Implementations ---

// Jitter defines a strategy for adding randomness to a backoff delay.
type Jitter interface {
	String() string
	Apply(d time.Duration) time.Duration
}

// EqualJitter implements a jitter strategy where the delay is
// randomly distributed between half the delay and the full delay.
type EqualJitter struct{}

func NewEqualJitter() Jitter {
	return EqualJitter{}
}

func (j EqualJitter) String() string {
	return "equal jitter"
}

func (j EqualJitter) Apply(d time.Duration) time.Duration {
	half := max(0, d) / 2 //nolint:mnd
	if half == 0 {
		return 0
	}

	return half + time.Duration(rand.Int63n(int64(half)))
}

// FullJitter implements a jitter strategy where the delay is
// randomly distributed between 0 and the full delay.
type FullJitter struct{}

func NewFullJitter() Jitter {
	return FullJitter{}
}

func (j FullJitter) String() string {
	return "full jitter"
}

func (j FullJitter) Apply(d time.Duration) time.Duration {
	return time.Duration(rand.Int63n(int64(max(0, d))))
}

// Backoff defines the interface for calculating retry delays.
type Backoff interface {
	String() string
	// CalculateDelay returns the delay duration for the n-th retry attempt.
	CalculateDelay(n uint) time.Duration
	// GetJitter returns the jitter used by this backoff strategy, or nil if none.
	GetJitter() Jitter
}

// BackoffOptions holds configuration options for backoffs.
type BackoffOptions struct {
	// jitter is the Jitter to apply to the calculated delay. If nil, no jitter is applied.
	jitter Jitter
	// maxDelay is the maximum delay that can be returned by the backoff strategy.
	// 0 means no limit. This is applied before jitter.
	maxDelay time.Duration
}

type BackoffOption func(*BackoffOptions)

// NewDefaultOptions creates a new BackoffOptions with the provided options.
func NewDefaultOptions(opts ...BackoffOption) *BackoffOptions {
	o := &BackoffOptions{
		jitter:   nil,
		maxDelay: 0,
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

// WithJitter sets the jitter function for the retry strategy.
func WithJitter(jitterFn func() Jitter) BackoffOption {
	return func(o *BackoffOptions) {
		o.jitter = jitterFn()
	}
}

// WithMaxDelay sets the maximum delay for the retry strategy.
func WithMaxDelay(maxDelay time.Duration) BackoffOption {
	return func(o *BackoffOptions) {
		o.maxDelay = maxDelay
	}
}

// --- Constant Backoff Strategy ---

// Constant implements a retry strategy with a fixed delay.
type Constant struct {
	baseDelay time.Duration
	opts      *BackoffOptions
}

// NewConstant creates a new ConstantBackoffStrategy with the given delay and options.
func NewConstant(
	d time.Duration,
	opts ...BackoffOption,
) *Constant {
	return &Constant{
		baseDelay: max(0, d),
		opts:      NewDefaultOptions(opts...),
	}
}

func (s Constant) String() string {
	return fmt.Sprintf("constant with %s", jitterString(s.opts.jitter))
}

// CalculateDelay returns the fixed delay duration for any retry attempt.
func (s Constant) CalculateDelay(_ uint) time.Duration {
	return applyOptions(s.baseDelay, s.opts)
}

// GetJitter returns the jitter used by this backoff strategy, or nil if none.
func (s Constant) GetJitter() Jitter {
	return getJitter(s.opts)
}

// --- Linear Backoff Strategy ---

// Linear implements a retry strategy with linearly increasing delays,
// with a base delay incremented by a fixed step for each retry attempt.
type Linear struct {
	baseDelay time.Duration
	step      time.Duration
	opts      *BackoffOptions
}

// NewLinear creates a new LinearStrategy with the given base delay, step, and options.
func NewLinear(
	d, s time.Duration,
	opts ...BackoffOption,
) *Linear {
	return &Linear{
		baseDelay: max(0, d),
		step:      max(0, s),
		opts:      NewDefaultOptions(opts...),
	}
}

func (s Linear) String() string {
	return fmt.Sprintf("linear with %s", jitterString(s.opts.jitter))
}

// CalculateDelay returns the delay duration for the n-th retry attempt.
func (s Linear) CalculateDelay(n uint) time.Duration {
	steps := max(n, 1) - 1
	ns := float64(s.baseDelay) + float64(s.step)*float64(steps)
	delay := clampToMaxDuration(ns)

	return applyOptions(delay, s.opts)
}

// GetJitter returns the jitter used by this backoff strategy, or nil if none.
func (s Linear) GetJitter() Jitter {
	return getJitter(s.opts)
}

// --- Exponential Backoff Strategy ---

// Exponential implements a retry strategy where each delay is
// computed as baseDelay multiplied by growthFactor raised to the retry count.
type Exponential struct {
	baseDelay    time.Duration
	growthFactor float32
	opts         *BackoffOptions
}

// NewExponential creates a new Exponential strategy with the given base delay, factor, and options.
func NewExponential(
	d time.Duration,
	m float32,
	opts ...BackoffOption,
) *Exponential {
	return &Exponential{
		baseDelay:    max(0, d),
		growthFactor: max(1, m),
		opts:         NewDefaultOptions(opts...),
	}
}

func (s Exponential) String() string {
	return fmt.Sprintf("exponential with %s", jitterString(s.opts.jitter))
}

// CalculateDelay returns the delay duration for the n-th retry attempt.
func (s Exponential) CalculateDelay(n uint) time.Duration {
	n = max(n, 1)

	if s.growthFactor == 1 {
		return applyOptions(s.baseDelay, s.opts)
	}

	ns := float64(s.baseDelay) * math.Pow(float64(s.growthFactor), float64(n-1))
	delay := clampToMaxDuration(ns)

	return applyOptions(delay, s.opts)
}

// GetJitter returns the jitter used by this backoff strategy, or nil if none.
func (s Exponential) GetJitter() Jitter {
	return getJitter(s.opts)
}

// applyOptions applies the backoff options to the given duration.
func applyOptions(d time.Duration, opts *BackoffOptions) time.Duration {
	if opts == nil {
		return d
	}

	if md := opts.maxDelay; md > 0 && d > md {
		d = md
	}

	if j := opts.jitter; j != nil {
		d = j.Apply(d)
	}

	return max(0, d)
}

// getJitter extracts the jitter from the backoff options, or returns nil if none.
func getJitter(opts *BackoffOptions) Jitter {
	if opts == nil {
		return nil
	}

	return opts.jitter
}

func jitterString(j Jitter) string {
	if j == nil {
		return "no jitter"
	}

	return j.String()
}

// clampToMaxDuration clamps a float64 nanosecond value to a valid time.Duration.
func clampToMaxDuration(ns float64) time.Duration {
	if ns >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}

	return time.Duration(ns)
}
