package job

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

type Handler func(ctx context.Context, job *Job) error

type HandlerRegistry interface {
	Register(t Type, h Handler)
	GetHandler(t Type) (Handler, bool)
}

type DefaultJobHandlerRegistry struct {
	handlers map[Type]Handler
}

func NewDefaultHandlerRegistry() *DefaultJobHandlerRegistry {
	handlers := make(map[Type]Handler)
	handlers[TypeNoop] = NoopHandler

	return &DefaultJobHandlerRegistry{
		handlers: handlers,
	}
}

func (r *DefaultJobHandlerRegistry) Register(t Type, h Handler) {
	r.handlers[t] = h
}

func (r *DefaultJobHandlerRegistry) GetHandler(t Type) (Handler, bool) {
	handler, ok := r.handlers[t]

	return handler, ok
}

// --- Handlers ---

// NoopHandler simulates work by sleeping for the job's stress duration and
// randomly returning an error based on the configured error rate.
func NoopHandler(ctx context.Context, job *Job) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(job.Payload.Opts.Stress) * time.Millisecond):
		return maybeError(job.Payload.Opts.ErrorRate)
	}
}

// -- Helpers ---.
func maybeError(errorRate float32) error {
	errList := []error{
		errors.New("refusing to complete"),
		errors.New("this is not a bug, it's a statement"),
		errors.New("operation failed successfully"),
		errors.New("error thrown for educational purposes"),
		errors.New("could not fail"),
		errors.New("success was never an option"),
		errors.New("error about an error"),
		errors.New("this job has seen enough"),
		errors.New("done trying"),
	}

	if rand.Float32() < errorRate {
		return errList[rand.IntN(len(errList))]
	}

	return nil
}
