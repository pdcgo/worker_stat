package streaming_compute

import (
	"context"
	"log/slog"
	"time"
)

type RunnerContext struct {
	ctx    context.Context
	cancel context.CancelFunc
	Error  error
}

func (r *RunnerContext) WithError(err error) error {
	if err == nil {
		return nil
	}

	if r.Error == nil {
		r.Error = err
	}
	r.cancel()
	return err
}

// Deadline implements [context.Context].
func (r *RunnerContext) Deadline() (deadline time.Time, ok bool) {
	return r.ctx.Deadline()
}

// Done implements [context.Context].
func (r *RunnerContext) Done() <-chan struct{} {
	return r.ctx.Done()
}

// Err implements [context.Context].
func (r *RunnerContext) Err() error {
	return r.ctx.Err()
}

// Value implements [context.Context].
func (r *RunnerContext) Value(key any) any {
	return r.ctx.Value(key)
}

func NewRunnerContext(ctx context.Context) *RunnerContext {
	ctx, cancel := context.WithCancel(ctx)
	return &RunnerContext{ctx, cancel, nil}
}

func RunPeriodically(name string, rctx *RunnerContext, d time.Duration, handler func() error) {
	var err error

	tic := time.NewTimer(d)

	go func() {
	Parent:
		for {
			select {
			case <-rctx.Done():
				break Parent
			case <-tic.C:
				defer tic.Reset(d)

				slog.Debug("executing",
					"name", name,
				)
				err = handler()
				tic.Reset(d)

				if err != nil {
					slog.Error(err.Error(),
						"name", name,
					)
					rctx.WithError(err)
					break Parent
				}

			}
		}
	}()
}

func Run(name string, rctx *RunnerContext, handler func() error) {
	slog.Debug("executing",
		"name", name,
	)

	go func() {
		err := handler()
		if err != nil {
			rctx.WithError(err)
		}
		slog.Info("replication finish")
	}()

}
