package retry

import (
	"context"
	"math/rand"
	"time"

	"github.com/bufbuild/connect-go"
	"go.uber.org/zap"
)

func Jitter(d time.Duration, j float64) time.Duration {
	if d == 0 {
		return 0
	}
	if j == 0 {
		return d
	}
	// +/- jitter from our target
	mult := j * (rand.Float64()*2 - 1)
	return time.Duration(float64(d) * (1 + mult))
}

func Forever(ctx context.Context, l *zap.Logger, d time.Duration, j float64, fn func() error) error {
	var (
		err error
		i   int
	)
	for {
		if err = fn(); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		i++

		d := Jitter(d, j)
		l.Warn("retrying",
			zap.Duration("wait", d),
			zap.Int("attempt", i),
			zap.String("error", err.Error()),
		)

		t := time.NewTicker(d)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
}

func RetryFunc[T any](ctx context.Context, l *zap.Logger, n int, d time.Duration, j float64, fn func() (T, error)) (T, error) {
	var (
		v   T
		err error
		i   int
	)
	for {
		if v, err = fn(); err == nil {
			return v, nil
		}
		// first check if the context is done
		if ctx.Err() != nil {
			return v, ctx.Err()
		}

		// if n < 0, we are retrying forever
		if n > 0 && i == n {
			// all attempts exhausted
			return v, err
		}
		i++

		d := Jitter(d, j)
		l.Warn("retrying",
			zap.Duration("wait", d),
			zap.Int("attempt", i),
			zap.String("error", err.Error()),
		)

		t := time.NewTicker(d)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return v, ctx.Err()
		}
	}
}

func UnaryInterceptor(l *zap.Logger, n int, d time.Duration, j float64) connect.UnaryInterceptorFunc {
	interceptor := func(next connect.UnaryFunc) connect.UnaryFunc {
		return connect.UnaryFunc(func(
			ctx context.Context,
			req connect.AnyRequest,
		) (connect.AnyResponse, error) {
			if !req.Spec().IsClient || n == 0 {
				return next(ctx, req)
			}
			return RetryFunc(ctx, l, n, d, j, func() (connect.AnyResponse, error) {
				return next(ctx, req)
			})
		})
	}
	return connect.UnaryInterceptorFunc(interceptor)
}
