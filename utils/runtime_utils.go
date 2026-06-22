package utils

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
)

func GoRecover(ctx context.Context, fn func(ctx context.Context)) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
					Msg:       "recovered from panic in goroutine",
					Error:     panicToError(r),
					Component: "utils",
					Method:    "GoRecover",
				})
			}
		}()

		select {
		case <-ctx.Done():
			logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
				Msg:       "goroutine cancelled before start",
				Component: "utils",
				Method:    "GoRecover",
			})
			return
		default:
		}

		fn(ctx)
	}()
}

func panicToError(value any) error {
	switch v := value.(type) {
	case error:
		return v
	case string:
		return errors.New(v)
	default:
		return fmt.Errorf("panic: %v", v)
	}
}

func Recover(ctx context.Context) {
	if r := recover(); r != nil {
		logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
			Msg:       "recovered from panic in goroutine",
			Error:     r.(error),
			Component: "utils",
			Method:    "Recover",
		})
	}
}

func WaitOrCtx(ctx context.Context, wait time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(wait):
		return nil
	}
}
