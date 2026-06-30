package utils

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

func GoRecover(ctx context.Context, fn func(ctx context.Context)) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("recovered from panic in goroutine: %v", r)
			}
		}()

		select {
		case <-ctx.Done():
			log.Printf("goroutine cancelled before start: %v", ctx.Err())
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
		log.Printf("recovered from panic in goroutine: %v", r)
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
