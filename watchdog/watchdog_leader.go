package watchdog

import (
	"context"
	"math/rand"
	"time"

	"github.com/PavelAgarkov/service-pkg/locker"
	"github.com/PavelAgarkov/service-pkg/utils"

	"github.com/google/uuid"
)

const (
	DefaultLeaderExpiration = 30 * time.Second

	ShkOnPlaceStreamProcessorDownloader = "watchdog_shk_on_place_stream_processor_downloader"
	Cron                                = "watchdog_cron"
	ClickhouseBusher                    = "watchdog_clickhouse_busher"

	LostAcquire  = 1
	TakenAcquire = 2
)

type Config struct {
	ElectionName string
	Expiration   time.Duration
}

type RedisWatchdogLeader struct {
	ctx    context.Context
	cancel context.CancelFunc
	locker locker.Locker
}

func NewRedisWatchdogLeader(ctx context.Context, locker locker.Locker) *RedisWatchdogLeader {
	ctx, cancel := context.WithCancel(ctx)
	return &RedisWatchdogLeader{
		ctx:    ctx,
		cancel: cancel,
		locker: locker,
	}
}

func (rwl *RedisWatchdogLeader) Elect(cfg Config) <-chan int {
	if cfg.ElectionName == "" {
		panic("ElectionName is empty")
	}
	if cfg.Expiration <= 0 {
		cfg.Expiration = DefaultLeaderExpiration
	}

	watcher := make(chan int, 8) // 8 на случай моргания сети или редиса, чтобы не блокировать поток сразу

	utils.GoRecover(rwl.ctx, func(ctx context.Context) {
		defer close(watcher)

		value := uuid.NewString()
		renewIntervalJitter := cfg.Expiration/3 + time.Duration(rand.Int63n(int64(cfg.Expiration/10)))
		ticker := time.NewTicker(renewIntervalJitter)
		defer ticker.Stop()

		isLeader := false
		send := func(event int) {
			select {
			case <-ctx.Done():
				return
			case watcher <- event:
			}
		}

		// для выбора лидера сразу
		ok, _ := rwl.locker.Lock(ctx, cfg.ElectionName, value, cfg.Expiration)
		if ok {
			isLeader = true
			send(TakenAcquire)
		}

		for {
			select {
			case <-ctx.Done():
				if isLeader {
					_, _ = rwl.locker.Unlock(context.Background(), cfg.ElectionName, value)
					isLeader = false
					send(LostAcquire)
					return
				}
			case <-ticker.C:
				if !isLeader {
					ok, _ := rwl.locker.Lock(ctx, cfg.ElectionName, value, cfg.Expiration)
					if ok {
						isLeader = true
						send(TakenAcquire)
					}
					continue
				}

				ok, _ := rwl.locker.ExtendLockTTL(ctx, cfg.ElectionName, value, cfg.Expiration)
				if !ok {
					isLeader = false
					send(LostAcquire)
				}
			}
		}
	})

	return watcher
}

func (rwl *RedisWatchdogLeader) Stop() {
	if rwl.cancel != nil {
		rwl.cancel()
	}
}
