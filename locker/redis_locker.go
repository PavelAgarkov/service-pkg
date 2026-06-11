package locker

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	lockKeyPrefix = "gitlab.wildberries.ru/whs-ai/whs-ai/github.com/PavelAgarkov/service-pkg-block-prefix-"

	// Скрипт для взятия блокировки
	lockScript = `return redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) and 1 or 0`

	// Скрипт для снятия блокировки
	unlockScript = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`

	// Скрипт для продления блокировки
	extendTTLScript = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end`
)

type RedisLocker struct {
	redisClient *redis.Client
}

type LockerConfig struct {
	Address  string
	Username string
	Password string
	DB       int
}

func NewLocker(c *redis.Client) Locker {
	return &RedisLocker{
		redisClient: c,
	}
}

func (locker *RedisLocker) Lock(ctx context.Context, key, value string, expiration time.Duration) (bool, error) {
	result, err := locker.redisClient.Eval(ctx, lockScript, []string{lockKeyPrefix + key}, value, expiration.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("eval: %v", err)
	}

	return result == 1, nil
}

func (locker *RedisLocker) Unlock(ctx context.Context, key, value string) (bool, error) {
	result, err := locker.redisClient.Eval(ctx, unlockScript, []string{lockKeyPrefix + key}, value).Int()
	if err != nil {
		return false, fmt.Errorf("eval: %v", err)
	}

	return result == 1, nil
}

func (locker *RedisLocker) ExtendLockTTL(ctx context.Context, key, value string, expiration time.Duration) (bool, error) {
	result, err := locker.redisClient.Eval(ctx, extendTTLScript, []string{lockKeyPrefix + key}, value, expiration.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("eval: %v", err)
	}

	return result == 1, nil
}
