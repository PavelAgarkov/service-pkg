package redis

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/redis/go-redis/v9"
)

var defaultOptions = []Option{
	WithDialTimeout(10 * time.Second),
	WithReadTimeout(3 * time.Second),
	WithMaxRetries(5),
	WithMaxRetryBackoff(2 * time.Second),
}

type Option interface {
	applyStandalone(*redis.Options)
	applyFailover(*redis.FailoverOptions)
}

type dialTimeoutOption time.Duration

func WithDialTimeout(timeout time.Duration) Option {
	return dialTimeoutOption(timeout)
}

func (o dialTimeoutOption) applyStandalone(opts *redis.Options) {
	opts.DialTimeout = time.Duration(o)
}

func (o dialTimeoutOption) applyFailover(opts *redis.FailoverOptions) {
	opts.DialTimeout = time.Duration(o)
}

type readTimeoutOption time.Duration

func WithReadTimeout(timeout time.Duration) Option {
	return readTimeoutOption(timeout)
}

func (o readTimeoutOption) applyStandalone(opts *redis.Options) {
	opts.ReadTimeout = time.Duration(o)
}

func (o readTimeoutOption) applyFailover(opts *redis.FailoverOptions) {
	opts.ReadTimeout = time.Duration(o)
}

type maxRetriesOption int

func WithMaxRetries(retries int) Option {
	return maxRetriesOption(retries)
}

func (o maxRetriesOption) applyStandalone(opts *redis.Options) {
	opts.MaxRetries = int(o)
}

func (o maxRetriesOption) applyFailover(opts *redis.FailoverOptions) {
	opts.MaxRetries = int(o)
}

type maxRetryBackoffOption time.Duration

func WithMaxRetryBackoff(backoff time.Duration) Option {
	return maxRetryBackoffOption(backoff)
}

func (o maxRetryBackoffOption) applyStandalone(opts *redis.Options) {
	opts.MaxRetryBackoff = time.Duration(o)
}

func (o maxRetryBackoffOption) applyFailover(opts *redis.FailoverOptions) {
	opts.MaxRetryBackoff = time.Duration(o)
}

type passwordOption string

func WithPassword(password string) Option {
	return passwordOption(password)
}

func (o passwordOption) applyStandalone(opts *redis.Options) {
	opts.Password = string(o)
}

type sentinelPasswordOption string

func WithSentinelPassword(password string) Option {
	return sentinelPasswordOption(password)
}

func (o sentinelPasswordOption) applyStandalone(opts *redis.Options) {}

func (o sentinelPasswordOption) applyFailover(opts *redis.FailoverOptions) {
	opts.SentinelPassword = string(o)
}

func (o passwordOption) applyFailover(opts *redis.FailoverOptions) {
	opts.Password = string(o)
}

func NewRedisClient(dsn string, options ...Option) (*redis.Client, error) {
	ctx := context.Background()

	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	allOptions := make([]Option, 0)
	allOptions = append(allOptions, defaultOptions...)
	allOptions = append(allOptions, options...)

	var client *redis.Client

	switch {
	case u.Query().Get("master_name") != "":
		client, err = makeSentinelClient(dsn, allOptions...)
	default:
		client, err = makeStandaloneClient(dsn, allOptions...)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create redis client: %w", err)
	}

	if err := client.Ping(ctx).Err(); err != nil {
		fmt.Println("failed to connect to Redis:", err)
	}

	return client, nil
}

func makeSentinelClient(dsn string, options ...Option) (*redis.Client, error) {
	opts, err := redis.ParseFailoverURL(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse redis failover dsn: %w", err)
	}

	for _, option := range options {
		option.applyFailover(opts)
	}

	fmt.Println("Sentinel client options:", opts)
	fmt.Printf(
		"master=%s sentinelAddrs=%v password=%q sentinelPassword=%q\n",
		opts.MasterName,
		opts.SentinelAddrs,
		opts.Password,
		opts.SentinelPassword,
	)
	return redis.NewFailoverClient(opts), nil
}

func makeStandaloneClient(dsn string, options ...Option) (*redis.Client, error) {
	opts, err := redis.ParseURL(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse redis dsn: %w", err)
	}

	for _, option := range options {
		option.applyStandalone(opts)
	}

	fmt.Println("Standalone client options:", opts)
	return redis.NewClient(opts), nil
}
