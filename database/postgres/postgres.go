package postgres

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Configs struct {
	Host     string
	Port     string
	Username string
	Password string
	Database string
	SSLMode  string

	MaxOpenedConnections int

	ApplicationName string

	ConnectionMaxIdleTime time.Duration
	ConnectionMaxLifeTime time.Duration
	HealthCheckPeriod     time.Duration
	ConnectTimeout        time.Duration
	MaxConnLifeTimeJitter time.Duration
}

type Connection struct {
	pool *pgxpool.Pool
}

func NewPostgresConnection(ctx context.Context, config Configs) *Connection {
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		config.Username, config.Password, config.Host, config.Port, config.Database, config.SSLMode,
	)

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Printf("Failed to parse pgxpool config: %v", err)
	}
	if poolConfig == nil {
		log.Printf("pgxpool config is nil")
		return nil
	}

	// MaxConns — максимальное число одновременных соединений в пуле.
	// Ограничивает общий лимит, чтобы не забить сервер БД и не упереться в max_connections на Postgres.
	// У нас это 20, т.к. сервис малопоточный и нам не нужен большой пул.
	poolConfig.MaxConns = int32(config.MaxOpenedConnections)

	// Запоминаем общее число соединений, чтобы от него рассчитать минимальное.
	maxConnections := poolConfig.MaxConns

	// Минимальное количество постоянно открытых соединений в пуле.
	// Пул не будет опускаться ниже этого значения, даже если нагрузка низкая.
	// Это уменьшает задержки при «прогреве» под нагрузкой.
	minConnections := maxConnections / 4
	poolConfig.MinConns = minConnections

	// Максимальное время, в течение которого соединение может оставаться в пуле без использования.
	// По истечении времени соединение закрывается и создаётся заново при следующей необходимости.
	// Полезно, если сервер может «забыть» соединения или чтобы освобождать неиспользуемые.
	poolConfig.MaxConnIdleTime = config.ConnectionMaxIdleTime

	// Максимальное время жизни соединения в пуле, даже если оно активно используется.
	// По истечении срока соединение будет закрыто и заменено новым.
	// Это помогает равномерно обновлять соединения и избегать долгоживущих зависаний.
	poolConfig.MaxConnLifetime = config.ConnectionMaxLifeTime

	// jitter — случайное время, добавляемое к максимальному времени жизни соединения
	//poolConfig.MaxConnLifetimeJitter = 30 * time.Second
	poolConfig.MaxConnLifetimeJitter = config.MaxConnLifeTimeJitter

	// чтобы новые коннекты не висели вечно в момент глитчей
	//poolConfig.ConnConfig.ConnectTimeout = 1 * time.Second
	poolConfig.ConnConfig.ConnectTimeout = config.ConnectTimeout

	// периодическое health check для поддержания соединений
	//poolConfig.HealthCheckPeriod = 15 * time.Second
	poolConfig.HealthCheckPeriod = config.HealthCheckPeriod

	// Устанавливаем параметры сессии PostgreSQL (GUC) для всех соединений из пула.
	// application_name — для идентификации сессий в pg_stat_activity, логах и мониторинге.
	// Помогает быстро понять, какой сервис или воркер держит соединение.
	poolConfig.ConnConfig.RuntimeParams["application_name"] = config.ApplicationName

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Printf("Failed to connect to Postgres (pgxpool): %v", err)
	}

	return &Connection{pool: pool}
}

func (r *Connection) Stop() {
	r.pool.Close()
}

func (r *Connection) GetPool() *pgxpool.Pool {
	return r.pool
}
