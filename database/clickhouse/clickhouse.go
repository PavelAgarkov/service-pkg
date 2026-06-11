package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/PavelAgarkov/service-pkg/utils"

	"github.com/ClickHouse/clickhouse-go/v2"
	ch "github.com/ClickHouse/clickhouse-go/v2"
)

type Clickhouse struct {
	Host            string        `mapstructure:"host"     envconfig:"HOST"`
	Port            int           `mapstructure:"port"     envconfig:"PORT"`
	Username        string        `mapstructure:"username" envconfig:"USERNAME"`
	Password        string        `mapstructure:"password" envconfig:"PASSWORD"`
	Database        string        `mapstructure:"database" envconfig:"DATABASE"`
	DialTimeout     time.Duration `mapstructure:"dial_timeout" envconfig:"DIAL_TIMEOUT"`
	MaxOpenConn     int           `mapstructure:"max_open_conn" envconfig:"MAX_OPEN_CONN"`
	MaxIdleConn     int           `mapstructure:"max_idle_conn" envconfig:"MAX_IDLE_CONN"`
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time" envconfig:"CONN_MAX_IDLE_TIME"`
	ConnMaxLifeTime time.Duration `mapstructure:"conn_max_life_time" envconfig:"CONN_MAX_LIFE_TIME"`
}

type Connection struct {
	mu   sync.Mutex
	conn *sql.DB
	cfg  Clickhouse
}

func NewClickhouseConnection(ctx context.Context, cfg Clickhouse) (*Connection, error) {
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 3 * time.Second
	}
	if cfg.MaxOpenConn == 0 {
		cfg.MaxOpenConn = 1
	}
	if cfg.MaxIdleConn == 0 {
		cfg.MaxIdleConn = 1
	}
	if cfg.ConnMaxIdleTime == 0 {
		cfg.ConnMaxIdleTime = 30 * time.Minute
	}
	if cfg.ConnMaxLifeTime == 0 {
		cfg.ConnMaxLifeTime = 24 * time.Hour
	}

	host := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	var opt *clickhouse.Options
	// temp hack
	if cfg.Database != "" {
		opt = &clickhouse.Options{
			Addr:        []string{host},
			Auth:        clickhouse.Auth{Username: cfg.Username, Password: cfg.Password, Database: cfg.Database},
			DialTimeout: cfg.DialTimeout,
			Protocol:    clickhouse.Native,
			Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		}
	} else {
		opt = &clickhouse.Options{
			Addr:        []string{host},
			Auth:        clickhouse.Auth{Username: cfg.Username, Password: cfg.Password},
			DialTimeout: cfg.DialTimeout,
			Protocol:    clickhouse.Native,
			Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		}
	}

	var (
		db        *sql.DB
		sleepStep = 3000 * time.Millisecond
	)

	db = clickhouse.OpenDB(opt)
	db.SetMaxOpenConns(cfg.MaxOpenConn)
	db.SetMaxIdleConns(cfg.MaxIdleConn)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	db.SetConnMaxLifetime(cfg.ConnMaxLifeTime)

	tryContext, cancelContext := utils.TimeoutNoDeadline(ctx, 2*time.Second)
	err := db.PingContext(tryContext)
	cancelContext()

	if err != nil {
		if err := utils.WaitOrCtx(ctx, sleepStep); err != nil {
			return nil, err
		}
	}

	return &Connection{conn: db, cfg: cfg}, nil
}

func (c *Connection) GetClickHouseConfig() Clickhouse {
	return c.cfg
}

func (c *Connection) Reconnect(ctx context.Context, cfg Clickhouse) error {
	newConn, err := NewClickhouseConnection(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to reconnect to Clickhouse: %w", err)
	}

	c.mu.Lock()
	old := c.conn
	c.conn = newConn.conn
	c.mu.Unlock()

	err = old.Close()
	if err != nil {
		logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
			Msg:       "Failed to close old Clickhouse connection",
			Error:     err,
			Component: "ClickhouseConnection",
			Method:    "Reconnect",
		})
		return fmt.Errorf("failed to close old Clickhouse connection: %w", err)
	}
	return nil
}

func (c *Connection) GetDB() *sql.DB {
	return c.conn
}

func (c *Connection) disconnectFromDB(ctx context.Context) error {
	if err := c.conn.Close(); err != nil {
		logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
			Msg:       "Failed to close Clickhouse connection",
			Error:     err,
			Component: "ClickhouseConnection",
			Method:    "disconnectFromDB",
		})
		return err
	}

	return nil
}

func (c *Connection) Shutdown(ctx context.Context) func() {
	return func() {
		if err := c.disconnectFromDB(ctx); err != nil {
			logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
				Msg:       "Error during Clickhouse shutdown",
				Error:     err,
				Component: "ClickhouseConnection",
				Method:    "Shutdown",
			})
		}
	}
}

// NeedReconnect возвращает true, если ошибку логично лечить
// полным закрытием подключения и открытием нового.
func NeedReconnect(err error) (bool, *ch.Exception) {
	var exc *ch.Exception
	if errors.As(err, &exc) {
		switch exc.Code {

		// Не та схема за HAProxy
		case
			60, // UNKNOWN_TABLE
			81, // UNKNOWN_DATABASE

			// Чистые сетевые проблемы
			3,   // UNEXPECTED_END_OF_FILE
			159, // TIMEOUT_EXCEEDED
			209, // SOCKET_TIMEOUT
			210, // NETWORK_ERROR

			// Read‑only и прочие узло‑специфичные сбои
			425,  // SYSTEM_ERROR
			999,  // KEEPER_EXCEPTION
			1002: // UNKNOWN_EXCEPTION
			return true, stubOrExc(exc, err)
		}
	}

	// Обрыв без кода (Deadline, EOF)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true, stubOrExc(exc, err)
	}

	return false, exc
}

// NeedWait ситуации, где помогает только пауза
func NeedWait(err error) (bool, time.Duration, *ch.Exception) {
	var exc *ch.Exception
	if errors.As(err, &exc) {
		switch exc.Code {
		case 201: // QUOTA_EXCEEDED (кластерный счётчик)
			return true, 5 * time.Second, stubOrExc(exc, err)
		case 202, 203: // TOO_MANY_SIMULTANEOUS_QUERIES / NO_FREE_CONNECTION
			return true, 1 * time.Second, stubOrExc(exc, err)
		case 252, 285: // TOO_MANY_PARTS / TOO_FEW_LIVE_REPLICAS
			return true, 2 * time.Second, stubOrExc(exc, err)
		}
	}

	// для неизвестных ошибок пауза не требуется
	return false, 0, stubOrExc(exc, err)
}

func stubOrExc(exc *ch.Exception, err error) *ch.Exception {
	if exc == nil {
		return &ch.Exception{
			Code:    7777,
			Message: fmt.Errorf("nil stub exception: %w", err).Error(),
		}
	}
	return exc
}
