package scheduler

import (
	"context"
	"fmt"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"

	"github.com/robfig/cron/v3"
)

type Cron struct {
	c *cron.Cron
}

func NewCron() *Cron {
	return &Cron{
		c: cron.New(cron.WithSeconds()),
	}
}

// Add "*/10 * * * * *" - каждые 10 секунд
func (c *Cron) Add(ctx context.Context, calendar string, fn func(ctx context.Context) error) {
	_, err := c.c.AddFunc(calendar, func() {
		if err := fn(ctx); err != nil {
			logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
				Msg:       "cron job failed",
				Component: "cron",
				Method:    "Add",
				Args:      calendar,
				Error:     err,
			})
		}
	})
	if err != nil {
		panic(fmt.Sprintf("failed to add cron job %s: %v", calendar, err))
	}
}

func (c *Cron) Stop() {
	c.c.Stop()
}

func (c *Cron) Start() {
	c.c.Start()
}
