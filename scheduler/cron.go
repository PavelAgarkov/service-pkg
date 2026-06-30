package scheduler

import (
	"context"
	"fmt"
	"log"

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
			log.Printf("cron job failed: %v", err)
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
