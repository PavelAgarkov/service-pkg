package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/PavelAgarkov/service-pkg/utils"
)

type StopMode int

const (
	StopImmediate StopMode = iota
	StopGraceful
)

type JobConfiguration struct {
	Name     string
	Func     func(context.Context) error
	Tick     time.Duration
	Deadline time.Duration
	StopMode StopMode
}

type job struct {
	name     string
	rmu      sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	fn       func(context.Context) error
	tick     time.Duration
	ticker   *time.Ticker
	deadline time.Duration
	wg       sync.WaitGroup
	stopMode StopMode
}

type JobScheduler struct {
	mu         sync.Mutex
	started    bool
	goroutines map[string]*job
	rate       chan struct{}
}

func NewJobScheduler(rate int64) *JobScheduler {
	if rate <= 0 {
		rate = 1
	}
	return &JobScheduler{
		rate:       make(chan struct{}, rate),
		goroutines: make(map[string]*job),
	}
}

func (s *JobScheduler) Add(cfg JobConfiguration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("scheduler.Add(%s): already started", cfg.Name)
	}
	if _, exists := s.goroutines[cfg.Name]; exists {
		return fmt.Errorf("scheduler.Add(%s): job already exists", cfg.Name)
	}

	s.goroutines[cfg.Name] = &job{
		name:     cfg.Name,
		fn:       cfg.Func,
		tick:     cfg.Tick,
		deadline: cfg.Deadline,
		stopMode: cfg.StopMode,
	}
	return nil
}

func (s *JobScheduler) Start(ctx context.Context) func() {
	return func() {
		s.mu.Lock()
		if s.started {
			s.mu.Unlock()
			log.Printf("scheduler.Start: already started")
			return
		}
		s.started = true

		// Сохраним локальную копию job-ов и освободим глобальный лок
		jobs := make(map[string]*job, len(s.goroutines))
		for name, j := range s.goroutines {
			jobs[name] = j
		}
		s.mu.Unlock()

		// Дальше – без глобального лока
		for name, j := range jobs {
			j.rmu.Lock()
			j.ctx, j.cancel = context.WithCancel(ctx)
			j.ticker = time.NewTicker(j.tick)
			j.wg.Add(1)
			j.rmu.Unlock()

			utils.GoRecover(ctx, func(ctx context.Context) {
				s.run(name, j)
			})
		}
	}
}

// Stop останавливает задачи и дожидается их завершения.
func (s *JobScheduler) Stop() func() {
	return func() {
		s.mu.Lock()
		if !s.started {
			s.mu.Unlock()
			return
		}
		s.started = false

		// отзываем контексты + останавливаем тикеры под защитой локов каждого job
		for _, j := range s.goroutines {
			j.cancel()
			j.rmu.Lock()
			if j.ticker != nil {
				j.ticker.Stop()
			}
			j.rmu.Unlock()
		}

		// копия слайса, чтобы ждать уже без глобального лока
		jobs := make([]*job, 0, len(s.goroutines))
		for _, j := range s.goroutines {
			jobs = append(jobs, j)
		}
		s.mu.Unlock()

		for _, j := range jobs {
			j.wg.Wait()
		}
	}
}

func (s *JobScheduler) run(name string, j *job) {
	defer j.wg.Done()

	for {
		j.rmu.RLock()
		ctx := j.ctx
		ticker := j.ticker
		j.rmu.RUnlock()

		select {
		case <-ctx.Done():
			log.Printf("Job %s stopped: %v", name, ctx.Err())
			return

		case <-ticker.C:
			if err := s.exec(j); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("Job %s execution failed: %v", name, err)
			}
		}
	}
}

func (s *JobScheduler) exec(j *job) (err error) {
	select {
	case <-j.ctx.Done():
		return j.ctx.Err()
	case s.rate <- struct{}{}:
	}
	defer func() { <-s.rate }()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Job %s panicked: %v", j.name, r)
			err = fmt.Errorf("panic in job: %v", r)
		}
	}()

	if j.ctx.Err() != nil {
		return j.ctx.Err()
	}

	switch j.stopMode {
	case StopImmediate:
		ctx, cancel := context.WithTimeout(j.ctx, j.deadline)
		defer cancel()
		err = j.fn(ctx)
	case StopGraceful:
		ctx, cancel := context.WithTimeout(context.Background(), j.deadline)
		defer cancel()
		err = j.fn(ctx)
	}

	return err
}
