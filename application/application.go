package application

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"syscall"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/PavelAgarkov/service-pkg/utils"
	"github.com/PavelAgarkov/service-pkg/watchdog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	LowestPriority    = 10000
	LowPriority       = 1000
	MediumPriority    = 500
	HighPriority      = 100
	HighestPriority   = 50
	CriticalPriority  = 20
	ImmediatePriority = 1
)

type linkedList struct {
	node *shutdown
}

type shutdown struct {
	priority     int
	name         string
	next         *shutdown
	shutdownFunc func()
}

type LeaderSupervisor struct {
	ctx            context.Context
	cancel         context.CancelFunc
	Watcher        <-chan int
	Start          func()
	Stop           func()
	Watchdog       watchdog.LeaderElectingWatchdog
	SupervisorName string
	mu             sync.Mutex
	Working        bool
}

type App struct {
	ctx               context.Context
	shutdownRWM       sync.RWMutex
	shutdown          *linkedList
	leaderSupervisors []*LeaderSupervisor
	sig               chan os.Signal
}

func NewApp(ctx context.Context, cores int, heapOverflow int) *App {
	if heapOverflow == 0 {
		heapOverflow = 100
	}
	debug.SetGCPercent(heapOverflow)
	runtime.GOMAXPROCS(cores)
	logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
		Msg:       fmt.Sprintf("Application registred with runtime.GOMAXPROCS(%d) and debug.SetGCPercent(%d)", cores, heapOverflow),
		Component: "application",
		Method:    "NewApp",
		Args:      fmt.Sprintf("cores: %d, heapOverflow: %d", cores, heapOverflow),
	})
	return &App{
		shutdown: &linkedList{},
		ctx:      ctx,
		sig:      make(chan os.Signal, 1),
	}
}

func (app *App) StartWatchdogsLeadership() {
	if len(app.leaderSupervisors) == 0 {
		logger.WriteInfoLog(context.Background(), &logger_wrapper.LogEntry{
			Msg:       "No supervisors registered for leadership",
			Component: "application",
			Method:    "StartWatchdogsLeadership",
		})
		return
	}

	for _, supervisor := range app.leaderSupervisors {
		utils.GoRecover(app.ctx, func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					logger.WriteInfoLog(app.ctx, &logger_wrapper.LogEntry{
						Msg:       fmt.Sprintf("Stopping supervisor %s due to context cancellation", supervisor.SupervisorName),
						Component: "application",
						Method:    "StartWatchdogsLeadership",
					})
					return
				case <-supervisor.ctx.Done():
					supervisor.mu.Lock()
					supervisor.Working = false
					supervisor.mu.Unlock()
					return
				case res, ok := <-supervisor.Watcher:
					if !ok {
						logger.WriteInfoLog(app.ctx, &logger_wrapper.LogEntry{
							Msg:       fmt.Sprintf("Supervisor %s receive channel closed", supervisor.SupervisorName),
							Component: "application",
							Method:    "StartWatchdogsLeadership",
						})
						return
					}
					if res == watchdog.LostAcquire {
						supervisor.mu.Lock()
						if supervisor.Working {
							supervisor.Stop()
							supervisor.Working = false
							logger.WriteInfoLog(app.ctx, &logger_wrapper.LogEntry{
								Msg:       fmt.Sprintf("Supervisor %s has stopped due to lost leadership", supervisor.SupervisorName),
								Component: "application",
								Method:    "StartWatchdogsLeadership",
							})
						}
						supervisor.mu.Unlock()
					}
					if res == watchdog.TakenAcquire {
						supervisor.mu.Lock()
						if !supervisor.Working {
							supervisor.Start()
							supervisor.Working = true
							logger.WriteInfoLog(app.ctx, &logger_wrapper.LogEntry{
								Msg:       fmt.Sprintf("Supervisor %s has started successfully", supervisor.SupervisorName),
								Component: "application",
								Method:    "StartWatchdogsLeadership",
							})
						}
						supervisor.mu.Unlock()
					}
				}
			}
		})
	}
}

func (app *App) RegisterWatchdogsLeadership(supervisor *LeaderSupervisor) {
	if supervisor == nil {
		logger.WriteErrorLog(app.ctx, &logger_wrapper.LogEntry{
			Msg:       "Failed to register nil supervisor",
			Component: "application",
			Method:    "RegisterWatchdogsLeadership",
			Error:     fmt.Errorf("supervisor cannot be nil"),
		})
		return
	}

	supervisor.ctx, supervisor.cancel = context.WithCancel(app.ctx)
	app.leaderSupervisors = append(app.leaderSupervisors, supervisor)
}

func (app *App) RegisterShutdown(name string, fn func(), priority int) {
	defer func() {
		logger.WriteInfoLog(app.ctx, &logger_wrapper.LogEntry{
			Msg:       fmt.Sprintf("Registered shutdown func %s with priority %d", name, priority),
			Component: "application",
			Method:    "RegisterShutdown",
		})
	}()
	app.shutdownRWM.Lock()
	defer app.shutdownRWM.Unlock()
	newShutdown := &shutdown{
		name:         name,
		priority:     priority,
		shutdownFunc: fn,
	}
	if app.shutdown.node == nil || app.shutdown.node.priority > priority {
		newShutdown.next = app.shutdown.node
		app.shutdown.node = newShutdown
		return
	}
	current := app.shutdown.node
	for current.next != nil && current.next.priority <= priority {
		current = current.next
	}
	newShutdown.next = current.next
	current.next = newShutdown
}

func (app *App) shutdownAllAndDeleteAllCanceled() {
	app.shutdownRWM.Lock()
	defer app.shutdownRWM.Unlock()
	for app.shutdown.node != nil {
		app.shutdown.node.shutdownFunc()
		logger.WriteInfoLog(app.ctx, &logger_wrapper.LogEntry{
			Msg:       fmt.Sprintf("Shutdown func %s executed with priority %d", app.shutdown.node.name, app.shutdown.node.priority),
			Component: "application",
			Method:    "shutdownAllAndDeleteAllCanceled",
		})
		app.shutdown.node = app.shutdown.node.next
	}
}

func (app *App) Stop() {
	for _, supervisor := range app.leaderSupervisors {
		supervisor.mu.Lock()
		supervisor.cancel()
		if supervisor.Working {
			supervisor.Stop()
			supervisor.Working = false
		}
		supervisor.mu.Unlock()
		logger.WriteInfoLog(app.ctx, &logger_wrapper.LogEntry{
			Msg:       fmt.Sprintf("Supervisor %s has been stopped", supervisor.SupervisorName),
			Component: "application",
			Method:    "Stop",
		})
	}
	logger.WriteInfoLog(app.ctx, &logger_wrapper.LogEntry{
		Msg:       "Stopping application",
		Component: "application",
		Method:    "Stop",
	})
	app.shutdownAllAndDeleteAllCanceled()
}

func (app *App) Start(cancel context.CancelFunc) {
	signal.Notify(app.sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)

	utils.GoRecover(app.ctx, func(ctx context.Context) {
		defer signal.Stop(app.sig)
		<-app.sig
		logger.WriteInfoLog(app.ctx, &logger_wrapper.LogEntry{
			Msg:       "Signal received. Shutting down application...",
			Component: "application",
			Method:    "Start",
		})
		cancel()
	})
}

func (app *App) RegisterRecovers() func() {
	return func() {
		if r := recover(); r != nil {
			logger.WriteErrorLog(app.ctx, &logger_wrapper.LogEntry{
				Msg:       "Panic happened in application",
				Component: "application",
				Method:    "RegisterRecovers",
				Error:     fmt.Errorf("%v", r),
			})
			app.sig <- syscall.SIGTERM
		}
	}
}

func (app *App) FlushLogger() {
	logger.FlushLogs()
}

func (app *App) Run() {
	<-app.ctx.Done()
}

type AppConfig struct {
	Cores        int
	HeapOverflow int
}

func defaultInitLogger() error {
	err := logger.InitLoggerForStdout(
		zapcore.InfoLevel, false, nil,
		zap.AddCallerSkip(2),
		zap.AddStacktrace(zapcore.DPanicLevel),
		zap.AddStacktrace(zapcore.PanicLevel),
		zap.AddStacktrace(zapcore.FatalLevel),
	)
	if err != nil {
		return fmt.Errorf("failed to init logger: %w", err)
	}
	return nil
}

func RunExecution(config AppConfig, execution func(ctx context.Context, app *App) error, initLogger func() error) {
	ctx, cancel := context.WithCancel(context.Background())
	app := NewApp(ctx, config.Cores, config.HeapOverflow)
	app.Start(cancel)

	isNil := initLogger == nil
	if isNil {
		defaultErr := defaultInitLogger()
		if defaultErr != nil {
			panic(fmt.Sprintf("failed to init default logger: %v", defaultErr))
		}
	} else {
		if err := initLogger(); err != nil {
			panic(fmt.Sprintf("failed to init custom logger: %v", err))
		}
	}

	if err := execution(ctx, app); err != nil {
		panic(fmt.Sprintf("failed to execute application logic: %v", err))
	}
	defer app.FlushLogger()
	defer app.Stop()
	defer app.RegisterRecovers()()

	app.Run()
}
