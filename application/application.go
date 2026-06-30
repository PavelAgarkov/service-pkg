package application

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"syscall"

	"github.com/PavelAgarkov/service-pkg/utils"
	"github.com/PavelAgarkov/service-pkg/watchdog"
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
	log.Printf("Application registred with runtime.GOMAXPROCS(%d) and debug.SetGCPercent(%d)\n", cores, heapOverflow)
	return &App{
		shutdown: &linkedList{},
		ctx:      ctx,
		sig:      make(chan os.Signal, 1),
	}
}

func (app *App) StartWatchdogsLeadership() {
	if len(app.leaderSupervisors) == 0 {
		log.Println("No supervisors registered for leadership")
		return
	}

	for _, supervisor := range app.leaderSupervisors {
		utils.GoRecover(app.ctx, func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					log.Printf("Stopping supervisor %s due to context cancellation\n", supervisor.SupervisorName)
					return
				case <-supervisor.ctx.Done():
					supervisor.mu.Lock()
					supervisor.Working = false
					supervisor.mu.Unlock()
					return
				case res, ok := <-supervisor.Watcher:
					if !ok {
						log.Printf("Supervisor %s receive channel closed\n", supervisor.SupervisorName)
						return
					}
					if res == watchdog.LostAcquire {
						supervisor.mu.Lock()
						if supervisor.Working {
							supervisor.Stop()
							supervisor.Working = false
							log.Printf("Supervisor %s has stopped due to lost leadership\n", supervisor.SupervisorName)
						}
						supervisor.mu.Unlock()
					}
					if res == watchdog.TakenAcquire {
						supervisor.mu.Lock()
						if !supervisor.Working {
							supervisor.Start()
							supervisor.Working = true
							log.Printf("Supervisor %s has started successfully\n", supervisor.SupervisorName)
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
		log.Println("Failed to register nil supervisor")
		return
	}

	supervisor.ctx, supervisor.cancel = context.WithCancel(app.ctx)
	app.leaderSupervisors = append(app.leaderSupervisors, supervisor)
}

func (app *App) RegisterShutdown(name string, fn func(), priority int) {
	defer func() {
		log.Printf("Registered shutdown func %s with priority %d\n", name, priority)
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
		log.Printf("Shutdown func %s executed with priority %d\n", app.shutdown.node.name, app.shutdown.node.priority)
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
		log.Printf("Supervisor %s has been stopped\n", supervisor.SupervisorName)
	}
	log.Printf("Application is stopping...\n")
	app.shutdownAllAndDeleteAllCanceled()
}

func (app *App) Start(cancel context.CancelFunc) {
	signal.Notify(app.sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)

	utils.GoRecover(app.ctx, func(ctx context.Context) {
		defer signal.Stop(app.sig)
		<-app.sig
		log.Printf("Signal received. Shutting down application...\n")
		cancel()
	})
}

func (app *App) RegisterRecovers() func() {
	return func() {
		if r := recover(); r != nil {
			log.Printf("Panic happened in application: %v\n", r)
			app.sig <- syscall.SIGTERM
		}
	}
}

func (app *App) Run() {
	<-app.ctx.Done()
}

type AppConfig struct {
	Cores        int
	HeapOverflow int
}

func RunExecution(config AppConfig, execution func(ctx context.Context, app *App) error) {
	ctx, cancel := context.WithCancel(context.Background())
	app := NewApp(ctx, config.Cores, config.HeapOverflow)
	app.Start(cancel)

	if err := execution(ctx, app); err != nil {
		panic(fmt.Sprintf("failed to execute application logic: %v", err))
	}
	defer app.Stop()
	defer app.RegisterRecovers()()

	app.Run()
}
