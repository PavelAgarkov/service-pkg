package kernel

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
	"github.com/google/uuid"
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

type Kernel struct {
	id        string
	config    KernelConfig
	execution func(ctx context.Context, krl *Kernel) error

	ctx               context.Context
	shutdownRWM       sync.RWMutex
	shutdown          *linkedList
	leaderSupervisors []*LeaderSupervisor

	sig chan os.Signal
}

func NewDefaultKernel(ctx context.Context, cores int, heapOverflow int) *Kernel {
	if heapOverflow == 0 {
		heapOverflow = 100
	}
	kernelID := uuid.New().String()
	debug.SetGCPercent(heapOverflow)
	runtime.GOMAXPROCS(cores)
	log.Printf("kernel id - %s - registred with runtime.GOMAXPROCS(%d) and debug.SetGCPercent(%d)\n", kernelID, cores, heapOverflow)
	return &Kernel{
		id:       kernelID,
		shutdown: &linkedList{},
		ctx:      ctx,
		sig:      make(chan os.Signal, 1),
	}
}

func newKernel(ctx context.Context) *Kernel {
	return &Kernel{
		shutdown: &linkedList{},
		ctx:      ctx,
		sig:      make(chan os.Signal, 1),
	}
}

func (kernel *Kernel) setUp() {
	debug.SetGCPercent(kernel.config.HeapOverflow)
	runtime.GOMAXPROCS(kernel.config.Cores)

	log.Printf("kernel id - %s - registred with runtime.GOMAXPROCS(%d) and debug.SetGCPercent(%d)\n", kernel.ID(), kernel.config.Cores, kernel.config.HeapOverflow)
}

func (kernel *Kernel) StartWatchdogsLeadership() {
	if len(kernel.leaderSupervisors) == 0 {
		log.Println("No supervisors registered for leadership")
		return
	}

	for _, supervisor := range kernel.leaderSupervisors {
		utils.GoRecover(kernel.ctx, func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					log.Printf("Stopping supervisor %s due to context cancellation kernelID %s \n", supervisor.SupervisorName, kernel.ID())
					return
				case <-supervisor.ctx.Done():
					supervisor.mu.Lock()
					supervisor.Working = false
					supervisor.mu.Unlock()
					return
				case res, ok := <-supervisor.Watcher:
					if !ok {
						log.Printf("Supervisor %s receive channel closed kernelID %s \n", supervisor.SupervisorName, kernel.ID())
						return
					}
					if res == watchdog.LostAcquire {
						supervisor.mu.Lock()
						if supervisor.Working {
							supervisor.Stop()
							supervisor.Working = false
							log.Printf("Supervisor %s has stopped due to lost leadership kernelID %s \n", supervisor.SupervisorName, kernel.ID())
						}
						supervisor.mu.Unlock()
					}
					if res == watchdog.TakenAcquire {
						supervisor.mu.Lock()
						if !supervisor.Working {
							supervisor.Start()
							supervisor.Working = true
							log.Printf("Supervisor %s has started successfully kernelID %s \n", supervisor.SupervisorName, kernel.ID())
						}
						supervisor.mu.Unlock()
					}
				}
			}
		})
	}
}

func (kernel *Kernel) RegisterWatchdogsLeadership(supervisor *LeaderSupervisor) {
	if supervisor == nil {
		log.Println("Failed to register nil supervisor")
		return
	}

	supervisor.ctx, supervisor.cancel = context.WithCancel(kernel.ctx)
	kernel.leaderSupervisors = append(kernel.leaderSupervisors, supervisor)
}

func (kernel *Kernel) RegisterShutdown(name string, fn func(), priority int) {
	defer func() {
		log.Printf("Registered shutdown func %s with priority %d kernelID %s \n", name, priority, kernel.ID())
	}()
	kernel.shutdownRWM.Lock()
	defer kernel.shutdownRWM.Unlock()
	newShutdown := &shutdown{
		name:         name,
		priority:     priority,
		shutdownFunc: fn,
	}
	if kernel.shutdown.node == nil || kernel.shutdown.node.priority > priority {
		newShutdown.next = kernel.shutdown.node
		kernel.shutdown.node = newShutdown
		return
	}
	current := kernel.shutdown.node
	for current.next != nil && current.next.priority <= priority {
		current = current.next
	}
	newShutdown.next = current.next
	current.next = newShutdown
}

func (kernel *Kernel) shutdownAllAndDeleteAllCanceled() {
	kernel.shutdownRWM.Lock()
	defer kernel.shutdownRWM.Unlock()
	for kernel.shutdown.node != nil {
		kernel.shutdown.node.shutdownFunc()
		log.Printf("Shutdown func %s executed with priority %d kernelID %s \n", kernel.shutdown.node.name, kernel.shutdown.node.priority, kernel.ID())
		kernel.shutdown.node = kernel.shutdown.node.next
	}
}

func (kernel *Kernel) Stop() {
	for _, supervisor := range kernel.leaderSupervisors {
		supervisor.mu.Lock()
		supervisor.cancel()
		if supervisor.Working {
			supervisor.Stop()
			supervisor.Working = false
		}
		supervisor.mu.Unlock()
		log.Printf("Supervisor %s has been stopped kernelID %s \n", supervisor.SupervisorName, kernel.ID())
	}
	log.Printf("kernel is stopping...\n")
	kernel.shutdownAllAndDeleteAllCanceled()
}

func (kernel *Kernel) Start(cancel context.CancelFunc) {
	signal.Notify(kernel.sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)

	utils.GoRecover(kernel.ctx, func(ctx context.Context) {
		defer signal.Stop(kernel.sig)
		<-kernel.sig
		log.Printf("Signal received. Shutting down kernel kernelID %s ...\n", kernel.ID())
		cancel()
	})
}

func (kernel *Kernel) RegisterRecovers() func() {
	return func() {
		if r := recover(); r != nil {
			log.Printf("Panic happened in kernel: %v kernelID %s \n", r, kernel.ID())
			kernel.sig <- syscall.SIGTERM
		}
	}
}

func (kernel *Kernel) Run() {
	<-kernel.ctx.Done()
}

type KernelConfig struct {
	KernelId     string
	Cores        int
	HeapOverflow int
}

func (kernel *Kernel) ID() string {
	return kernel.id
}

type Option func(krl *Kernel)

func WithConfigs(config KernelConfig) Option {
	return func(kernel *Kernel) {
		kernel.config = config

		if kernel.config.KernelId != "" {
			kernel.id = kernel.config.KernelId
		}
	}
}

func WithExecution(execution func(ctx context.Context, kernel *Kernel) error) Option {
	return func(kernel *Kernel) {
		kernel.execution = execution
	}
}

func RunKernel(options ...Option) {
	ctx, cancel := context.WithCancel(context.Background())

	kernel := newKernel(ctx)
	for _, option := range options {
		option(kernel)
	}

	if kernel.execution == nil {
		kernel.execution = func(ctx context.Context, kernel *Kernel) error {
			log.Printf("No execution function provided. Exiting. kernelID %s \n", kernel.ID())
			return nil
		}
	}
	if kernel.config.Cores == 0 {
		kernel.config.Cores = 1
	}
	if kernel.config.HeapOverflow == 0 {
		kernel.config.HeapOverflow = 100
	}
	if kernel.config.KernelId == "" {
		kernel.id = uuid.New().String()
	}

	kernel.setUp()
	kernel.Start(cancel)

	if err := kernel.execution(ctx, kernel); err != nil {
		panic(fmt.Sprintf("failed to execute kernel logic: %v kernelID %s \n", err, kernel.ID()))
	}
	defer kernel.Stop()
	defer kernel.RegisterRecovers()()

	kernel.Run()
}
