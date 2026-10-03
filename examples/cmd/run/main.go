package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/PavelAgarkov/service-pkg/kernel"
	"github.com/PavelAgarkov/service-pkg/locker"
	redis2 "github.com/PavelAgarkov/service-pkg/redis"
	"github.com/PavelAgarkov/service-pkg/scheduler"
	"github.com/PavelAgarkov/service-pkg/server"
	"github.com/PavelAgarkov/service-pkg/watchdog"
)

func main() {
	kernel.RunKernel(
		kernel.WithConfigs(
			kernel.KernelConfig{
				//Cores:        4, all cores of vm will be used if not provided
				HeapOverflow: 100,
				//KernelId:     "my-kernel-id", UUID will be generated if not provided
			}),
		kernel.WithExecution(
			func(ctx context.Context, krl *kernel.Kernel) error {
				storage := server.NewPreShutdownState(
					true,
					1*time.Second,
					1*time.Second,
				)
				httpStop := server.CreateHTTPChiServer(
					func(s *server.HTTPServerChi) {
						s.Router.Use(server.RecoverChiMiddleware, server.LoggingChiMiddleware)

						s.Router.Use(server.DrainMiddleware(storage, func(w http.ResponseWriter) {
							func(w http.ResponseWriter, msg string, status int, code int) {
								type RequestError struct {
									Message string `json:"message"`
									Code    int    `json:"code"`
								}
								w.Header().Set("Content-Type", "application/json")
								w.WriteHeader(status)
								_, internalErr := json.Marshal(&RequestError{
									Message: msg,
									Code:    code,
								})

								if internalErr != nil {
									_, _ = json.Marshal(&RequestError{
										Message: msg,
										Code:    code,
									})
									log.Printf("failed to write json error response: %v", internalErr)
								}
							}(w, "server in draining state try again", http.StatusServiceUnavailable, 5555)
						}))
						s.Router.Get("/health", func(w http.ResponseWriter, r *http.Request) {
							w.WriteHeader(200)
						})
					},
					":8083",
					storage,
				)
				krl.RegisterShutdown("http", httpStop, kernel.HighPriority)

				// Планировщик с одной задачей
				sch := scheduler.NewJobScheduler(1)
				_ = sch.Add(scheduler.JobConfiguration{
					Name:     "heartbeat",
					Tick:     5 * time.Second,
					Deadline: 2 * time.Second,
					StopMode: scheduler.StopImmediate,
					Func: func(ctx context.Context) error {
						log.Printf("heartbeat tick")
						return nil
					},
				})
				krl.RegisterShutdown("scheduler", func() {
					scheduler.NewTaskSupervisor([]scheduler.JobSchedulerInterface{sch}).Stop()
				}, kernel.MediumPriority)

				// Лидер‑элекция (при необходимости)
				//rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
				//rdb, err := redis2.NewRedisClient("redis://:qwerty12345@localhost:6379")
				rdb, err := redis2.NewRedisClient("redis://:qwerty12345@localhost:26379?master_name=mymaster&password=qwerty12345")
				if err != nil {
					log.Printf("failed to create redis client: %v", err)
					return err
				}
				lck := locker.NewLocker(rdb)
				wd := watchdog.NewRedisWatchdogLeader(ctx, lck)

				krl.RegisterWatchdogsLeadership(&kernel.LeaderSupervisor{
					Watchdog:       wd,
					Watcher:        wd.Elect(watchdog.Config{ElectionName: watchdog.Cron}),
					SupervisorName: "cron-supervisor",
					Start:          func() { sch.Start(ctx)() },
					Stop:           func() { sch.Stop()() },
				})
				return nil
			},
		),
	)
}
