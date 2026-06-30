package main

import (
	"context"
	"net/http"
	"time"

	"github.com/PavelAgarkov/service-pkg/kernel"
	"github.com/PavelAgarkov/service-pkg/server"
	"github.com/go-chi/chi/v5"
)

func main() {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	RunMain(parent, cancel)
}

func RunMain(parent context.Context, cancel context.CancelFunc) {
	kl := kernel.NewDefaultKernel(parent, 1, 100)
	kl.Start(cancel)

	defer kl.Stop()
	defer kl.RegisterRecovers()()

	storage := server.NewPreShutdownState(
		true,
		10*time.Second,
		5*time.Second,
	)
	shutdown := server.CreateHTTPChiServer(func(s *server.HTTPServerChi) {
		s.Router.Route("/api", func(r chi.Router) {
			// мидлвары на всю группу /api/*
			r.Use(server.DrainMiddleware(storage, func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("server in draining state try again"))
			}))
			// таймаут на все ручки в группе /api/*
			// если обработка запроса занимает больше 2 секунд, то контекст в ручке будет отменен
			r.Use(server.RequestTimeout(2 * time.Second))
			r.Use(server.LoggingChiMiddleware)

			r.Get("/ping", func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(3 * time.Second)
				ctx := r.Context()
				if ctx.Err() != nil {
					w.WriteHeader(http.StatusRequestTimeout)
					_, _ = w.Write([]byte("request timeout"))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("pong"))
			})
		})

		// настраиваем http.Server
		s.ApplyServerOptions(
			server.WithReadTimeout(5*time.Second),
			server.WithWriteTimeout(10*time.Second),
			server.WithIdleTimeout(30*time.Second),
			server.WithReadHeaderTimeout(2*time.Second),
			server.WithMaxHeaderBytes(2<<20),
		)
	},
		":8088",
		storage,
		server.RecoverChiMiddleware,
		server.LoggingChiMiddleware,
	)
	kl.RegisterShutdown("chi_http_server", shutdown, kernel.ImmediatePriority)

	kl.Run()
}
