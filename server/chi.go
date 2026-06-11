package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	logger_wrapper "github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/PavelAgarkov/service-pkg/utils"
	"github.com/go-chi/chi/v5"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

type Option func(*http.Server)

func WithReadTimeout(d time.Duration) Option {
	return func(s *http.Server) {
		s.ReadTimeout = d
	}
}

func WithWriteTimeout(d time.Duration) Option {
	return func(s *http.Server) {
		s.WriteTimeout = d
	}
}

func WithIdleTimeout(d time.Duration) Option {
	return func(s *http.Server) {
		s.IdleTimeout = d
	}
}

func WithReadHeaderTimeout(d time.Duration) Option {
	return func(s *http.Server) {
		s.ReadHeaderTimeout = d
	}
}
func WithMaxHeaderBytes(n int) Option {
	return func(s *http.Server) {
		s.MaxHeaderBytes = n
	}
}
func WithErrorLog(l *log.Logger) Option {
	return func(s *http.Server) {
		s.ErrorLog = l
	}
}

func WithBaseContext(fn func(net.Listener) context.Context) Option {
	return func(s *http.Server) {
		s.BaseContext = fn
	}
}

func WithConnContext(fn func(ctx context.Context, c net.Conn) context.Context) Option {
	return func(s *http.Server) {
		s.ConnContext = fn
	}
}

func WithTLSConfig(cfg *tls.Config) Option {
	return func(s *http.Server) {
		s.TLSConfig = cfg
	}
}

func WithDisableKeepAlives(disable bool) Option {
	return func(s *http.Server) {
		s.SetKeepAlivesEnabled(!disable)
	}
}

type serverState string

const (
	stateInitial  serverState = "initial"
	stateRunning  serverState = "running"
	stateDraining serverState = "draining"
	stateShutdown serverState = "shutdown"
)

type PreShutdownState struct {
	State           atomic.Value
	Need            bool
	TimeForDraining time.Duration
	TimeForShutdown time.Duration
}

func NewPreShutdownState(need bool, timeForDraining, timeForShutdown time.Duration) *PreShutdownState {
	pss := &PreShutdownState{
		Need:            need,
		TimeForDraining: timeForDraining,
		TimeForShutdown: timeForShutdown,
	}
	pss.State.Store(stateInitial)
	return pss
}

type HTTPServerChi struct {
	port   string
	Router *chi.Mux
	logger *zap.Logger

	opts             []Option // сюда складываем опции для http.Server
	preShutdownState *PreShutdownState
}

// CreateHTTPChiServer создаёт и запускает HTTP-сервер на chi.
// Сигнатуры не меняем: маршруты, порт, middleware-список.
// Конфигурацию http.Server можно задать внутри `routes` через s.ApplyServerOptions(...).
func CreateHTTPChiServer(
	routes func(*HTTPServerChi),
	port string,
	preShutdownState *PreShutdownState,
	mwf ...func(http.Handler) http.Handler,
) func() {
	s := newHTTPServer(port)

	if len(mwf) > 0 {
		s.Router.Use(mwf...)
	}

	s.apply(routes)
	s.preShutdownState = preShutdownState

	return s.run(nil)
}

func newHTTPServer(port string) *HTTPServerChi {
	return &HTTPServerChi{
		port:   port,
		Router: chi.NewRouter(),
	}
}

func (s *HTTPServerChi) apply(cfg func(*HTTPServerChi)) {
	cfg(s)
}

// ApplyServerOptions — точка конфигурации http.Server из кода маршрутов.
func (s *HTTPServerChi) ApplyServerOptions(opts ...Option) {
	s.opts = append(s.opts, opts...)
}

func (s *HTTPServerChi) run(balancer http.Handler) func() {
	srv := &http.Server{
		Addr:    s.port,
		Handler: ifNil(balancer, s.Router),
	}

	for _, opt := range s.opts {
		opt(srv)
	}

	ctx, cancel := context.WithCancel(context.Background())
	utils.GoRecover(ctx, func(ctx context.Context) {
		defer cancel()
		logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
			Msg:       fmt.Sprintf("HTTP server listening on %s", s.port),
			Component: "HTTPServer",
			Method:    "run",
			Args:      s.port,
		})
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(fmt.Sprintf("server stopped: %s", err))
		}
	})

	if s.preShutdownState != nil && s.preShutdownState.Need {
		s.preShutdownState.State.Store(stateRunning)
	}

	return s.shutdown(srv)
}

func (s *HTTPServerChi) shutdown(srv *http.Server) func() {
	return func() {
		logger.WriteInfoLog(context.Background(), &logger_wrapper.LogEntry{
			Msg:       "Shutting down HTTP server...",
			Component: "HTTPServer",
			Method:    "shutdown",
			Args:      s.port,
		})

		timeout := 5 * time.Second
		if s.preShutdownState != nil {
			timeout = s.preShutdownState.TimeForShutdown
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if s.preShutdownState != nil && s.preShutdownState.Need {
			s.preShutdownState.State.Store(stateDraining)
			logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
				Msg:       fmt.Sprintf("Draining connections for %s...", s.preShutdownState.TimeForDraining),
				Component: "HTTPServer",
				Method:    "shutdown",
			})
			select {
			case <-time.After(s.preShutdownState.TimeForDraining):
			}
		}

		logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
			Msg:       "Shutdown HTTP server",
			Component: "HTTPServer",
			Method:    "shutdown",
		})
		if err := srv.Shutdown(ctx); err != nil {
			logger.WriteErrorLog(context.Background(), &logger_wrapper.LogEntry{
				Msg:       "HTTP shutdown failed",
				Error:     err,
				Component: "HTTPServer",
				Method:    "shutdown",
				Args:      s.port,
			})
		}

		if s.preShutdownState != nil && s.preShutdownState.Need {
			s.preShutdownState.State.Store(stateShutdown)
		}
	}
}

func ifNil(balancer, router http.Handler) http.Handler {
	if balancer != nil {
		return balancer
	}
	return router
}

type contextChiKey string

const correlationChiIDCtxKey contextChiKey = "correlation_id"

func LoggerChiContextMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RecoverChiMiddleware ловит panic внутри хэндлеров.
func RecoverChiMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func(c context.Context) {
			if rec := recover(); rec != nil {
				logger.WriteErrorLog(c, &logger_wrapper.LogEntry{
					Msg:       "panic caught in HTTP request",
					Error:     fmt.Errorf("%v", rec),
					Component: "HTTPServer",
					Method:    "RecoverMiddleware",
				})
				w.WriteHeader(http.StatusInternalServerError)
			}
		}(r.Context())
		next.ServeHTTP(w, r)
	})
}

func RequestTimeout(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func DrainMiddleware(
	state *PreShutdownState,
	responseCallback func(w http.ResponseWriter),
) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if state != nil && state.Need {
				if val, ok := state.State.Load().(serverState); ok && val == stateDraining {
					if responseCallback != nil {
						responseCallback(w)
					} else {
						w.Header().Set("Retry-After", fmt.Sprintf("%.0f", state.TimeForShutdown.Seconds()))
						w.WriteHeader(http.StatusServiceUnavailable)
					}
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// LoggingChiMiddleware логирует запрос, добавляет X-Correlation-ID.
func LoggingChiMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		corrID := xid.New().String()
		ctx := context.WithValue(r.Context(), correlationChiIDCtxKey, corrID)

		w.Header().Set("X-Correlation-ID", corrID)

		lrw := newLoggingChiResponseWriter(w)

		start := time.Now()
		next.ServeHTTP(lrw, r.WithContext(ctx))

		logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
			Msg:       fmt.Sprintf("%s %s completed", r.Method, r.URL.Path),
			Component: "HTTPServer",
			Method:    "LoggingMiddleware",
			Args: fmt.Sprintf("status=%d duration=%s ua=%s",
				lrw.statusCode, time.Since(start), r.UserAgent()),
		})
	})
}

type loggingChiResponseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func newLoggingChiResponseWriter(w http.ResponseWriter) *loggingChiResponseWriter {
	return &loggingChiResponseWriter{w, http.StatusOK, false}
}

func (lrw *loggingChiResponseWriter) WriteHeader(code int) {
	if lrw.wroteHeader {
		// второй вызов — только обновим код для логов, в реальный RW не лезем
		return
	}
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
	lrw.wroteHeader = true
}

func (lrw *loggingChiResponseWriter) Write(b []byte) (int, error) {
	// как в стандартном ResponseWriter: если заголовки не ушли — шлём 200
	if !lrw.wroteHeader {
		lrw.WriteHeader(http.StatusOK)
	}
	return lrw.ResponseWriter.Write(b)
}

func (lrw *loggingChiResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := lrw.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

// Flush делегирует http.Flusher для стриминга (SSE, chunked и т.п.).
func (lrw *loggingChiResponseWriter) Flush() {
	if f, ok := lrw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
	// если нижний не поддерживает Flusher — просто no-op
}

// Push делегирует http.Pusher (HTTP/2 server push).
func (lrw *loggingChiResponseWriter) Push(target string, opts *http.PushOptions) error {
	if p, ok := lrw.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

// ReadFrom делегирует io.ReaderFrom для ускорения io.Copy(w, r).
func (lrw *loggingChiResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := lrw.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	// fallback без ReaderFrom
	return io.Copy(lrw.ResponseWriter, r)
}
