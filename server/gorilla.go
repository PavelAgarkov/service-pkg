package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/PavelAgarkov/service-pkg/utils"

	"github.com/gorilla/mux"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

type HTTPServer struct {
	port   string
	Router *mux.Router
	logger *zap.Logger
}

func (simple *HTTPServer) RunHTTPServer(balancer http.Handler, mwf ...mux.MiddlewareFunc) func() {
	simple.Router.Use(mwf...)

	var server *http.Server
	if balancer != nil {
		server = &http.Server{
			Addr:    simple.port,
			Handler: balancer,
		}
	} else {
		server = &http.Server{
			Addr:    simple.port,
			Handler: simple.Router,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	utils.GoRecover(ctx, func(ctx context.Context) {
		defer cancel()
		logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
			Msg:       fmt.Sprintf("Server is running on %s", simple.port),
			Component: "HTTPServer",
			Method:    "RunHTTPServer",
			Args:      simple.port,
		})
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(fmt.Sprintf("Server stopped by error: %s", err))
		}
		logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
			Msg:       "Server has stopped",
			Component: "HTTPServer",
			Method:    "RunHTTPServer",
			Args:      simple.port,
		})
	})

	return simple.shutdown(server)
}

// Shutdown gracefully shuts down the server without interrupting any active connections.
func (simple *HTTPServer) shutdown(server *http.Server) func() {
	return func() {
		logger.WriteInfoLog(context.Background(), &logger_wrapper.LogEntry{
			Msg:       "Shutting down the server...",
			Component: "HTTPServer",
			Method:    "shutdown",
			Args:      simple.port,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// использовать server.Shutdown(ctx) вместо server.Close() для корректного завершения запросов
		// и предотвращения утечек ресурсов
		// последние запросы за 5 секунд будут обработаны
		// после этого сервер будет остановлен
		// если необходимо остановить сервер сразу, то использовать server.Close()
		if err := server.Shutdown(ctx); err != nil {
			logger.WriteErrorLog(context.Background(), &logger_wrapper.LogEntry{
				Msg:       fmt.Sprintf("Server shutdown failed: %s", err),
				Error:     err,
				Component: "HTTPServer",
				Method:    "shutdown",
				Args:      simple.port,
			})
		}
		logger.WriteInfoLog(context.Background(), &logger_wrapper.LogEntry{
			Msg:       fmt.Sprintf("Server has done: %s", simple.port),
			Component: "HTTPServer",
			Method:    "shutdown",
			Args:      simple.port,
		})
	}
}

type Handlers struct {
}

func CreateHttpServer(router func(simple *HTTPServer), port string, mwf ...mux.MiddlewareFunc) func() {
	simpleHttpServerShutdownFunction := newSimpleHTTPServer(port).
		ToConfigureHandlers(router).
		RunHTTPServer(nil, mwf...)

	return simpleHttpServerShutdownFunction
}

func newSimpleHTTPServer(port string) *HTTPServer {
	return &HTTPServer{
		Router: mux.NewRouter(),
		port:   port,
	}
}

func (simple *HTTPServer) ToConfigureHandlers(configure func(simple *HTTPServer)) *HTTPServer {
	configure(simple)
	return simple
}

func LoggerContextMiddleware() mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Добавляем логгер в контекст запроса
			ctx := r.Context()
			r = r.WithContext(ctx)

			// Создаём новый ResponseWriter для логирования ответа (опционально)
			lrw := newLoggingResponseWriter(w)

			// Передаём обработку следующему обработчику в цепочке
			next.ServeHTTP(lrw, r)
		})
	}
}

func RecoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rr := r.WithContext(r.Context())
		defer func() {
			if r := recover(); r != nil {
				logger.WriteErrorLog(rr.Context(), &logger_wrapper.LogEntry{
					Msg:       "Паника произошла в http запросе приложении",
					Error:     fmt.Errorf("%v", r),
					Component: "HTTPServer",
					Method:    "RecoverMiddleware",
				})
				w.WriteHeader(http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type contextKey string

const (
	correlationIDCtxKey contextKey = "correlation_id"
)

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlationID := xid.New().String()

		ctx := context.WithValue(
			r.Context(),
			correlationIDCtxKey,
			correlationID,
		)

		r = r.WithContext(ctx)
		w.Header().Add("X-Correlation-ID", correlationID)

		lrw := newLoggingResponseWriter(w)

		defer func(start time.Time) {
			logger.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
				Msg:       fmt.Sprintf("%s request to %s completed", r.Method, r.RequestURI),
				Component: "HTTPServer",
				Method:    "LoggingMiddleware",
				Args: fmt.Sprintf("method: %s, url: %s, user_agent: %s, status_code: %d, elapsed_ms: %s",
					r.Method, r.RequestURI, r.UserAgent(), lrw.statusCode, time.Since(start)),
			})
		}(time.Now())
		next.ServeHTTP(lrw, r)
	})
}

func (lrw *loggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := lrw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("the ResponseWriter does not implement http.Hijacker")
	}
	return hj.Hijack()
}

func (lrw *loggingResponseWriter) Write(data []byte) (int, error) {
	return lrw.ResponseWriter.Write(data)
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

// Override метода WriteHeader для логирования статуса
func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func newLoggingResponseWriter(w http.ResponseWriter) *loggingResponseWriter {
	return &loggingResponseWriter{w, http.StatusOK}
}
