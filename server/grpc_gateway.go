package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/PavelAgarkov/service-pkg/utils"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	httpSwagger "github.com/swaggo/http-swagger/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type SwaggerConfig struct {
	Enabled bool

	// URL Swagger UI.
	UIPath string

	// URL, по которому клиент получит JSON-спецификацию.
	JSONPath string

	// Файл на диске.
	JSONFile string
}

type GRPCGatewayConfigs struct {
	// Адрес HTTP grpc-gateway, например ":9099".
	HTTPAddr string

	// Адрес настоящего gRPC-сервера, например "127.0.0.1:9009".
	GRPCEndpoint string

	// Обычно "tcp".
	Network string

	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration

	ShutdownTimeout time.Duration
	MaxHeaderBytes  int

	Swagger SwaggerConfig
}

type RegisterGatewayFunc func(
	ctx context.Context,
	mux *runtime.ServeMux,
	endpoint string,
	opts []grpc.DialOption,
) error

type GRPCGatewayServer struct {
	configs GRPCGatewayConfigs
	server  *http.Server
}

func NewGRPCGatewayServer(
	configs GRPCGatewayConfigs,
) (*GRPCGatewayServer, error) {
	if configs.HTTPAddr == "" {
		return nil, errors.New("grpc gateway HTTP address is required")
	}

	if configs.GRPCEndpoint == "" {
		return nil, errors.New("grpc gateway upstream endpoint is required")
	}

	if configs.Network == "" {
		configs.Network = "tcp"
	}

	if configs.ReadHeaderTimeout <= 0 {
		configs.ReadHeaderTimeout = 5 * time.Second
	}

	if configs.IdleTimeout <= 0 {
		configs.IdleTimeout = 60 * time.Second
	}

	if configs.ShutdownTimeout <= 0 {
		configs.ShutdownTimeout = 5 * time.Second
	}

	if configs.MaxHeaderBytes <= 0 {
		configs.MaxHeaderBytes = 1 << 20
	}

	if configs.Swagger.Enabled {
		if configs.Swagger.UIPath == "" {
			configs.Swagger.UIPath = "/swagger/"
		}

		if configs.Swagger.JSONPath == "" {
			configs.Swagger.JSONPath = "/swagger/api.swagger.json"
		}

		if configs.Swagger.JSONFile == "" {
			return nil, errors.New(
				"swagger JSON file is required when Swagger is enabled",
			)
		}
	}

	return &GRPCGatewayServer{
		configs: configs,
	}, nil
}

func buildGatewayHTTPHandler(
	gatewayMux *runtime.ServeMux,
	config SwaggerConfig,
) (http.Handler, error) {
	if !config.Enabled {
		return gatewayMux, nil
	}

	rootMux := http.NewServeMux()

	rootMux.Handle(
		config.JSONPath,
		swaggerJSONHandler(config.JSONFile),
	)

	rootMux.Handle(
		config.UIPath,
		httpSwagger.Handler(
			httpSwagger.URL(config.JSONPath),
		),
	)

	// grpc-gateway должен регистрироваться последним
	// как fallback для остальных HTTP-путей.
	rootMux.Handle("/", gatewayMux)

	return rootMux, nil
}

func swaggerJSONHandler(filename string) http.Handler {
	return http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		if request.Method != http.MethodGet &&
			request.Method != http.MethodHead {
			writer.Header().Set("Allow", "GET, HEAD")
			http.Error(
				writer,
				http.StatusText(http.StatusMethodNotAllowed),
				http.StatusMethodNotAllowed,
			)
			return
		}

		writer.Header().Set(
			"Content-Type",
			"application/json; charset=utf-8",
		)

		http.ServeFile(writer, request, filename)
	})
}

type GatewayRegisterFunc func(
	ctx context.Context,
	mux *runtime.ServeMux,
	endpoint string,
	opts []grpc.DialOption,
) error

func (s *GRPCGatewayServer) Start(
	ctx context.Context,
	registerFunc GatewayRegisterFunc,
	dialOptions []grpc.DialOption,
	muxOptions []runtime.ServeMuxOption,
) (func(), error) {
	if registerFunc == nil {
		return nil, errors.New(
			"grpc gateway registration function is required",
		)
	}

	if len(dialOptions) == 0 {
		dialOptions = []grpc.DialOption{
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		}
	}

	gatewayMux := runtime.NewServeMux(muxOptions...)

	if err := registerFunc(
		ctx,
		gatewayMux,
		s.configs.GRPCEndpoint,
		dialOptions,
	); err != nil {
		return nil, fmt.Errorf(
			"register gateway handlers: %w",
			err,
		)
	}

	handler, err := buildGatewayHTTPHandler(
		gatewayMux,
		s.configs.Swagger,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"build gateway HTTP handler: %w",
			err,
		)
	}

	s.server = &http.Server{
		Addr:              s.configs.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: s.configs.ReadHeaderTimeout,
		ReadTimeout:       s.configs.ReadTimeout,
		WriteTimeout:      s.configs.WriteTimeout,
		IdleTimeout:       s.configs.IdleTimeout,
		MaxHeaderBytes:    s.configs.MaxHeaderBytes,
	}

	listener, err := net.Listen(
		s.configs.Network,
		s.configs.HTTPAddr,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"listen grpc gateway on %s/%s: %w",
			s.configs.Network,
			s.configs.HTTPAddr,
			err,
		)
	}

	utils.GoRecover(ctx, func(ctx context.Context) {
		log.Printf(
			"gRPC gateway started on %s, upstream %s",
			s.configs.HTTPAddr,
			s.configs.GRPCEndpoint,
		)

		if s.configs.Swagger.Enabled {
			log.Printf(
				"gRPC gateway Swagger UI: http://localhost%s%s",
				s.configs.HTTPAddr,
				s.configs.Swagger.UIPath,
			)
		}

		serveErr := s.server.Serve(listener)
		if serveErr != nil &&
			!errors.Is(serveErr, http.ErrServerClosed) {
			log.Printf(
				"gRPC gateway stopped with error: %v",
				serveErr,
			)
		}
	})

	return s.shutdown, nil
}

func (s *GRPCGatewayServer) shutdown() {
	if s.server == nil {
		return
	}

	log.Printf("Shutting down gRPC gateway on %s", s.configs.HTTPAddr)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		s.configs.ShutdownTimeout,
	)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		log.Printf("Graceful gRPC gateway shutdown failed: %v", err)

		if closeErr := s.server.Close(); closeErr != nil {
			log.Printf("Forced gRPC gateway shutdown failed: %v", closeErr)
		}
		return
	}

	log.Printf("gRPC gateway has gracefully stopped")
}

func IncomingHeaderMatcher(header string) (string, bool) {
	switch strings.ToLower(header) {
	case "authorization":
		return "authorization", true

	case "x-request-id":
		return "x-request-id", true

	case "x-correlation-id":
		return "x-correlation-id", true

	case "cookie":
		return "cookie", true

	default:
		return runtime.DefaultHeaderMatcher(header)
	}
}

func GatewayErrorHandler(
	ctx context.Context,
	mux *runtime.ServeMux,
	marshaler runtime.Marshaler,
	writer http.ResponseWriter,
	request *http.Request,
	err error,
) {
	code := status.Code(err)

	httpStatus := runtime.HTTPStatusFromCode(code)
	if code == codes.Unknown {
		httpStatus = http.StatusInternalServerError
	}

	writer.Header().Set("Content-Type", marshaler.ContentType(nil))
	writer.WriteHeader(httpStatus)

	payload := map[string]any{
		"code":    code.String(),
		"message": status.Convert(err).Message(),
	}

	if encodeErr := marshaler.NewEncoder(writer).Encode(payload); encodeErr != nil {
		// Здесь уже поздно менять HTTP status.
	}
}
