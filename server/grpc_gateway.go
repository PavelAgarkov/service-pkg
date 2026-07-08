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
	APIs    []SwaggerAPIConfig
}

type SwaggerAPIConfig struct {
	Name string

	// Например: /swagger/template/
	UIPath string

	// Например: /swagger/template/api.swagger.json
	JSONPath string

	// Например: ./protobuf/template.swagger.json
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

	return &GRPCGatewayServer{
		configs: configs,
	}, nil
}

func (s *GRPCGatewayServer) buildGatewayHTTPHandler(
	gatewayMux *runtime.ServeMux,
	config SwaggerConfig,
) (http.Handler, error) {
	if !config.Enabled {
		return gatewayMux, nil
	}

	if len(config.APIs) == 0 {
		return nil, errors.New(
			"swagger is enabled, but no Swagger APIs are configured",
		)
	}

	if err := validateSwaggerConfig(config); err != nil {
		return nil, err
	}

	rootMux := http.NewServeMux()

	for _, api := range config.APIs {
		rootMux.Handle(
			api.JSONPath,
			s.swaggerJSONHandler(api.JSONFile),
		)

		rootMux.Handle(
			api.UIPath,
			httpSwagger.Handler(
				httpSwagger.URL(api.JSONPath),
			),
		)
	}

	// grpc-gateway используется как fallback.
	rootMux.Handle("/", gatewayMux)

	return rootMux, nil
}

func validateSwaggerConfig(config SwaggerConfig) error {
	registeredPaths := make(map[string]string, len(config.APIs)*2)

	for index, api := range config.APIs {
		if err := validateSwaggerAPIConfig(api); err != nil {
			return fmt.Errorf(
				"validate Swagger API at index %d: %w",
				index,
				err,
			)
		}

		if owner, exists := registeredPaths[api.UIPath]; exists {
			return fmt.Errorf(
				"Swagger UI path %q for API %q is already used by API %q",
				api.UIPath,
				api.Name,
				owner,
			)
		}

		registeredPaths[api.UIPath] = api.Name

		if owner, exists := registeredPaths[api.JSONPath]; exists {
			return fmt.Errorf(
				"Swagger JSON path %q for API %q is already used by API %q",
				api.JSONPath,
				api.Name,
				owner,
			)
		}

		registeredPaths[api.JSONPath] = api.Name
	}

	return nil
}

func validateSwaggerAPIConfig(config SwaggerAPIConfig) error {
	if config.Name == "" {
		return errors.New("swagger API name is required")
	}

	if config.UIPath == "" {
		return fmt.Errorf(
			"swagger UI path is required for API %q",
			config.Name,
		)
	}

	if config.JSONPath == "" {
		return fmt.Errorf(
			"swagger JSON path is required for API %q",
			config.Name,
		)
	}

	if config.JSONFile == "" {
		return fmt.Errorf(
			"swagger JSON file is required for API %q",
			config.Name,
		)
	}

	if !strings.HasPrefix(config.UIPath, "/") {
		return fmt.Errorf(
			"swagger UI path for API %q must start with /",
			config.Name,
		)
	}

	if !strings.HasSuffix(config.UIPath, "/") {
		return fmt.Errorf(
			"swagger UI path for API %q must end with /",
			config.Name,
		)
	}

	if !strings.HasPrefix(config.JSONPath, "/") {
		return fmt.Errorf(
			"swagger JSON path for API %q must start with /",
			config.Name,
		)
	}

	if strings.HasSuffix(config.JSONPath, "/") {
		return fmt.Errorf(
			"swagger JSON path for API %q must not end with /",
			config.Name,
		)
	}

	if config.UIPath == config.JSONPath {
		return fmt.Errorf(
			"swagger UI path and JSON path for API %q must be different",
			config.Name,
		)
	}

	return nil
}

func (s *GRPCGatewayServer) swaggerJSONHandler(
	filename string,
) http.Handler {
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

	handler, err := s.buildGatewayHTTPHandler(
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
			for _, api := range s.configs.Swagger.APIs {
				log.Printf(
					"gRPC gateway Swagger UI %q: http://localhost%s%s",
					api.Name,
					s.configs.HTTPAddr,
					api.UIPath,
				)
			}
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
