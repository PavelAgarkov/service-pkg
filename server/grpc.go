package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"time"

	"github.com/PavelAgarkov/service-pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Configs struct {
	Port       string
	Network    string
	Reflection bool
}

type GRPCServer struct {
	configs Configs
	server  *grpc.Server
}

func newGRPCServer(configs Configs) *GRPCServer {
	return &GRPCServer{
		configs: configs,
	}
}

func (s *GRPCServer) Start(ctx context.Context, registerServices func(*grpc.Server), serverOptions ...grpc.ServerOption) func() {
	s.server = grpc.NewServer(serverOptions...)
	registerServices(s.server)
	if s.configs.Reflection {
		reflection.Register(s.server)
	}

	listener, err := net.Listen(s.configs.Network, s.configs.Port)
	if err != nil {
		log.Printf("Failed to listen on port %s: %v", s.configs.Port, err)
	}

	utils.GoRecover(ctx, func(ctx context.Context) {
		log.Printf("gRPC server is started on %s", s.configs.Port)
		if err = s.server.Serve(listener); err != nil {
			panic(fmt.Sprintf("Server gRPC stopped by error: %v", err))
		}
	})

	return s.shutdown
}

func (s *GRPCServer) shutdown() {
	logCtx := context.Background()
	log.Printf("Shutting down gRPC server on %s", s.configs.Port)

	timeoutCtx, cancel := context.WithTimeout(logCtx, 5*time.Second)
	defer cancel()

	done := make(chan struct{})

	utils.GoRecover(timeoutCtx, func(ctx context.Context) {
		s.server.GracefulStop()
		close(done)
	})

	select {
	case <-done:
		log.Printf("gRPC server has gracefully stopped.")
	case <-timeoutCtx.Done():
		log.Printf("Graceful shutdown timed out, forcing stop.")
		s.server.Stop()
	}
}

func CreateGRPCServer(ctx context.Context, registerServices func(*grpc.Server), configs Configs, serverOptions ...grpc.ServerOption) func() {
	grpcServer := newGRPCServer(configs)
	shutdownFunc := grpcServer.Start(ctx, registerServices, serverOptions...)
	return shutdownFunc
}

func PanicHandler(ctx context.Context, p interface{}) error {
	ts := grpc.ServerTransportStreamFromContext(ctx)
	fullMethod := ts.Method()
	stack := string(debug.Stack())

	log.Printf("panic in gRPC handler: %v\nMethod: %s\nStack trace:\n%s", p, fullMethod, stack)

	return status.Errorf(codes.Internal, "internal server error (%s)", fullMethod)
}

// EnforceMaxSendSize это костыль, который позволяет ограничить размер ответа сервера, чтобы сервер не протекал по памяти.
// если его убрать, то когда ответ превышает лимит, то сервер начинает течь по памяти, и в итоге падает. Днище, но нечего поделать.
// max передавать желательно меньше чем сервер может вернуть ответом. Я обычно передают 0.9*out_grpc_body_size
// Управлять только снаружи. Вызвать в цепочке только первым. Тронешь - убьет!
func EnforceMaxSendSize(max int) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		resp, err = handler(ctx, req)
		if err != nil || resp == nil {
			return resp, err
		}
		if m, ok := resp.(proto.Message); ok {
			if proto.Size(m) > max {
				return nil, status.Errorf(codes.ResourceExhausted,
					"response too large: %d > %d; use paging/streaming", proto.Size(m), max)
			}
		}
		return resp, nil
	}
}

func TimeoutUnaryInterceptor(timeout time.Duration) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		c, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		return handler(c, req)
	}
}

type ZeroPool struct{}

func (ZeroPool) Get(n int) *[]byte { b := make([]byte, n); return &b }
func (ZeroPool) Put(*[]byte)       {} // ничего не кэшируем
