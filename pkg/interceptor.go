package switchback

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	statusEndpoint = "/switchback.v1.Switchback/Status"
)

func (s *Server) UnaryInterceptors() grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(s.UnaryAvailable())
}

func (s *Server) StreamInterceptors() grpc.ServerOption {
	return grpc.ChainStreamInterceptor(s.StreamAvailable())
}

// UnaryAvailable returns an interceptor that should be first in the chain - returning
// an error if the server is in maintenance mode (unless a status request is sent).
func (s *Server) UnaryAvailable() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, in interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (out interface{}, err error) {
		// Since this is the first interceptor, track the latency of the method
		start := time.Now()

		if s.conf.Maintenance && info.FullMethod != statusEndpoint {
			// The only RPC we allow in maintenance mode is Status
			// Otherwise stop processing and return unavailable
			return nil, status.Error(codes.Unavailable, "the switchback server is currently in maintenance mode")
		}

		// Call the handler to finalize the request and get the response
		out, err = handler(ctx, in)

		// Log with zerolog - checkout grpclog.LoggerV2 for default logging
		log.Debug().Err(err).Str("method", info.FullMethod).Dur("latency", time.Since(start)).Msg("grpc unary request")
		return out, err
	}
}

// StreamAvailable returns an interceptor that should be first in the chain - returning
// an error if the server is in maintenance mode (unless a status request is sent).
func (s *Server) StreamAvailable() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Since this is the first interceptor, track the uptime of the stream
		start := time.Now()

		if s.conf.Maintenance {
			// Do not allow a stream to be opened in maintenance mode
			return status.Error(codes.Unavailable, "the switchback server is currently in maintenance mode")
		}

		// Call the handler to execute the stream RPC
		err := handler(srv, ss)

		// Log with zerolog - checkout grcplog.LoggerV2 for default logging
		log.Debug().Err(err).Str("method", info.FullMethod).Dur("duration", time.Since(start)).Msg("grpc stream request")
		return err
	}
}
