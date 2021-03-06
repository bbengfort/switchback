package switchback

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/bbengfort/switchback/pkg/api/v1"
	"github.com/bbengfort/switchback/pkg/config"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	// Initialize zerolog with GCP logging requirements
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.TimestampFieldName = "time"
	zerolog.MessageFieldName = "message"
	log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
}

type Server struct {
	api.UnimplementedSwitchbackServer
	conf    config.Config
	srv     *grpc.Server
	pubsub  *PubSub
	echan   chan error
	started time.Time
}

func New(conf config.Config) (s *Server, err error) {
	if conf.IsZero() {
		if conf, err = config.New(); err != nil {
			return nil, err
		}
	}

	// Set the global level
	zerolog.SetGlobalLevel(conf.GetLogLevel())

	// Set human readable logging if specified
	if conf.ConsoleLog {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	// Create the server and prepare to serve
	s = &Server{conf: conf, echan: make(chan error, 1)}
	s.pubsub = &PubSub{
		topics: make(map[string]map[string]*Group),
	}

	s.srv = grpc.NewServer(s.StreamInterceptors(), s.UnaryInterceptors())
	api.RegisterSwitchbackServer(s.srv, s)
	return s, nil
}

func (s *Server) Serve() (err error) {
	// Catch OS signals for graceful shutdowns
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	go func() {
		<-quit
		s.echan <- s.Shutdown()
	}()

	// Run management routines only if we're not in maintenance mode
	if s.conf.Maintenance {
		log.Warn().Msg("starting server in maintenance mode")
	}

	// Listen for TCP requests on the specified address and port
	var sock net.Listener
	if sock, err = net.Listen("tcp", s.conf.BindAddr); err != nil {
		return fmt.Errorf("could not listen on %q", s.conf.BindAddr)
	}

	// Run the server
	go s.Run(sock)
	s.started = time.Now()
	log.Info().Str("listen", s.conf.BindAddr).Str("version", Version()).Msg("switchback server started")

	// Listen for any errors that might have occurred and wait for all go routines to finish
	if err = <-s.echan; err != nil {
		return err
	}
	return nil
}

func (s *Server) Run(sock net.Listener) {
	defer sock.Close()
	if err := s.srv.Serve(sock); err != nil {
		s.echan <- err
	}
}

func (s *Server) Shutdown() (err error) {
	log.Info().Msg("gracefully shutting down")
	s.srv.GracefulStop()
	return nil
}

func (s *Server) Publish(stream api.Switchback_PublishServer) (err error) {
	log.Info().Str("id", uuid.New().String()).Msg("publisher connected")
	for {
		var event *api.Event
		if event, err = stream.Recv(); err != nil {
			if err != io.EOF {
				log.Error().Err(err).Msg("could not recv event from stream")
				return err
			}
			return nil
		}

		if err = s.pubsub.Publish(event); err != nil {
			log.Error().Err(err).Msg("could not publish event")
		}
	}
}

func (s *Server) Subscribe(in *api.Subscription, stream api.Switchback_SubscribeServer) (err error) {
	var events <-chan *api.Event
	if events, err = s.pubsub.Connect(in); err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	for event := range events {
		if err = stream.Send(event); err != nil {
			// TODO: close the stream on error
			if err != io.EOF {
				log.Error().Err(err).Msg("could not send event to stream")
				return err
			}
			return nil
		}
	}
	return nil
}

func (s *Server) Status(ctx context.Context, in *api.HealthCheck) (out *api.ServiceState, err error) {
	out = &api.ServiceState{
		Status:  "ok",
		Uptime:  time.Since(s.started).String(),
		Version: Version(),
	}

	if s.conf.Maintenance {
		out.Status = "maintenance"
	}
	return out, nil
}
