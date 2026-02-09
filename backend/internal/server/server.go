package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ApoorvYdv/go-boilerplate/internal/config"
	"github.com/ApoorvYdv/go-boilerplate/internal/database"
	"github.com/ApoorvYdv/go-boilerplate/internal/lib/job"
	loggerPkg "github.com/ApoorvYdv/go-boilerplate/internal/logger"
	nrredis "github.com/newrelic/go-agent/v3/integrations/nrredis-v9"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

type Server struct {
	Config        *config.Config
	Logger        *zerolog.Logger
	LoggerService *loggerPkg.LoggerService
	DB            *database.Database
	Redis         *redis.Client
	httpServer    *http.Server
	Job           *job.JobService
}

func New(cfg *config.Config, logger *zerolog.Logger, loggerService *loggerPkg.LoggerService) (*Server, error) {
	db, err := database.New(cfg, logger, loggerService)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Redis client with New Relic integration
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.Redis.Address})

	// Add New Relic Redis hooks if possible
	if loggerService != nil && loggerService.GetApplication() != nil {
		redisClient.AddHook(nrredis.NewHook(redisClient.Options()))
	}

	// Test Redis connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	logger.Info().Msg("Redis connection successful")

	// Initialize Job service
	jobService := job.NewJobService(logger, cfg)
	jobService.InitHandlers(cfg, logger)

	// Start job server
	if err := jobService.Start(); err != nil {
		return nil, fmt.Errorf("failed to start job server: %w", err)
	}

	// Create HTTP server
	server := &Server{
		Config:        cfg,
		Logger:        logger,
		LoggerService: loggerService,
		DB:            db,
		Redis:         redisClient,
		Job:           jobService,
	}

	return server, nil
}

func (s *Server) SetupHTTPServer(handler http.Handler) {
	s.httpServer = &http.Server{
		Addr:         ":" + s.Config.Server.Port,
		Handler:      handler,
		ReadTimeout:  time.Duration(s.Config.Server.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(s.Config.Server.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(s.Config.Server.IdleTimeout) * time.Second,
	}
}

func (s *Server) Start() error {
	if s.httpServer == nil {
		return errors.New("http server not initialized")
	}

	s.Logger.Info().Str("port", s.Config.Server.Port).Str("env", s.Config.Primary.Env).Msg("Starting HTTP server")

	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown HTTP servers: %w", err)
	}

	if err := s.DB.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	if err := s.Redis.Close(); err != nil {
		return fmt.Errorf("failed to close Redis: %w", err)
	}

	if s.Job != nil {
		s.Job.Stop()
	}

	return nil
}
