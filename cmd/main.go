package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/logger"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/otel"
	pbclient "github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/pubsub"
	"github.com/ctrlaltcloud008-hub/prj-apex-transcode-orchestrator-service/internal/config"
	"github.com/ctrlaltcloud008-hub/prj-apex-transcode-orchestrator-service/internal/handler"
	"github.com/ctrlaltcloud008-hub/prj-apex-transcode-orchestrator-service/internal/pubsub"
	"github.com/ctrlaltcloud008-hub/prj-apex-transcode-orchestrator-service/internal/services"
	"github.com/ctrlaltcloud008-hub/prj-apex-transcode-orchestrator-service/internal/spanner"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func run() error {
	cfg, err := config.LoadOrchestratorConfig()
	if err != nil {
		return err
	}

	log := logger.New(cfg.Service(), cfg.Region(), cfg.AppEnv())

	log.Info(context.Background(), "service.startup", "transcode orchestrator bootstrap started",
		slog.String("component", "bootstrap"),
		slog.String("http_addr", cfg.Port()),
		slog.String("project_id", cfg.ProjectID()),
		slog.String("spanner_database", cfg.SpannerDatabase()),
		slog.Bool("audit", false),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdown, err := otel.InitTracer(ctx, otel.TracerConfig{
		AppEnv:      cfg.AppEnv(),
		ServiceName: cfg.Service(),
		ProjectID:   cfg.ProjectID(),
		Region:      cfg.Region(),
	})
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(shutdownCtx); err != nil {
			log.Error(context.Background(), "tracer.shutdown_failed", "failed to shutdown tracer",
				slog.String("error", err.Error()),
				slog.Bool("audit", true),
			)
		}
	}()

	pubsubClient, err := pubsub.NewClient(ctx, pubsub.Config{
		ProjectID:      cfg.ProjectID(),
		EnabledTracing: true,
	})
	if err != nil {
		return err
	}
	defer pubsubClient.Close()

	validatedSubscriber := pbclient.NewSubscriber(pubsubClient, cfg.Subscription(),
		pbclient.WithMaxOutstandingMessages(100),
		pbclient.WithNumGoroutines(10),
		pbclient.WithMaxOutstandingBytes(100*1024*1024),
	)

	completionSubscriber := pbclient.NewSubscriber(pubsubClient, cfg.CompletionSubscription(),
		pbclient.WithMaxOutstandingMessages(200),
		pbclient.WithNumGoroutines(20),
		pbclient.WithMaxOutstandingBytes(100*1024*1024),
	)

	spannerClient, err := spanner.NewClient(ctx, cfg.SpannerDatabase(), spanner.DefaultConfig())
	if err != nil {
		return err
	}
	defer spannerClient.Close()

	log.Info(ctx, "service.startup_complete", "all dependencies initialized",
		slog.String("component", "bootstrap"),
		slog.Bool("audit", false),
	)

	processor := services.NewMessageProcessor(log, spannerClient, cfg)
	msgHandler := handler.NewHandler(log, processor)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.Healthz)
	mux.HandleFunc("POST /stall-sweep", msgHandler.StallSweep)

	server := &http.Server{
		Addr:    cfg.Port(),
		Handler: mux,
	}

	errCh := make(chan error, 3)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info(ctx, "subscriber.starting", "starting video.validated subscriber",
			slog.String("subscription", cfg.Subscription()),
			slog.Bool("audit", false),
		)
		if err := validatedSubscriber.Receive(ctx, msgHandler.HandleValidatedMessage); err != nil {
			errCh <- fmt.Errorf("validated subscriber: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info(ctx, "subscriber.starting", "starting transcode.job.completed subscriber",
			slog.String("subscription", cfg.CompletionSubscription()),
			slog.Bool("audit", false),
		)
		if err := completionSubscriber.Receive(ctx, msgHandler.HandleCompletionMessage); err != nil {
			errCh <- fmt.Errorf("completion subscriber: %w", err)
		}
	}()

	var runErr error
	select {
	case <-ctx.Done():
		log.Info(ctx, "shutdown.initiated", "shutdown signal received",
			slog.String("reason", "signal"),
			slog.Bool("audit", false),
		)
	case err := <-errCh:
		runErr = err
		log.Error(ctx, "service.fatal", "fatal error",
			slog.String("error", err.Error()),
			slog.Bool("audit", true),
		)
	}
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error(context.Background(), "server.shutdown_failed", "HTTP server shutdown failed",
			slog.String("error", err.Error()),
			slog.Bool("audit", true),
		)
		if runErr == nil {
			runErr = fmt.Errorf("http server shutdown: %w", err)
		}
	}

	wg.Wait()
	return runErr
}
