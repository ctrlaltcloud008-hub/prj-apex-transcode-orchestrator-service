package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/logger"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/models"
	spannerutils "github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/spanner"
	"github.com/ctrlaltcloud008-hub/prj-apex-transcode-orchestrator-service/internal/config"
	localmodels "github.com/ctrlaltcloud008-hub/prj-apex-transcode-orchestrator-service/internal/models"
	"github.com/ctrlaltcloud008-hub/prj-apex-transcode-orchestrator-service/internal/store"
)

type MessageProcessor struct {
	logger  *logger.Logger
	spanner *spanner.Client
	cfg     *config.OrchestratorConfig
}

func NewMessageProcessor(logger *logger.Logger, spannerClient *spanner.Client, cfg *config.OrchestratorConfig) *MessageProcessor {
	return &MessageProcessor{
		logger:  logger,
		spanner: spannerClient,
		cfg:     cfg,
	}
}

// ProcessValidatedMessage handles a video.validated event by creating transcode_jobs rows
// and dispatching work items via the outbox.
func (mp *MessageProcessor) ProcessValidatedMessage(ctx context.Context, requestID string, msg *models.VideoValidatedPayload, startedAt time.Time) error {
	log := mp.logger.WithSpanContext(ctx)

	var video *store.VideoDispatchRecord

	_, err := spannerutils.RunRW(ctx, mp.spanner, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		var fetchErr error
		video, fetchErr = store.FetchVideoForDispatch(ctx, txn, msg.VideoID)
		if fetchErr != nil {
			return fetchErr
		}

		if video.Status != string(models.StatusValidated) {
			log.Warn(ctx, "process.invalid_status", "video not in VALIDATED status, skipping",
				slog.String("request_id", requestID),
				slog.String("video_id", msg.VideoID),
				slog.String("status", video.Status),
				slog.String("outcome", "skipped"),
				slog.Bool("audit", true),
			)
			video = nil
			return nil
		}

		already, err := store.JobsAlreadyCreated(ctx, txn, msg.VideoID)
		if err != nil {
			return fmt.Errorf("check existing jobs: %w", err)
		}
		if already {
			log.Warn(ctx, "process.already_dispatched", "transcode jobs already exist, skipping",
				slog.String("request_id", requestID),
				slog.String("video_id", msg.VideoID),
				slog.String("outcome", "skipped"),
				slog.Bool("audit", true),
			)
			video = nil
			return nil
		}

		if len(video.RenditionLadder) == 0 {
			return fmt.Errorf("no renditions in ladder for video %s", msg.VideoID)
		}

		sourceGCSURI := fmt.Sprintf("gs://%s/%s", video.SourceBucket, video.SourceObject)
		return store.CreateTranscodeJobsAndDispatch(
			ctx, txn,
			msg.VideoID, sourceGCSURI,
			video.RenditionLadder,
			video.UserTier,
			mp.cfg.OutputGCSBucket(),
			mp.cfg.NormalTopic(),
			mp.cfg.PriorityTopic(),
			startedAt,
		)
	})

	if err != nil {
		if errors.Is(err, store.ErrVideoNotFound) {
			log.Warn(ctx, "process.video_not_found", "video not found, skipping",
				slog.String("request_id", requestID),
				slog.String("video_id", msg.VideoID),
				slog.String("outcome", "skipped"),
				slog.Bool("audit", true),
			)
			return nil
		}
		return err
	}

	if video != nil {
		log.Info(ctx, "process.dispatched", "transcode jobs dispatched",
			slog.String("request_id", requestID),
			slog.String("video_id", msg.VideoID),
			slog.Int("rendition_count", len(video.RenditionLadder)),
			slog.String("user_tier", video.UserTier),
			slog.String("outcome", "success"),
			slog.Bool("audit", true),
		)
	}
	return nil
}

// ProcessCompletionMessage handles a transcode.job.completed event.
func (mp *MessageProcessor) ProcessCompletionMessage(ctx context.Context, requestID string, msg *localmodels.TranscodeJobCompletedPayload) error {
	log := mp.logger.WithSpanContext(ctx)

	switch msg.Status {
	case "COMPLETED":
		// The TRANSCODED transition happens inside HandleRenditionCompleted's
		// transaction — never in a follow-up transaction, which a crash could skip.
		allDone, err := store.HandleRenditionCompleted(ctx, mp.spanner,
			msg.VideoID, msg.RenditionName, msg.Attempt, msg.WorkerID, msg.OutputGCSPrefix)
		if err != nil {
			return err
		}

		if allDone {
			log.Info(ctx, "process.all_renditions_done", "all renditions complete, video transcoded",
				slog.String("request_id", requestID),
				slog.String("video_id", msg.VideoID),
				slog.String("outcome", "success"),
				slog.Bool("audit", true),
			)
		} else {
			log.Info(ctx, "process.rendition_completed", "rendition marked complete",
				slog.String("request_id", requestID),
				slog.String("video_id", msg.VideoID),
				slog.String("rendition_name", msg.RenditionName),
				slog.String("outcome", "success"),
				slog.Bool("audit", false),
			)
		}

	case "FAILED":
		sourceGCSURI, userTier, spec, err := store.FetchVideoSourceAndRendition(ctx, mp.spanner, msg.VideoID, msg.RenditionName)
		if err != nil {
			return fmt.Errorf("fetch video for retry: %w", err)
		}

		if err := store.HandleRenditionFailed(ctx, mp.spanner,
			msg.VideoID, msg.RenditionName, msg.ErrorMessage, sourceGCSURI,
			msg.Attempt, mp.cfg.MaxRenditionAttempts(),
			mp.cfg.NormalTopic(), mp.cfg.PriorityTopic(), userTier,
			mp.cfg.OutputGCSBucket(), spec,
		); err != nil {
			return err
		}

		log.Warn(ctx, "process.rendition_failed", "rendition failed",
			slog.String("request_id", requestID),
			slog.String("video_id", msg.VideoID),
			slog.String("rendition_name", msg.RenditionName),
			slog.Int64("attempt", msg.Attempt),
			slog.Int64("max_attempts", mp.cfg.MaxRenditionAttempts()),
			slog.String("error", msg.ErrorMessage),
			slog.Bool("audit", true),
		)

	default:
		log.Warn(ctx, "process.unknown_status", "unknown completion status, acking",
			slog.String("request_id", requestID),
			slog.String("video_id", msg.VideoID),
			slog.String("status", msg.Status),
			slog.Bool("audit", true),
		)
	}

	return nil
}

// ProcessStallSweep scans for stalled transcode_jobs and re-dispatches them.
// Intended to be called by the /stall-sweep HTTP endpoint triggered by Cloud Scheduler.
func (mp *MessageProcessor) ProcessStallSweep(ctx context.Context) error {
	log := mp.logger.WithSpanContext(ctx)

	threshold := time.Duration(mp.cfg.StallThresholdMinutes()) * time.Minute
	jobs, err := store.ScanStalledJobs(ctx, mp.spanner, threshold)
	if err != nil {
		return fmt.Errorf("scan stalled jobs: %w", err)
	}

	log.Info(ctx, "stall_sweep.found", "stalled jobs found",
		slog.Int("count", len(jobs)),
		slog.Bool("audit", false),
	)

	for _, job := range jobs {
		sourceGCSURI, userTier, spec, err := store.FetchVideoSourceAndRendition(ctx, mp.spanner, job.VideoID, job.RenditionName)
		if err != nil {
			log.Error(ctx, "stall_sweep.fetch_error", "failed to fetch video for stalled job",
				slog.String("video_id", job.VideoID),
				slog.String("rendition_name", job.RenditionName),
				slog.String("error", err.Error()),
				slog.Bool("audit", true),
			)
			continue
		}

		if err := store.RedispatchStalledJob(ctx, mp.spanner, job, threshold,
			mp.cfg.NormalTopic(), mp.cfg.PriorityTopic(), userTier,
			mp.cfg.OutputGCSBucket(), sourceGCSURI, spec,
		); err != nil {
			log.Error(ctx, "stall_sweep.redispatch_error", "failed to redispatch stalled job",
				slog.String("video_id", job.VideoID),
				slog.String("rendition_name", job.RenditionName),
				slog.String("error", err.Error()),
				slog.Bool("audit", true),
			)
			continue
		}

		log.Info(ctx, "stall_sweep.redispatched", "stalled job redispatched",
			slog.String("video_id", job.VideoID),
			slog.String("rendition_name", job.RenditionName),
			slog.Int64("attempt", job.Attempt),
			slog.Bool("audit", true),
		)
	}

	return nil
}
