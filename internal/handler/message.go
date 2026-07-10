package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/apperror"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/logger"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/models"
	pbclient "github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/pubsub"
	localmodels "github.com/ctrlaltcloud008-hub/prj-apex-transcode-orchestrator-service/internal/models"
	"github.com/ctrlaltcloud008-hub/prj-apex-transcode-orchestrator-service/internal/services"
)

type Handler struct {
	logger  *logger.Logger
	process *services.MessageProcessor
}

func NewHandler(logger *logger.Logger, process *services.MessageProcessor) *Handler {
	return &Handler{
		logger:  logger,
		process: process,
	}
}

// HandleValidatedMessage processes video.validated Pub/Sub messages.
func (h *Handler) HandleValidatedMessage(ctx context.Context, msg *pubsub.Message) {
	ctx, span := pbclient.StartConsumerSpan(ctx, msg, "message.validated.receive")
	defer span.End()

	log := h.logger.WithSpanContext(ctx)
	startedAt := time.Now()
	requestID := msg.ID

	var payload models.VideoValidatedPayload
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		// Malformed payloads never succeed on retry — ack and discard rather
		// than poison-looping through the DLQ budget.
		log.Error(ctx, "message.parse_error", "failed to parse video.validated message; discarding",
			slog.String("request_id", requestID),
			slog.String("error", err.Error()),
			slog.String("outcome", "failure"),
			slog.Bool("audit", true),
		)
		msg.Ack()
		return
	}

	err := h.process.ProcessValidatedMessage(ctx, requestID, &payload, startedAt)
	if err != nil {
		errType := apperror.Classify(err)
		log.Error(ctx, "message.process_error", "failed to process video.validated",
			slog.String("request_id", requestID),
			slog.String("video_id", payload.VideoID),
			slog.String("error", err.Error()),
			slog.String("error_type", string(errType)),
			slog.String("outcome", "failure"),
			slog.Bool("audit", true),
		)
		// Only provably-permanent errors are acked; ambiguous errors retry
		// (and dead-letter if they keep failing) instead of stranding the video.
		if errType == apperror.Permanent {
			msg.Ack()
			return
		}
		msg.Nack()
		return
	}

	log.Info(ctx, "message.processed", "video.validated processed",
		slog.String("request_id", requestID),
		slog.String("video_id", payload.VideoID),
		slog.String("outcome", "success"),
		slog.Bool("audit", true),
	)
	msg.Ack()
}

// HandleCompletionMessage processes transcode.job.completed Pub/Sub messages.
func (h *Handler) HandleCompletionMessage(ctx context.Context, msg *pubsub.Message) {
	ctx, span := pbclient.StartConsumerSpan(ctx, msg, "message.completion.receive")
	defer span.End()

	log := h.logger.WithSpanContext(ctx)
	requestID := msg.ID

	var payload localmodels.TranscodeJobCompletedPayload
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		// Malformed payloads never succeed on retry — ack and discard.
		log.Error(ctx, "message.parse_error", "failed to parse transcode.job.completed message; discarding",
			slog.String("request_id", requestID),
			slog.String("error", err.Error()),
			slog.String("outcome", "failure"),
			slog.Bool("audit", true),
		)
		msg.Ack()
		return
	}

	err := h.process.ProcessCompletionMessage(ctx, requestID, &payload)
	if err != nil {
		errType := apperror.Classify(err)
		log.Error(ctx, "message.process_error", "failed to process transcode.job.completed",
			slog.String("request_id", requestID),
			slog.String("video_id", payload.VideoID),
			slog.String("rendition_name", payload.RenditionName),
			slog.String("error", err.Error()),
			slog.String("error_type", string(errType)),
			slog.String("outcome", "failure"),
			slog.Bool("audit", true),
		)
		// Only provably-permanent errors are acked; ambiguous errors retry.
		if errType == apperror.Permanent {
			msg.Ack()
			return
		}
		msg.Nack()
		return
	}

	log.Info(ctx, "message.processed", "transcode.job.completed processed",
		slog.String("request_id", requestID),
		slog.String("video_id", payload.VideoID),
		slog.String("rendition_name", payload.RenditionName),
		slog.String("status", payload.Status),
		slog.String("outcome", "success"),
		slog.Bool("audit", true),
	)
	msg.Ack()
}
