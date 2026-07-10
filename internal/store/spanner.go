package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/lifecycle"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/models"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/outbox"
	localmodels "github.com/ctrlaltcloud008-hub/prj-apex-transcode-orchestrator-service/internal/models"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

var ErrVideoNotFound = errors.New("video not found")

// renditionOutputPrefix builds the GCS output prefix for one encode attempt.
// The attempt is part of the path so a superseded worker (stall-sweep takeover,
// duplicate delivery) can never interleave segments with the winning attempt.
func renditionOutputPrefix(bucket, videoID, renditionName string, attempt int64) string {
	return fmt.Sprintf("gs://%s/%s/%s/a%d", bucket, videoID, renditionName, attempt)
}

type VideoDispatchRecord struct {
	Status          string
	UserTier        string
	SourceBucket    string
	SourceObject    string
	RenditionLadder []localmodels.RenditionSpec
}

type StalledJob struct {
	VideoID       string
	RenditionName string
	Attempt       int64
}

// FetchVideoForDispatch reads the columns needed to dispatch transcode jobs.
func FetchVideoForDispatch(ctx context.Context, txn *spanner.ReadWriteTransaction, videoID string) (*VideoDispatchRecord, error) {
	row, err := txn.ReadRow(ctx, "videos",
		spanner.Key{videoID},
		[]string{"status", "user_tier", "source_bucket", "source_object", "rendition_ladder"},
	)
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			return nil, ErrVideoNotFound
		}
		return nil, fmt.Errorf("read video: %w", err)
	}

	var status string
	var userTier spanner.NullString
	var sourceBucket, sourceObject string
	var ladderJSON spanner.NullJSON

	if err := row.Columns(&status, &userTier, &sourceBucket, &sourceObject, &ladderJSON); err != nil {
		return nil, fmt.Errorf("parse video row: %w", err)
	}

	var renditions []localmodels.RenditionSpec
	if ladderJSON.Valid {
		raw, err := json.Marshal(ladderJSON.Value)
		if err != nil {
			return nil, fmt.Errorf("marshal rendition_ladder: %w", err)
		}
		if err := json.Unmarshal(raw, &renditions); err != nil {
			return nil, fmt.Errorf("parse rendition_ladder: %w", err)
		}
	}

	return &VideoDispatchRecord{
		Status:          status,
		UserTier:        userTier.StringVal,
		SourceBucket:    sourceBucket,
		SourceObject:    sourceObject,
		RenditionLadder: renditions,
	}, nil
}

// JobsAlreadyCreated returns true if transcode_jobs rows already exist for this video.
func JobsAlreadyCreated(ctx context.Context, txn *spanner.ReadWriteTransaction, videoID string) (bool, error) {
	stmt := spanner.Statement{
		SQL:    `SELECT COUNT(1) FROM transcode_jobs WHERE video_id = @id`,
		Params: map[string]any{"id": videoID},
	}
	iter := txn.Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		return false, fmt.Errorf("count transcode_jobs: %w", err)
	}
	var count int64
	if err := row.Columns(&count); err != nil {
		return false, fmt.Errorf("scan count: %w", err)
	}
	return count > 0, nil
}

// CreateTranscodeJobsAndDispatch writes transcode_jobs rows, outbox entries, and advances video
// status to TRANSCODING — all within the caller's RW transaction.
func CreateTranscodeJobsAndDispatch(
	ctx context.Context,
	txn *spanner.ReadWriteTransaction,
	videoID, sourceGCSURI string,
	renditions []localmodels.RenditionSpec,
	userTier, outputBucket, normalTopic, priorityTopic string,
	startedAt time.Time,
) error {
	topic := normalTopic
	if userTier == string(models.UserTierPremium) {
		topic = priorityTopic
	}

	jobMutations := make([]*spanner.Mutation, 0, len(renditions))
	outboxEntries := make([]outbox.Entry, 0, len(renditions))

	for _, r := range renditions {
		outputGCSPrefix := renditionOutputPrefix(outputBucket, videoID, r.Name, 1)

		jobMutations = append(jobMutations, spanner.InsertOrUpdate("transcode_jobs",
			[]string{"video_id", "rendition_name", "attempt", "status", "created_at", "updated_at"},
			[]any{videoID, r.Name, 1, "PENDING", spanner.CommitTimestamp, spanner.CommitTimestamp},
		))

		outboxEntries = append(outboxEntries, outbox.Entry{
			VideoID: videoID,
			Topic:   topic,
			Payload: localmodels.TranscodeJobRequestedPayload{
				VideoID:         videoID,
				RenditionName:   r.Name,
				Attempt:         1,
				SourceGCSURI:    sourceGCSURI,
				OutputGCSPrefix: outputGCSPrefix,
				RenditionSpec:   r,
			},
		})
	}

	if err := txn.BufferWrite(jobMutations); err != nil {
		return fmt.Errorf("buffer transcode_jobs: %w", err)
	}

	if err := outbox.Write(ctx, txn, outboxEntries); err != nil {
		return fmt.Errorf("write outbox: %w", err)
	}

	if err := txn.BufferWrite([]*spanner.Mutation{
		spanner.Update("videos",
			[]string{"video_id", "status", "updated_at"},
			[]any{videoID, string(models.StatusTranscoding), spanner.CommitTimestamp},
		),
	}); err != nil {
		return fmt.Errorf("update video status: %w", err)
	}

	if err := lifecycle.AppendLifecycleEvents(ctx, txn, videoID, lifecycle.LifeCycleEventParams{
		FromStatus: models.StatusValidated,
		ToStatus:   models.StatusTranscoding,
		Actor:      "transcode-orchestrator",
		Reason:     "rendition jobs dispatched",
	}); err != nil {
		return fmt.Errorf("append lifecycle events: %w", err)
	}

	if err := lifecycle.TransitionVideoStage(ctx, txn, lifecycle.StageTransitionParams{
		VideoID:        videoID,
		FromStage:      models.StatusValidated,
		FromAttempt:    1,
		ToStage:        models.StatusTranscoding,
		ToAttempt:      1,
		TransitionedAt: startedAt,
		Outcome:        "SUCCEEDED",
		Actor:          "transcode-orchestrator",
	}); err != nil {
		return fmt.Errorf("transition validated stage: %w", err)
	}

	return nil
}

// HandleRenditionCompleted idempotently marks a rendition complete and, when this
// call flips the last rendition, advances the video to TRANSCODED and writes the
// video.transcoded outbox entry — all in the same transaction. The status flip
// must not run in a separate transaction: a crash between "last rendition
// COMPLETED" and "video TRANSCODED" would strand the video forever, because the
// redelivered completion event matches zero rows and skips the all-done check.
// Returns allDone=true only when this call performed the transition.
func HandleRenditionCompleted(
	ctx context.Context,
	spannerClient *spanner.Client,
	videoID, renditionName string,
	attempt int64,
	workerID, outputGCSPrefix string,
) (allDone bool, err error) {
	_, err = spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		allDone = false

		rowCount, err := txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE transcode_jobs
				SET status = 'COMPLETED',
				    completed_at = PENDING_COMMIT_TIMESTAMP(),
				    worker_id = @worker,
				    output_gcs_prefix = @prefix,
				    updated_at = PENDING_COMMIT_TIMESTAMP()
				WHERE video_id = @vid
				  AND rendition_name = @rend
				  AND attempt = @attempt
				  AND status = 'PROCESSING'`,
			Params: map[string]any{
				"vid":     videoID,
				"rend":    renditionName,
				"attempt": attempt,
				"worker":  workerID,
				"prefix":  outputGCSPrefix,
			},
		})
		if err != nil {
			return fmt.Errorf("update transcode_job completed: %w", err)
		}
		if rowCount == 0 {
			// Already processed — idempotent skip.
			return nil
		}

		stmt := spanner.Statement{
			SQL:    `SELECT COUNTIF(status = 'COMPLETED') = COUNT(1) AS all_done FROM transcode_jobs WHERE video_id = @id`,
			Params: map[string]any{"id": videoID},
		}
		iter := txn.Query(ctx, stmt)
		defer iter.Stop()

		row, err := iter.Next()
		if err != nil {
			return fmt.Errorf("all-done query: %w", err)
		}
		if err := row.Columns(&allDone); err != nil {
			return fmt.Errorf("scan all_done: %w", err)
		}

		if allDone {
			if err := markVideoTranscoded(ctx, txn, videoID); err != nil {
				return err
			}
		}
		return nil
	})
	return allDone, err
}

// markVideoTranscoded advances video status to TRANSCODED and emits the
// video.transcoded outbox entry. The update is conditional on the current
// status so concurrent or replayed transitions are no-ops.
func markVideoTranscoded(ctx context.Context, txn *spanner.ReadWriteTransaction, videoID string) error {
	transitionedAt := time.Now().UTC()
	rowCount, err := txn.Update(ctx, spanner.Statement{
		SQL: `UPDATE videos
			SET status = @to, updated_at = PENDING_COMMIT_TIMESTAMP()
			WHERE video_id = @vid AND status = @from`,
		Params: map[string]any{
			"vid":  videoID,
			"to":   string(models.StatusTranscoded),
			"from": string(models.StatusTranscoding),
		},
	})
	if err != nil {
		return fmt.Errorf("update video transcoded: %w", err)
	}
	if rowCount == 0 {
		// Video already past TRANSCODING (replay) — nothing to emit.
		return nil
	}

	if err := outbox.Write(ctx, txn, []outbox.Entry{
		{
			VideoID: videoID,
			Topic:   "video.transcoded",
			Payload: localmodels.VideoTranscodedPayload{VideoID: videoID},
		},
	}); err != nil {
		return fmt.Errorf("write video.transcoded outbox: %w", err)
	}

	if err := lifecycle.AppendLifecycleEvents(ctx, txn, videoID, lifecycle.LifeCycleEventParams{
		FromStatus: models.StatusTranscoding,
		ToStatus:   models.StatusTranscoded,
		Actor:      "transcode-orchestrator",
		Reason:     "all renditions completed",
	}); err != nil {
		return fmt.Errorf("append lifecycle events: %w", err)
	}
	if err := lifecycle.TransitionVideoStage(ctx, txn, lifecycle.StageTransitionParams{
		VideoID:        videoID,
		FromStage:      models.StatusTranscoding,
		FromAttempt:    1,
		ToStage:        models.StatusTranscoded,
		ToAttempt:      1,
		TransitionedAt: transitionedAt,
		Outcome:        "SUCCEEDED",
		Actor:          "transcode-orchestrator",
	}); err != nil {
		return fmt.Errorf("transition transcoding stage: %w", err)
	}

	return nil
}

// HandleRenditionFailed handles a FAILED completion event: retries up to maxAttempts, then fails the video.
func HandleRenditionFailed(
	ctx context.Context,
	spannerClient *spanner.Client,
	videoID, renditionName, errorMsg, sourceGCSURI string,
	attempt, maxAttempts int64,
	normalTopic, priorityTopic, userTier, outputBucket string,
	spec localmodels.RenditionSpec,
) error {
	_, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Mark this attempt as FAILED. errorMsg is raw FFmpeg stderr — it must
		// go through a bound parameter, never string-concatenated into SQL.
		if _, err := txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE transcode_jobs
				SET status = 'FAILED',
				    error_details = @details,
				    updated_at = PENDING_COMMIT_TIMESTAMP()
				WHERE video_id = @vid AND rendition_name = @rend AND attempt = @attempt`,
			Params: map[string]any{
				"vid":     videoID,
				"rend":    renditionName,
				"attempt": attempt,
				"details": spanner.NullJSON{Value: map[string]string{"message": errorMsg}, Valid: true},
			},
		}); err != nil {
			return fmt.Errorf("mark rendition failed: %w", err)
		}

		if attempt < maxAttempts {
			nextAttempt := attempt + 1
			outputGCSPrefix := renditionOutputPrefix(outputBucket, videoID, renditionName, nextAttempt)

			// Re-insert row with incremented attempt.
			if err := txn.BufferWrite([]*spanner.Mutation{
				spanner.InsertOrUpdate("transcode_jobs",
					[]string{"video_id", "rendition_name", "attempt", "status", "created_at", "updated_at"},
					[]any{videoID, renditionName, nextAttempt, "PENDING", spanner.CommitTimestamp, spanner.CommitTimestamp},
				),
			}); err != nil {
				return fmt.Errorf("re-insert transcode_job: %w", err)
			}

			topic := normalTopic
			if userTier == string(models.UserTierPremium) {
				topic = priorityTopic
			}

			if err := outbox.Write(ctx, txn, []outbox.Entry{
				{
					VideoID: videoID,
					Topic:   topic,
					Payload: localmodels.TranscodeJobRequestedPayload{
						VideoID:         videoID,
						RenditionName:   renditionName,
						Attempt:         nextAttempt,
						SourceGCSURI:    sourceGCSURI,
						OutputGCSPrefix: outputGCSPrefix,
						RenditionSpec:   spec,
					},
				},
			}); err != nil {
				return fmt.Errorf("write retry outbox: %w", err)
			}
			return nil
		}

		// Max attempts exhausted — check if the whole video should be FAILED.
		row, err := txn.Query(ctx, spanner.Statement{
			SQL:    `SELECT COUNT(1) FROM transcode_jobs WHERE video_id = @id AND status NOT IN ('COMPLETED','FAILED')`,
			Params: map[string]any{"id": videoID},
		}).Next()
		if err != nil {
			return fmt.Errorf("check remaining jobs: %w", err)
		}
		var remaining int64
		if err := row.Columns(&remaining); err != nil {
			return fmt.Errorf("scan remaining: %w", err)
		}
		if remaining > 0 {
			return nil
		}

		return markVideoFailed(ctx, txn, videoID, "max rendition attempts exhausted")
	})
	return err
}

// ScanStalledJobs returns transcode_jobs rows stuck in PROCESSING past the stall threshold.
func ScanStalledJobs(ctx context.Context, spannerClient *spanner.Client, stallThreshold time.Duration) ([]StalledJob, error) {
	thresholdMinutes := int64(stallThreshold.Minutes())

	stmt := spanner.Statement{
		SQL: `SELECT video_id, rendition_name, attempt
			FROM transcode_jobs
			WHERE status = 'PROCESSING'
			  AND last_heartbeat_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @thresh MINUTE)`,
		Params: map[string]any{"thresh": thresholdMinutes},
	}

	iter := spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var jobs []StalledJob
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("scan stalled jobs: %w", err)
		}
		var j StalledJob
		if err := row.Columns(&j.VideoID, &j.RenditionName, &j.Attempt); err != nil {
			return nil, fmt.Errorf("parse stalled job row: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// RedispatchStalledJob re-checks staleness inside a transaction then increments attempt and re-queues.
func RedispatchStalledJob(
	ctx context.Context,
	spannerClient *spanner.Client,
	job StalledJob,
	stallThreshold time.Duration,
	normalTopic, priorityTopic, userTier, outputBucket, sourceGCSURI string,
	spec localmodels.RenditionSpec,
) error {
	thresholdMinutes := int64(stallThreshold.Minutes())

	_, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// THEN RETURN yields the post-update attempt so the published payload
		// always matches the row, even if a concurrent sweep already bumped it
		// between the scan and this transaction.
		iter := txn.Query(ctx, spanner.Statement{
			SQL: `UPDATE transcode_jobs
				SET attempt = attempt + 1,
				    status = 'PENDING',
				    updated_at = PENDING_COMMIT_TIMESTAMP()
				WHERE video_id = @vid
				  AND rendition_name = @rend
				  AND status = 'PROCESSING'
				  AND last_heartbeat_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @thresh MINUTE)
				THEN RETURN attempt`,
			Params: map[string]any{
				"vid":    job.VideoID,
				"rend":   job.RenditionName,
				"thresh": thresholdMinutes,
			},
		})
		defer iter.Stop()

		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			// Job no longer stalled (heartbeat resumed or another sweep won).
			return nil
		}
		if err != nil {
			return fmt.Errorf("update stalled job: %w", err)
		}

		var nextAttempt int64
		if err := row.Columns(&nextAttempt); err != nil {
			return fmt.Errorf("scan redispatched attempt: %w", err)
		}

		outputGCSPrefix := renditionOutputPrefix(outputBucket, job.VideoID, job.RenditionName, nextAttempt)

		topic := normalTopic
		if userTier == string(models.UserTierPremium) {
			topic = priorityTopic
		}

		return outbox.Write(ctx, txn, []outbox.Entry{
			{
				VideoID: job.VideoID,
				Topic:   topic,
				Payload: localmodels.TranscodeJobRequestedPayload{
					VideoID:         job.VideoID,
					RenditionName:   job.RenditionName,
					Attempt:         nextAttempt,
					SourceGCSURI:    sourceGCSURI,
					OutputGCSPrefix: outputGCSPrefix,
					RenditionSpec:   spec,
				},
			},
		})
	})
	return err
}

func markVideoFailed(ctx context.Context, txn *spanner.ReadWriteTransaction, videoID, reason string) error {
	transitionedAt := time.Now().UTC()
	if err := txn.BufferWrite([]*spanner.Mutation{
		spanner.Update("videos",
			[]string{"video_id", "status", "updated_at"},
			[]any{videoID, string(models.StatusFailed), spanner.CommitTimestamp},
		),
	}); err != nil {
		return fmt.Errorf("update video failed: %w", err)
	}

	if err := lifecycle.AppendLifecycleEvents(ctx, txn, videoID, lifecycle.LifeCycleEventParams{
		FromStatus: models.StatusTranscoding,
		ToStatus:   models.StatusFailed,
		Actor:      "transcode-orchestrator",
		Reason:     reason,
	}); err != nil {
		return fmt.Errorf("append lifecycle events: %w", err)
	}

	if err := lifecycle.TransitionVideoStage(ctx, txn, lifecycle.StageTransitionParams{
		VideoID:        videoID,
		FromStage:      models.StatusTranscoding,
		FromAttempt:    1,
		ToStage:        models.StatusFailed,
		ToAttempt:      1,
		TransitionedAt: transitionedAt,
		Outcome:        "FAILED",
		Actor:          "transcode-orchestrator",
	}); err != nil {
		return fmt.Errorf("transition failed stage: %w", err)
	}
	return nil
}

// FetchVideoSourceAndRendition reads a video's source GCS URI and a specific rendition spec.
// Used by the stall sweep to reconstruct the work item payload.
func FetchVideoSourceAndRendition(ctx context.Context, spannerClient *spanner.Client, videoID, renditionName string) (sourceGCSURI, userTier string, spec localmodels.RenditionSpec, err error) {
	row, err := spannerClient.Single().ReadRow(ctx, "videos",
		spanner.Key{videoID},
		[]string{"source_bucket", "source_object", "user_tier", "rendition_ladder"},
	)
	if err != nil {
		return "", "", spec, fmt.Errorf("read video for stall: %w", err)
	}

	var bucket, object string
	var tier spanner.NullString
	var ladderJSON spanner.NullJSON
	if err := row.Columns(&bucket, &object, &tier, &ladderJSON); err != nil {
		return "", "", spec, fmt.Errorf("parse video row: %w", err)
	}

	sourceGCSURI = fmt.Sprintf("gs://%s/%s", bucket, object)
	userTier = tier.StringVal

	if ladderJSON.Valid {
		raw, marshalErr := json.Marshal(ladderJSON.Value)
		if marshalErr != nil {
			return "", "", spec, fmt.Errorf("marshal rendition_ladder: %w", marshalErr)
		}
		var all []localmodels.RenditionSpec
		if err := json.Unmarshal(raw, &all); err != nil {
			return "", "", spec, fmt.Errorf("parse rendition_ladder: %w", err)
		}
		for _, r := range all {
			if r.Name == renditionName {
				spec = r
				break
			}
		}
	}
	return sourceGCSURI, userTier, spec, nil
}
