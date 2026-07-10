# Temporary Cloud Run deployment for the pull-based transcode orchestrator.
#
# The fixed instance and instance-based CPU allocation are intentional: both
# Pub/Sub subscribers process work outside the HTTP request lifecycle.

# --- Variables ---

project_id := "apex-494315"
region     := "asia-south1"
registry   := "asia-south1-docker.pkg.dev"
repo       := "prj-apex-artifact-registry"
service    := "transcode-orchestrator"
sa         := "ci-cd-98@apex-494315.iam.gserviceaccount.com"
image      := registry + "/" + project_id + "/" + repo + "/" + service
git_sha    := `git rev-parse --short HEAD`

spanner_db             := "projects/apex-494315/instances/apex-spanner-instance/databases/apex-database"
subscription           := "projects/apex-494315/subscriptions/sub-transcode-orch"
completion_subscription := "projects/apex-494315/subscriptions/sub-transcode-completion"
normal_topic           := "projects/apex-494315/topics/transcode.job.requested"
priority_topic         := "projects/apex-494315/topics/transcode.job.requested.priority"
output_bucket          := "prj-apex-trancoded-data"

# --- Build & Push ---

docker-auth:
  gcloud auth configure-docker {{registry}}

build:
  docker buildx build --platform=linux/amd64 --load \
    -t prj-apex-transcode-orchestrator-service \
    .

push:
  docker buildx build --platform=linux/amd64 --push \
    -t {{image}}:{{git_sha}} \
    -t {{image}}:latest \
    .

# --- Cloud Run Deploy (temporary -- migrate to GKE later) ---

deploy: push
  gcloud run deploy {{service}} \
    --project={{project_id}} \
    --image={{image}}:{{git_sha}} \
    --region={{region}} \
    --platform=managed \
    --execution-environment=gen2 \
    --no-allow-unauthenticated \
    --service-account={{sa}} \
    --cpu=1 \
    --memory=512Mi \
    --concurrency=1 \
    --scaling=1 \
    --no-cpu-throttling \
    --clear-secrets \
    --set-env-vars="APP_ENV=development,SERVICE={{service}},REGION={{region}},PROJECT_ID={{project_id}},SPANNER_DATABASE={{spanner_db}},SUBSCRIPTION={{subscription}},COMPLETION_SUBSCRIPTION={{completion_subscription}},NORMAL_TOPIC={{normal_topic}},PRIORITY_TOPIC={{priority_topic}},MAX_RENDITION_ATTEMPTS=3,STALL_THRESHOLD_MINUTES=15,OUTPUT_GCS_BUCKET={{output_bucket}},OTEL_RESOURCE_ATTRIBUTES=gcp.project_id={{project_id}},OTEL_EXPORTER_OTLP_ENDPOINT=https://telemetry.googleapis.com,GOOGLE_CLOUD_QUOTA_PROJECT={{project_id}}"

# --- Code Quality ---

fmt:
  go fmt ./...

vet:
  go vet ./...

lint:
  golangci-lint run

test:
  go test ./... -v
