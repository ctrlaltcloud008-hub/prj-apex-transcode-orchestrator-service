locals {
  default_labels = {
    team        = "apex"
    environment = var.environment
    managed_by  = "terraform"
  }
}

data "google_pubsub_topic" "validated" {
  project = var.project_id
  name    = "video.validated"
}

data "google_pubsub_topic" "job_completed" {
  project = var.project_id
  name    = "transcode.job.completed"
}

data "google_pubsub_topic" "job_requested" {
  project = var.project_id
  name    = "transcode.job.requested"
}

data "google_pubsub_topic" "job_requested_priority" {
  project = var.project_id
  name    = "transcode.job.requested.priority"
}

data "google_pubsub_topic" "dlq" {
  project = var.project_id
  name    = "video.dlq"
}

# Subscription for video.validated (dispatch path).
resource "google_pubsub_subscription" "validated" {
  project = var.project_id
  name    = "sub-transcode-orch"
  topic   = data.google_pubsub_topic.validated.id

  ack_deadline_seconds       = var.ack_deadline_seconds
  message_retention_duration = var.message_retention_duration
  retain_acked_messages      = false

  retry_policy {
    minimum_backoff = var.retry_minimum_backoff
    maximum_backoff = var.retry_maximum_backoff
  }

  dead_letter_policy {
    dead_letter_topic     = data.google_pubsub_topic.dlq.id
    max_delivery_attempts = var.dlq_max_delivery_attempts
  }

  labels = local.default_labels
}

# Subscription for transcode.job.completed (completion tracking path).
resource "google_pubsub_subscription" "completion" {
  project = var.project_id
  name    = "sub-transcode-completion"
  topic   = data.google_pubsub_topic.job_completed.id

  ack_deadline_seconds       = var.ack_deadline_seconds
  message_retention_duration = var.message_retention_duration
  retain_acked_messages      = false

  retry_policy {
    minimum_backoff = var.retry_minimum_backoff
    maximum_backoff = var.retry_maximum_backoff
  }

  dead_letter_policy {
    dead_letter_topic     = data.google_pubsub_topic.dlq.id
    max_delivery_attempts = var.dlq_max_delivery_attempts
  }

  labels = local.default_labels
}

# Cloud Scheduler job for stall detection (every 5 minutes).
resource "google_cloud_scheduler_job" "stall_sweep" {
  project  = var.project_id
  region   = var.region
  name     = "transcode-orchestrator-stall-sweep"
  schedule = "*/5 * * * *"

  http_target {
    http_method = "POST"
    uri         = "${var.service_url}/stall-sweep"

    oidc_token {
      service_account_email = var.scheduler_service_account_email
    }
  }

  retry_config {
    retry_count = 1
  }
}
