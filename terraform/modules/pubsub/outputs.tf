output "transcode_job_requested_topic_id" {
  description = "The ID of the centrally managed transcode.job.requested topic."
  value       = data.google_pubsub_topic.job_requested.id
}

output "transcode_job_requested_priority_topic_id" {
  description = "The ID of the centrally managed premium-tier priority work topic."
  value       = data.google_pubsub_topic.job_requested_priority.id
}
