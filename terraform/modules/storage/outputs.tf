output "transcoder_output_bucket_name" {
  description = "The name of the Cloud Storage bucket for transcoded video outputs."
  value       = google_storage_bucket.transcoded.name
}
