terraform {
  backend "gcs" {
    bucket = "apex-bkt-tf-state"
    prefix = "terraform/state/apex-transcode-orchestrator-service/development"
  }
}
