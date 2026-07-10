resource "google_storage_bucket" "transcoded" {
  project  = var.project_id
  name     = var.trancoder_bucket_name
  location = var.project_region

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }

    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365
    }

    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }

  force_destroy = true

  labels = {
    team        = "apex"
    environment = var.environment
    managed_by  = "terraform"
  }

}
