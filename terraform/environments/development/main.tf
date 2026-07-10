module "pubsub" {
  source                          = "../../modules/pubsub"
  project_id                      = var.project_id
  project_region                  = var.project_region
  environment                     = var.environment
  region                          = var.project_region
  service_url                     = var.service_url
  scheduler_service_account_email = var.scheduler_service_account_email
}

module "storage" {
  source         = "../../modules/storage"
  project_id     = var.project_id
  project_region = var.project_region
  environment    = var.environment
}
