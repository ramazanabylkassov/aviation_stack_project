variable "credentials" {
  description = "My credentials"
  default     = "../GC_creds/de-project-flight-analyzer-3354c85abe73.json"
}

variable "project_name" {
  description = "Name of the project"
  default     = "de-project-flight-analyzer"
}

variable "project_region" {
  description = "region of the project"
  default     = "us-centrall"
}

variable "location" {
  description = "Project location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "cities_raw_data"
}

variable "composer_service_account" {
  description = "Service account for composer"
  default     = "composer-251@de-project-flight-analyzer.iam.gserviceaccount.com"
}