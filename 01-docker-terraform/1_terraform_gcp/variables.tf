variable "project" {
  description = "Project Name"
  default     = "versatile-age-411912"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "fmz_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "fmz-data-engineering-project-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my_keys.json"
}