variable "credentials" {
  description = "Credentials"
  default     = "./keys/ny-taxi-data-412619-a66b3a72c5f7.json"
}

variable "project" {
  description = "Project"
  default     = "ny-taxi-data-412619"
}

variable "region" {
  description = "Region"
  default     = "us-east1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My Big Query Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "ny-green-taxi-data"
}