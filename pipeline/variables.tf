variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
}

variable "bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
}

variable "bucket_location" {
  description = "The location of the GCS bucket"
  type        = string
}

variable "dataset_id" {
  description = "The BigQuery dataset ID"
  type        = string
}

variable "dataset_location" {
  description = "The location of the BigQuery dataset"
  type        = string
}

variable "delete_object_age" {
  description = "Age in days for deleting objects in the bucket"
  type        = number
}

variable "abort_incomplete_multipart_upload_age" {
  description = "Age in days for aborting incomplete multipart uploads"
  type        = number
}
