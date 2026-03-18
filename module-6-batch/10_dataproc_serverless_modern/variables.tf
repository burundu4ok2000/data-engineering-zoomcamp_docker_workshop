variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region for all resources"
  type        = string
}

variable "subnet_name" {
  description = "Name for the Serverless subnet"
  type        = string
}

variable "bucket_name" {
  description = "The name of the existing GCS bucket with parquet files"
  type        = string
}

variable "bq_dataset" {
  description = "The existing BigQuery dataset ID"
  type        = string
}

variable "pyspark_script_path" {
  description = "URI of the PySpark script inside the GCS bucket"
  type        = string
}