terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# "hello-bucket" - уникальное имя внутри моего GCP
# "de-zoomcamp-2026-485615-hello-bucket" - уникальное имя в мире. 
# Это нужно, чтобы другие люди могли обращаться к моему бакету по этому имени.

resource "google_storage_bucket" "hello-bucket" {
  name          = var.bucket_name
  location      = var.bucket_location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = var.delete_object_age
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = var.abort_incomplete_multipart_upload_age
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "hello-dataset" {
  dataset_id                  = var.dataset_id
  location                    = var.dataset_location
}
