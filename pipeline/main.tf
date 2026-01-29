terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project = "de-zoomcamp-2026-485615"
  region  = "us-central1"
}

# "hello-bucket" - уникальное имя внутри моего GCP
# "de-zoomcamp-2026-485615-hello-bucket" - уникальное имя в мире. 
# Это нужно, чтобы другие люди могли обращаться к моему бакету по этому имени.

resource "google_storage_bucket" "hello-bucket" {
  name          = "de-zoomcamp-2026-485615-hello-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
