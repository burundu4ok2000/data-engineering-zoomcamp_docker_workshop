terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

# Настраиваем провайдер
provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. Включаем Private Google Access на подсети
resource "google_compute_subnetwork" "serverless_subnet" {
  name                     = var.subnet_name
  ip_cidr_range            = "10.0.1.0/24"
  region                   = var.region 
  network                  = "default"
  private_ip_google_access = true
}

# 2. ПРОПУЩЕННЫЙ ПАЗЛ: Файрвол для общения Воркеров и Мастера
resource "google_compute_firewall" "allow_dataproc_internal" {
  name    = "allow-dataproc-internal"
  network = "default" # Сеть, к которой привязана наша подсеть

  # Разрешаем весь TCP, UDP и ICMP (ping) трафик
  allow {
    protocol = "tcp"
  }
  allow {
    protocol = "udp"
  }
  allow {
    protocol = "icmp"
  }

  # Ограничиваем радиус действия: правило работает ТОЛЬКО для нашей подсети
  source_ranges = [google_compute_subnetwork.serverless_subnet.ip_cidr_range]
}

# 3. Создаем бессерверный Batch Job
resource "google_dataproc_batch" "yellow_taxi_processing" {
  project  = var.project_id
  location = var.region 

  batch_id = "taxi-etl-${formatdate("YYYYMMDD-hhmmss", timestamp())}"

  pyspark_batch {
    main_python_file_uri = var.pyspark_script_path
    
    args = [
        "--input_path", "gs://${var.bucket_name}/yellow_tripdata_2024-*.parquet",
        # ИЗМЕНЕНИЕ: Пишем в новую, чистую таблицу, чтобы избежать конфликтов схем
        "--bq_table", "${var.project_id}.${var.bq_dataset}.yellow_tripdata_terraform_dataproc_serverless_test"
        ]
  }

  environment_config {
    execution_config {
      subnetwork_uri = google_compute_subnetwork.serverless_subnet.id
    }
  }

  runtime_config {
    # Приводим к минимальным требованиям Google Cloud (4, 8 или 16 ядер)
    properties = {
      "spark.executor.instances" = "2"
      "spark.driver.cores"       = "4"
      "spark.executor.cores"     = "4"
      "spark.driver.memory"      = "8g"
      "spark.executor.memory"    = "8g"
    }
  }

  # Явное указание Терраформу: сначала подними сеть и файрвол, и только потом запускай Спарк
  depends_on = [
    google_compute_subnetwork.serverless_subnet,
    google_compute_firewall.allow_dataproc_internal
  ]
}