terraform {
  required_providers {
    # В видео тут "hashicorp/google". У нас — Docker.
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.1"
    }
  }
}

provider "docker" {}

# 1. Скачиваем образ (В видео это аналог настройки доступа)
resource "docker_image" "postgres" {
  name         = "postgres:13"
  keep_locally = true
}

resource "docker_image" "pgadmin" {
  name         = "dpage/pgadmin4"
  keep_locally = true
}

# 2. Создаем сеть (В видео этого нет, но в Докере это важно)
resource "docker_network" "private_network" {
  name = "my_terraform_network"
}

# 3. Поднимаем Базу (В видео это resource "google_bigquery_dataset")
resource "docker_container" "postgres" {
  image = docker_image.postgres.image_id
  name  = "ny_taxi_postgres_tf" # Имя контейнера

  ports {
    internal = 5432
    external = 5432
  }

  # Переменные окружения (те же, что были в docker-compose)
  env = [
    "POSTGRES_USER=root",
    "POSTGRES_PASSWORD=root",
    "POSTGRES_DB=ny_taxi"
  ]

  networks_advanced {
    name = docker_network.private_network.name
  }

  # Persistent volume (чтобы данные не пропали)
  volumes {
    host_path      = "${abspath(path.module)}/ny_taxi_postgres_data"
    container_path = "/var/lib/postgresql/data"
  }
}

# 4. Поднимаем pgAdmin (В видео этого нет, но нам удобно)
resource "docker_container" "pgadmin" {
  image = docker_image.pgadmin.image_id
  name  = "pgadmin_tf"

  ports {
    internal = 80
    external = 8080
  }

  env = [
    "PGADMIN_DEFAULT_EMAIL=admin@admin.com",
    "PGADMIN_DEFAULT_PASSWORD=root"
  ]

  networks_advanced {
    name = docker_network.private_network.name
  }
}
