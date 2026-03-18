#!/bin/bash

# Останавливаем выполнение при любой ошибке
set -e

# Подгружаем переменные из .env, если они там есть
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Проверка обязательных переменных
: "${PROJECT_ID:?Проверь PROJECT_ID в .env или экспортах}"
: "${BUCKET_NAME:?Проверь BUCKET_NAME в .env или экспортах}"

echo "🚀 Запуск Spark Job для проекта: $PROJECT_ID"

spark-submit \
  --packages com.google.cloud.spark:spark-4.1-bigquery:0.44.0-preview,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22 \
  --conf "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" \
  --conf "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" \
  06_spark_sql_big_query.py \
  --input_green "data/pq/green/" \
  --input_yellow "data/pq/yellow/" \
  --output "${PROJECT_ID}.zoomcamp_spark_test.report_2020" \
  --gcp_temp_bucket "${BUCKET_NAME}"
