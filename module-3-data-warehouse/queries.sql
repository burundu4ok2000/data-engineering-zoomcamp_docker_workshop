-- ============================================================================
-- Module 3 Homework: BigQuery & Data Warehouse
-- Yellow Taxi Trip Records (January - June 2024)
-- ============================================================================

-- ============================================================================
-- SETUP: Create External Table
-- ============================================================================
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp-2026-485615-yellow-taxi-2024/yellow_tripdata_2024-*.parquet']
);

-- ============================================================================
-- SETUP: Create Materialized (Regular) Table
-- ============================================================================
CREATE OR REPLACE TABLE `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024`
AS SELECT * FROM `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024_external`;

-- ============================================================================
-- QUESTION 1: Counting records
-- What is count of records for the 2024 Yellow Taxi Data?
-- ============================================================================
SELECT COUNT(*) as total_records
FROM `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024`;
-- Expected answer: 20,332,093

-- ============================================================================
-- QUESTION 2: Data read estimation
-- Count distinct PULocationIDs on both tables
-- Check estimated bytes in BigQuery console BEFORE running
-- ============================================================================
-- External table query:
SELECT COUNT(DISTINCT PULocationID)
FROM `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024_external`;

-- Materialized table query:
SELECT COUNT(DISTINCT PULocationID)
FROM `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024`;
-- Expected: 0 MB for External Table and 155.12 MB for Materialized Table

-- ============================================================================
-- QUESTION 3: Understanding columnar storage
-- Retrieve PULocationID vs PULocationID + DOLocationID
-- ============================================================================
-- Query 1 column:
SELECT PULocationID
FROM `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024`;

-- Query 2 columns:
SELECT PULocationID, DOLocationID
FROM `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024`;
-- Answer: BigQuery is columnar, scans only requested columns

-- ============================================================================
-- QUESTION 4: Counting zero fare trips
-- How many records have a fare_amount of 0?
-- ============================================================================
SELECT COUNT(*) as zero_fare_trips
FROM `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024`
WHERE fare_amount = 0;
-- Expected: 128,210

-- ============================================================================
-- QUESTION 5: Partitioning and clustering (CREATE TABLE)
-- Best strategy: Partition by tpep_dropoff_datetime, Cluster on VendorID
-- ============================================================================
CREATE OR REPLACE TABLE `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS SELECT * FROM `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024`;

-- ============================================================================
-- QUESTION 6: Partition benefits
-- Query distinct VendorIDs between 2024-03-01 and 2024-03-15
-- ============================================================================
-- Non-partitioned table:
SELECT DISTINCT VendorID
FROM `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- Partitioned table:
SELECT DISTINCT VendorID
FROM `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- Expected: ~310.24 MB for non-partitioned, ~26.84 MB for partitioned

-- ============================================================================
-- QUESTION 9: Understanding table scans
-- SELECT count(*) bytes estimation
-- ============================================================================
SELECT COUNT(*)
FROM `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024`;
-- Expected: 0 bytes - BigQuery uses metadata for count(*)
