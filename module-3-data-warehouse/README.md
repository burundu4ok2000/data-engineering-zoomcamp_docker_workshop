# Module 3 Homework: Data Warehousing & BigQuery

## Setup

### Data Source
- **Yellow Taxi Trip Records**: January - June 2024 (Parquet format)
- **GCS Bucket**: `gs://de-zoomcamp-2026-485615-yellow-taxi-2024/`

### BigQuery Tables
| Table | Type | Description |
|-------|------|-------------|
| `yellow_taxi_2024_external` | External | Points to GCS parquet files |
| `yellow_taxi_2024` | Materialized | Regular table (no partition/cluster) |
| `yellow_taxi_2024_partitioned` | Partitioned | Partitioned by DATE(tpep_dropoff_datetime), clustered by VendorID |

---

## Answers

### Question 1: Counting records
**What is count of records for the 2024 Yellow Taxi Data?**

```sql
SELECT COUNT(*) FROM `de-zoomcamp-2026-485615.zoomcamp.yellow_taxi_2024`;
```

**Answer: 20,332,093** ✓

---

### Question 2: Data read estimation
**Estimated data read for COUNT(DISTINCT PULocationID) on External vs Materialized table?**

```sql
-- External Table
SELECT COUNT(DISTINCT PULocationID) FROM `...yellow_taxi_2024_external`;

-- Materialized Table  
SELECT COUNT(DISTINCT PULocationID) FROM `...yellow_taxi_2024`;
```

**Answer: 0 MB for External Table and 155.12 MB for Materialized Table**

> External tables can't estimate bytes (data is in GCS), but materialized tables store metadata about column sizes.

---

### Question 3: Understanding columnar storage
**Why are estimated bytes different when querying 1 vs 2 columns?**

```sql
-- 1 column
SELECT PULocationID FROM `...yellow_taxi_2024`;

-- 2 columns
SELECT PULocationID, DOLocationID FROM `...yellow_taxi_2024`;
```

**Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.**

---

### Question 4: Counting zero fare trips
**How many records have a fare_amount of 0?**

```sql
SELECT COUNT(*) FROM `...yellow_taxi_2024` WHERE fare_amount = 0;
```

**Answer: 8,333** ✓

---

### Question 5: Partitioning and clustering
**Best strategy for filtering on tpep_dropoff_datetime and ordering by VendorID?**

```sql
CREATE OR REPLACE TABLE `...yellow_taxi_2024_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS SELECT * FROM `...yellow_taxi_2024`;
```

**Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID**

> - Partition = filter predicate (WHERE clause on date range)
> - Cluster = ORDER BY optimization

---

### Question 6: Partition benefits
**Estimated bytes for querying 2024-03-01 to 2024-03-15 on non-partitioned vs partitioned table?**

```sql
-- Non-partitioned
SELECT DISTINCT VendorID FROM `...yellow_taxi_2024`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- Partitioned
SELECT DISTINCT VendorID FROM `...yellow_taxi_2024_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```

**Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**

> Partitioned table only scans 15 day-partitions instead of entire table.

---

### Question 7: External table storage
**Where is the data stored in the External Table?**

**Answer: GCP Bucket**

> External tables only store metadata in BigQuery. Actual data remains in GCS.

---

### Question 8: Clustering best practices
**Is it best practice to always cluster your data?**

**Answer: False**

> Clustering adds overhead for:
> - Tables < 1GB (no benefit)
> - High-cardinality columns
> - Frequently updated tables
> - Queries that don't filter/order on cluster key

---

### Question 9: Understanding table scans
**How many bytes does SELECT COUNT(*) estimate?**

```sql
SELECT COUNT(*) FROM `...yellow_taxi_2024`;
```

**Answer: 0 bytes**

> BigQuery stores row count in table metadata. COUNT(*) doesn't need to scan any data.

---

## Files

| File | Description |
|------|-------------|
| [load_yellow_taxi_data.py](./load_yellow_taxi_data.py) | Python script to download and upload parquet files to GCS |
| [queries.sql](./queries.sql) | All BigQuery SQL queries for homework |
