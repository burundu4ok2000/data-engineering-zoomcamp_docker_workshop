## First query

```sql
SELECT
	t.tpep_pickup_datetime,
	t.tpep_dropoff_datetime,
	t.total_amount,
	zpu."Zone" AS pickup_zone,
	zdo."Zone" AS dropoff_zone
FROM 
	yellow_taxi_data t
INNER JOIN zones zpu
	ON t."PULocationID" = zpu."LocationID"
INNER JOIN zones zdo
	ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;
```
