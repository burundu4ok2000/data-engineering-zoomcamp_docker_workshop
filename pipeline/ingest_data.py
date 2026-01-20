import pandas as pd
from sqlalchemy import create_engine
from time import time
from tqdm.auto import tqdm as fignushka

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

def run():

    year = 2021
    month = 1

    pg_user = 'root'
    pg_password = 'root'
    pg_host = 'localhost'
    pg_port = '5432'
    pg_db = 'ny_taxi'

    chunksize = 100000

    target_table = 'yellow_taxi_data'

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'yellow_tripdata_{year}-{month:02d}.csv.gz'

    df_iter = pd.read_csv(
        prefix + url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    first_chunk = next(df_iter)

    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace",
    )

    print("Table created")

    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append",
    )
    print("Inserted first chunk:", len(first_chunk))

    for i, df_chunk in enumerate(fignushka(df_iter), start=2):

        t_start = time()

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )

        t_end = time()

        print(f'Chunk {i} inserted ({len(df_chunk)} rows), took {t_end - t_start:.3f} seconds')


if __name__ == '__main__':
    run()