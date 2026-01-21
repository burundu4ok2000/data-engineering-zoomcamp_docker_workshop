import pandas as pd
import click
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

@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='yellow_taxi_data', help='Target table name')

def ingest_data(user, password, host, port, db, table):

    year = 2021
    month = 1

    pg_user = user
    pg_password = password
    pg_host = host
    pg_port = port
    pg_db = db
    chunksize = 100000

    target_table = table

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
    ingest_data()