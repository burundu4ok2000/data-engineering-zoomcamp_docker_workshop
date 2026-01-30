import pandas as pd
import click
from sqlalchemy import create_engine, inspect
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

    pg_user = user
    pg_password = password
    pg_host = host
    pg_port = port
    pg_db = db

    #Create connection to PostgreSQL
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')
    inspector = inspect(engine)
    
    # ---------------------------------------------------------
    # Part 1: Main Taxi Data Ingestion
    # ---------------------------------------------------------
    
    if inspector.has_table(table):
        print(f'Table {table} already exists. Exiting.')
    else:
        print(f'Table {table} does not exist. Proceeding with ingestion.')

        year = 2021
        month = 1
        chunksize = 100000
        prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
        url = f'yellow_tripdata_{year}-{month:02d}.csv.gz'

        df_iter = pd.read_csv(
            prefix + url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunksize,
        )
        first_chunk = next(df_iter)

        # Create table structure
        first_chunk.head(0).to_sql(
            name=table,
            con=engine,
            if_exists="replace",
        )
        print("Table structure created")

        first_chunk.to_sql(
            name=table,
            con=engine,
            if_exists="append",
        )
        print("Inserted first chunk:", len(first_chunk))

        for df_chunk in fignushka(df_iter):

            df_chunk.to_sql(
                name=table,
                con=engine,
                if_exists="append"
            )
        print("Data ingestion completed.")

    # ---------------------------------------------------------
    # Part 2: Zones Lookup Table
    # ---------------------------------------------------------
    zones_table = 'zones'
    if inspector.has_table(zones_table):
        print(f'Table {zones_table} already exists. Skipping zones ingestion.')
    else:
        print(f'Table {zones_table} does not exist. Proceeding with zones ingestion.')

        zones_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
        df_zones = pd.read_csv(zones_url)

        df_zones.head(0).to_sql(
            name=zones_table,
            con=engine,
            if_exists="replace",
        )
        print("Zones table structure created")

        df_zones.to_sql(
            name=zones_table,
            con=engine,
            if_exists="append",
        )
        print("Inserted zones data:", len(df_zones))
        print("Zones ingestion completed.")

if __name__ == '__main__':
    ingest_data()