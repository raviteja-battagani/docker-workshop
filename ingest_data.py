#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
from sqlalchemy import create_engine
import click
from tqdm import tqdm

@click.command()
@click.option('--pg_user', required=True, help='Username for PostgreSQL')
@click.option('--pg_password', required=True, help='Password for PostgreSQL')
@click.option('--pg_host', required=True, help='Host for PostgreSQL')
@click.option('--pg_port', required=True, type=int, help='Port for PostgreSQL')
@click.option('--pg_db', required=True, help='Database name for PostgreSQL')
@click.option('--target_table', required=True, help='Name of the target table to write results to')
@click.option('--year', required=True, type=int, help='Year of the taxi dataset to download')
@click.option('--month', required=True, type=int, help='Month of the taxi dataset to download')
@click.option('--chunk_size', default=100000, type=int, help='Chunk size for batching data into PostgreSQL')
def run(pg_user, pg_password, pg_host, pg_port, pg_db, target_table, year, month, chunk_size):
    
    # 1. Construct the URL for the New York Yellow Taxi dataset
    # Standard format padding month to two digits (e.g., 01, 12)
    month_str = f"{month:02d}"
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    file_name = f"yellow_tripdata_{year}-{month_str}.csv.gz"
    url = f"{prefix}/{file_name}"
    
    print(f"Starting ingestion workflow for: {file_name}")
    print(f"Source URL: {url}")
    
    # 2. Define explicit schema types to resolve pandas type-inference warnings
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float
    }
    
    parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

    # 3. Create Connection Engine to PostgreSQL
    connection_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(connection_string)

    # 4. Read data using an iterator to avoid overwhelming memory
    print("Connecting to source and initializing text stream iterator...")
    df_iter = pd.read_csv(
        url, 
        compression='gzip', 
        dtype=taxi_dtypes, 
        parse_dates=parse_dates, 
        chunksize=chunk_size
    )

    # 5. Drop old table and establish schema layout using the 1st chunk structure
    # (Fetches the structure without immediately pouring all data using 'if_exists=replace')
    first_chunk = next(df_iter)
    
    print(f"Dropping target table '{target_table}' if it exists and writing schema layout...")
    first_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace', index=False)
    
    # Insert the actual contents of the first chunk
    first_chunk.to_sql(name=target_table, con=engine, if_exists='append', index=False)
    print(f"Successfully ingested initial batch of {len(first_chunk)} rows.")

    # 6. Iterate through the remaining chunks tracked by a progress bar (tqdm)
    print("Processing subsequent batches...")
    for df in tqdm(df_iter, desc="Ingesting data chunks"):
        df.to_sql(name=target_table, con=engine, if_exists='append', index=False)
        
    print(f"All data from {file_name} successfully written to table '{target_table}'!")

if __name__ == '__main__':
    run()