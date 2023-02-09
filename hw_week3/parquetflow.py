import os
from urllib.parse import urlparse
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse
from datetime import timedelta
import numpy as np

@task(log_prints=True)
def fetch_file(url):
    
    parts = urlparse(url)
    p = Path(parts.path)
    f = p.name
    to_file = f"{os.getcwd()}/{f}"
    if not os.path.isfile(to_file):
        print(f"downloading {url}")
        os.system(f"wget {url} -O {to_file}")
    return to_file

def fetch_files(urls):
    files = []
    for url in urls:                        
        files.append(fetch_file(url))

    print("done downloading")
    print(files)
    return files

@task(log_prints=True)
def convert_file_to_parquet(f):
    print(f"converting file {f}")
    dtypes = {
        'dispatching_base_num'   : str,
        'PUlocationID'           : pd.Int64Dtype(),
        'DOlocationID'           : pd.Int64Dtype(),
        'SR_Flag'                : pd.Int64Dtype(),
        'Affiliated_base_number' : str
    }

    dates = ['pickup_datetime', 'dropOff_datetime']

    df = pd.read_csv(f, dtype=dtypes, parse_dates=dates)
    
    to = f.replace(".csv.gz", ".parquet")
    
    df.to_parquet(to)
    return to

def convert_to_parquet(files):
    parquets = []
    for f in files:        
        parquets.append(convert_file_to_parquet(f))
    print("done converting")
    print(parquets)
    return parquets


@task(log_prints=True)
def upload_file_to_gcs(from_path):
    gcs_block = GcsBucket.load("zoom-gcs")
    fileobj = Path(from_path)
    to_path = f"fhv_2019_parquets/{fileobj.name}"
    gcs_block.upload_from_path(
        from_path=from_path,
        to_path=to_path)

def upload_to_gcs(files):    
    for f in files:
        upload_file_to_gcs(f)
    
@task(log_prints=True)
def create_bg_tables():    
    bqblock = BigQueryWarehouse.load("zoom-gbq")

    print('creating external table ...')
    external = '''
    CREATE OR REPLACE EXTERNAL TABLE `raptor-land.de_zoomcamp.extparquet_fvh_data`
    OPTIONS (
        format= 'PARQUET',
        uris = ['gs://dtc_data_lake_raptor-land/fhv_2019_parquets/fhv_tripdata_2019-*.parquet']
        
    );'''

    bqblock.execute(external)
    print('created external table')
    print('creating materialized table')

    materialized = '''
    CREATE OR REPLACE TABLE `raptor-land.de_zoomcamp.parquet_fvh_data`
    as
    select * from raptor-land.de_zoomcamp.extparquet_fvh_data;
    '''

    bqblock.execute(materialized)

    print('created materialized table')


@flow
def web_to_parquet_to_bg():
    urls = [f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month+1:02}.csv.gz" for month in range(12)]
    files = fetch_files(urls)
    files = convert_to_parquet(files)
    upload_to_gcs(files)
    create_bg_tables()


if __name__ == "__main__":
    web_to_parquet_to_bg()