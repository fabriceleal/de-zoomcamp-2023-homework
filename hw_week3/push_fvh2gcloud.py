from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta
import os


@task()
def upload_to_gcs(from_path, to_path):
    """ upload local parquet file to gcs """
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=from_path,
        to_path=to_path)

@task
def download_files():
    for x in range(12):
        month = x + 1
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02}.csv.gz"
        print(f"downloading {url}")
        os.system(f"wget {url}")
    print("done downloading")

@flow()
def fhv_to_gcs():    
    download_files()

    for file in os.listdir():        
        if os.path.isfile(file) and file.endswith(".csv.gz"):
            print(f'uploading file {file}')
            upload_to_gcs(file, f"fvh2019/{file}")



if __name__ == "__main__":
    fhv_to_gcs()
