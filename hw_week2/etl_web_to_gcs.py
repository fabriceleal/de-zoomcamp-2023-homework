from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta
import os

#  a flow that loads the green taxi CSV dataset 
# for January 2020 into GCS 

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url : str) -> pd.DataFrame:
    """ read taxi data from url into pandas dataframe """

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df : pd.DataFrame) -> pd.DataFrame:
    """ fix some issues """
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows:  {len(df)}")
    
    return df

@task(log_prints=True)
def write_local(df : pd.DataFrame, color: str, dataset_file : str) -> Path:
    """ write dataframe locally as parquet file"""
        
    dir = f"data/{color}"
    if not os.path.exists(dir):
        os.makedirs(dir)

    path = Path(f"{dir}/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path : Path) -> None:
    """ upload local parquet file to gcs """
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path)


@flow()
def etl_web_to_gcs_par(color, year, month) -> None:
    """ the main etl function (parameterized)"""

    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-01.csv.gz

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_web_to_gcs() -> None:
    """ the main etl function"""

    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-01.csv.gz

    color = "green"
    year = 2020
    month = 1
    etl_web_to_gcs_par(color, year, month)

if __name__ == "__main__":
    etl_web_to_gcs()


