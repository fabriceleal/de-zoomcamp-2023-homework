from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta
from prefect_gcp import GcpCredentials

@task
def extract_from_gcs(color : str, year: int, month : int) -> Path:
    """ download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """ data cleaning example"""
    df = pd.read_parquet(path)

    print(f"pre: missing passanger count: {df['passenger_count'].isna().sum()}")
    #df['passenger_count'].fillna(0, inplace=True)
    #print(f"post: missing passanger count: {df['passenger_count'].isna().sum()}")

    return df

@task()
def write_bq(df:pd.DataFrame, color:str) -> None:
    """ write dataframe to bigquery"""
    from prefect_gcp import GcpCredentials
    gcp_credentials_block = GcpCredentials.load("gcpcred")

    df.to_gbq(destination_table=f"de_zoomcamp.rides_{color}", 
                project_id="raptor-land", 
                credentials=gcp_credentials_block.get_credentials_from_service_account(),
                chunksize=500_000,
                if_exists="append")

@flow(log_prints=True)
def etl_gcs_to_bq_par(color:str, year:int, month:int):
    """main etl flow to load data into big query"""
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    print(f"rows: {len(df)}")
    write_bq(df, color)

@flow(log_prints=True)
def etl_gcs_to_bq():
    """main etl flow to load data into big query"""
    color = "green"
    year = 2020
    month = 1

    etl_gcs_to_bq_par(color, year, month)

if __name__ == "__main__":
    etl_gcs_to_bq()


