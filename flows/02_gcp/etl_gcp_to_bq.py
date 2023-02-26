from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials



@task(retries=3)
def extract_from_gcs(color:str,year:int,month:int)->Path:
    gcs_path=f'Data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block=GcsBucket.load("zoom-gcs")
    gcs_block.download_object_to_path(from_path=gcs_path,to_path=f"Data/{color}/{color}_tripdata_{year}-{month:02}.parquet")
    return Path(f"{gcs_path}")


@task()
def transform(path:Path)-> pd.DataFrame:
    print(path)
    df=pd.read_parquet(path)
    print(f"pre:missing passenger count:{df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0,inplace=True)
    print(f"post:missing passenger count:{df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df:pd.DataFrame)-> None:
    '''Write dataFrame to BigQuery'''
    gcs_credentials_block=GcpCredentials.load('zoom-gcs-creds')

    df.to_gbq(destination_table="dezoomcamp.rides",
              project_id="orbital-concord-378412",
              credentials=gcs_credentials_block.get_credentials_from_service_account(),
              chunksize=500000,
              if_exists="append")

@flow()
def etl_gcp_to_bq():
    color='yellow'
    month=1
    year=2021
    path=extract_from_gcs(color,year,month)
    df=transform(path)
    write_bq(df)


if __name__=='__main__':
    etl_gcp_to_bq()