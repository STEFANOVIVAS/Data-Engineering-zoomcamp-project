from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3)
def fetch(dataset_url:str)->pd.DataFrame:
    '''Read taxi data from web to a pabndas Dataframe'''
    # with gzip.open(dataset_url, 'rb') as dataset:
    # if randint(0,1)>0:
    #     raise Exception
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df:pd.DataFrame)->pd.DataFrame:
    '''Fix dTypes issues'''
    df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)
    print(df.head(2))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    return df

@task()
def write_local(df:pd.DataFrame,color:str,dataset_file:str)-> Path:
    '''Write locally parquet files'''
    path=Path(f'Data/{color}/{dataset_file}.parquet')
    df.to_parquet(path,compression='gzip')
    return path


@task()
def write_gcs(path:Path)->None:
    '''Upload local parquet file do GCS'''
    
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(path,path)
    return

@flow()
def etl_web_to_gcp()->None:
    '''The main ETL'''
    color='yellow'
    month=1
    year=2021
    dataset_file=f'{color}_tripdata_{year}-{month:02}'
    dataset_url=f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'
    df=fetch(dataset_url)
    df_clean=clean(df)
    path=write_local(df_clean,color,dataset_file)
    write_gcs(path)


if __name__=='__main__':
    etl_web_to_gcp()