
import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse

def main(params):
    user=params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    table_name=params.table_name
    url=params.url
    csv_name='output.csv'
    
    os.system(f'wget {url} -O {csv_name}')
    



    engine=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter=pd.read_csv(csv_name, iterator=True,chunksize=100000,compression='gzip')



    df=next(df_iter)
    df.lpep_pickup_datetime=pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime=pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name,con=engine, if_exists='replace')

    df.to_sql(name=table_name,con=engine, if_exists='append')

    while True:

        try:

            t_start=time()
            
            df=next(df_iter)
            
            df.lpep_pickup_datetime=pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime=pd.to_datetime(df.lpep_dropoff_datetime)
            
            df.to_sql(name=table_name,con=engine, if_exists='append')
            t_end=time()
            print(f'Adding another chunk...took {t_end-t_start} seconds')

        except StopIteration:
            print("Finished ingestig data  into the postgres database")
            break



if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Ingest CVS data to Postgres')


    parser.add_argument('--user', help='user for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table  where we will write the results to')
    parser.add_argument('--url', help='url to CSV file')


    args = parser.parse_args()
    main(args)

