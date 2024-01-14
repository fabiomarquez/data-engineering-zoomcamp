# Script that downloads a file from a URL and save it as table
from time import time
import os
import pandas as pd
from sqlalchemy import create_engine
import argparse

def cast_time(df, columns):
    '''
    Receives a list of columns and cast every one of them as datetime
    '''
    for column in columns:
        df[column] = pd.to_datetime(df[column])
    return

def main(params):

    USER = params.user
    PASSWORD = params.password
    SERVER = params.server
    PORT = params.port
    DB = params.database
    TABLE = params.table_name
    URL = params.url

    gz_file_name = 'output.csv.gz'

    os.system(f"wget {URL} -O {gz_file_name}; gzip -d {gz_file_name}")

    engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DB}')

    df_iter = pd.read_csv('output.csv', iterator=True, chunksize=100000)
    
    # corrigir chamada
    df = df_iter.get_chunk()

    df.head(n=0).to_sql(name=TABLE, con=engine, if_exists='replace')

    c = 0
    for chunk in df_iter:
        t_start = time()
        cast_time(chunk, ['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
        chunk.to_sql(name=TABLE, con=engine, if_exists='append')
        t_end = time()
        c +=1
        print(f'Finished the {c}º chunk. It took {t_end - t_start}')
    

    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest data from url to database.')
    parser.add_argument('--user', help='User for postgres server')
    parser.add_argument('--password', help='Password for postgres server')
    parser.add_argument('--server', help='Postgres server')
    parser.add_argument('--port', help='Postgres server port')
    parser.add_argument('--database', help='Name of the database')
    parser.add_argument('--table_name', help='Name of table')
    parser.add_argument('--url', help='Source url for data')

    args = parser.parse_args()

    main(args)