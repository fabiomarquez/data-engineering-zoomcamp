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

def extract_file(url):
    csv_file_name = 'output.csv'
    gz_file_name = csv_file_name + '.gz'
    if '.gz' in url:
        os.system(f"wget {url} -O {gz_file_name}; gzip -d {gz_file_name}")
    else:
        os.system(f"wget {url} -O {csv_file_name}")
    print(f'The file was extracted to {csv_file_name}')
    return csv_file_name

def main(params):

    USER = params.user
    PASSWORD = params.password
    SERVER = params.server
    PORT = params.port
    DB = params.database
    TABLE = params.table_name
    URL = params.url

    file_name = extract_file(URL)

    engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DB}')

    df_head = pd.read_csv(file_name, nrows=1000)

    df_head.head(n=0).to_sql(name=TABLE, con=engine, if_exists='replace')

    df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000, low_memory=False)

    c = 0
    for chunk in df_iter:
        t_start = time()
        if 'tpep_pickup_datetime' in chunk.columns:
            cast_time(chunk, ['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
        chunk.to_sql(name=TABLE, con=engine, if_exists='append')
        t_end = time()
        c +=1
        print(f'Finished the {c}º chunk. It took {(t_end - t_start):.2f} seconds')  

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