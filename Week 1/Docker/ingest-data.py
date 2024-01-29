import os
import argparse
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'output.parquet'

    os.system(f"wget {url} -O {parquet_name}")

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_parquet(parquet_name)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.to_sql(name=table_name, con=engine, if_exists='replace',
              chunksize=100000, method='multi', index=False)

    df = pd.read_csv(
        f"https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv")
    df.to_sql(name='zones', con=engine, if_exists='replace',
              method='multi', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument(
        '--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the file to ingest')

    args = parser.parse_args()

    main(args)
