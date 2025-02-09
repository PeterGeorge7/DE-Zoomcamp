#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith(".csv.gz"):
        csv_name = "output.csv.gz"
    else:
        csv_name = "output.csv"

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists="append")

    print("inserted the first chunck ...")

    while True:
        t_start = time()

        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()

        print("inserted another chunck , took %.3f second" % (t_end - t_start))


parser = argparse.ArgumentParser(description="Ingest CSV Data to Postgres")

# user
# password
# host
# port
# database name
# table name
# url of the csv

if __name__ == "__main__":

    parser.add_argument("--user", help="user name for Postgres")
    parser.add_argument("--password", help="pass for Postgres")
    parser.add_argument("--host", help="host for Postgres")
    parser.add_argument("--port", help="port for Postgres")
    parser.add_argument("--db", help="database name for Postgres")
    parser.add_argument(
        "--table_name", help="name of the table where we will write the results to"
    )
    parser.add_argument("--url", help="url of the csv file ")

    args = parser.parse_args()

    main(args)
