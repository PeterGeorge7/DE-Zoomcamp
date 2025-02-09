import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os


def main(params):
    # create variables
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url

    if url.endswith(".csv.gz"):
        # read the csv file
        output = "output.csv.gz"
    else:
        output = "output.csv"

    # download the file
    os.system(f"wget {url} -O {output}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # read the csv file
    df_iter = pd.read_csv(output, iterator=True, chunksize=1000)

    df = next(df_iter)

    df.head(0).to_sql(table, con=engine, if_exists="replace", index=False)

    df.to_sql(table, con=engine, if_exists="append", index=False)

    while True:
        try:
            t0 = time()
            df = next(df_iter)
            df.to_sql(table, con=engine, if_exists="append", index=False)
            print(f"Time to insert next chunk: {time() - t0}")
            print(f"Rows inserted: {df.shape[0]}")
        except StopIteration:
            break


parser = argparse.ArgumentParser(description="Ingest CSV data into a database")
# user
# password
# host
# port
# database name
# table name
# url of the csv file

if __name__ == "__main__":
    parser.add_argument("--user", help="User name")
    parser.add_argument("--password", help="Password")
    parser.add_argument("--host", help="Host name")
    parser.add_argument("--port", help="Port number")
    parser.add_argument("--db", help="Database name")
    parser.add_argument("--table", help="Table name")
    parser.add_argument("--url", help="URL of the CSV file")

    args = parser.parse_args()

    main(args)
