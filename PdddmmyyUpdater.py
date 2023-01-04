import pandas as pd

import datetime
from datetime import timedelta
import sqlite3 as db
from sqlalchemy import create_engine

import config as cfg
import PdddmmyyReader as pdReader
import PRZipDownloader as Prdownloader


def create_test_db(table_name, con):
    start_day = datetime.datetime.today().date()
    skip_last_days = 10

    days = [start_day - timedelta(i)
            for i in range(skip_last_days, skip_last_days+20)]
    df = pdReader.days_to_df(days)
    df.to_sql(table_name, con, if_exists="replace",
              index=False)


def get_last_update_date(con, table_name):
    df = pd.read_sql(
        f'select MAX("DATE1") as last_date from {table_name}', con)
    return df.iloc[0, 0].to_pydatetime()


def get_new_data(con, table_name):
    last_update_date = get_last_update_date(con, table_name)
    print(f"Last updated date[{table_name}]: {last_update_date.date()}")
    today = datetime.datetime.today().date()
    days = [last_update_date + timedelta(i)
            for i in range((today-last_update_date.date()).days)]
    # download PR zip files
    Prdownloader.PRZip_download_for_days(days, cfg.DOWNLOAD_FOLDER)
    df = pdReader.days_to_df(days)
    df = df[df.DATE1 > last_update_date]
    return df


def update_db(con, table_name="raw_data"):
    # create_test_db(table_name, con)
    # update test db
    df = get_new_data(con, table_name)
    if df.empty:
        print(f"No update to database.[{table_name}]")
    else:
        new_days = df.DATE1.nunique()
        print(f"Updating for {new_days} days...")
        df.to_sql(table_name, con, if_exists="append",
                  chunksize=1000, index=False)
        print(f"Update completed.[{table_name}]")


def main():
    con = cfg.SQL_CON
    update_db(con)


if __name__ == "__main__":
    main()
