import pandas as pd
import sqlite3 as db
import datetime
from datetime import timedelta

import PdddmmyyReader as pdReader
import PRZipDownloader as Prdownloader


def create_test_db(table_name, con):
    start_day = datetime.datetime.today().date()
    skip_last_days = 10

    days = [start_day - timedelta(i)
            for i in range(skip_last_days, skip_last_days+20)]
    df = pdReader.days_to_df(days)
    df.to_sql(table_name, con, if_exists="replace",
              chunksize=1000, index=False)


def get_last_update_date(con, table_name):
    cur = con.cursor()
    cur.execute(
        f'select MAX(DATE1) "[timestamp]" from {table_name}')
    return cur.fetchall()[0][0]
    # df = pd.read_sql(f"select MAX(DATE1) as last_date from {table_name}", con)
    # return df.iloc[0, 0]


def get_new_data(con, table_name):
    last_update_date = get_last_update_date(con, table_name)
    print(f"last updated date: {last_update_date}")
    today = datetime.datetime.today().date()
    days = [last_update_date + timedelta(i)
            for i in range((today-last_update_date.date()).days)]
    print(days)
    # download PR zip files
    Prdownloader.PRZip_download_for_days(days, pdReader.BHAV_CPY_FLDER_PTH)
    df = pdReader.days_to_df(days)
    df = df[df.DATE1 > last_update_date]
    return df


def update_db(db_name, table_name="raw_data"):
    db_name = "test.db"
    table_name = "data"
    con = db.connect(
        db_name, detect_types=db.PARSE_DECLTYPES | db.PARSE_COLNAMES)
    # create_test_db(table_name, con)
    # update test db
    df = get_new_data(con, table_name)
    if df.empty:
        print("No update")
    else:
        print(df)
        df.to_sql(table_name, con, if_exists="append",
                  chunksize=1000, index=False)


def main():
    update_db("test.db", "data")


if __name__ == "__main__":
    main()
