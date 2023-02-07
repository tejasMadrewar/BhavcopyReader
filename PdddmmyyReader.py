import datetime
from datetime import timedelta
import calendar
import zipfile
import sqlite3 as db
import os.path
from pathlib import Path
import timeit

import pandas as pd
from tqdm import tqdm

import config as cfg

# zip files with errors
# 2018-01-02 same data as 2019-01-02
BLACK_LIST = [datetime.date(2018, 1, 2)]


def clean_df(date, df):
    # remove cell with " " in MKT column
    df = df[~df["MKT"].eq(" ")]
    # remove Unnamed columns
    df = df.loc[:, ~df.columns.str.match('Unnamed')]
    df = df.astype({"PREV_CL_PR": "float32", "OPEN_PRICE": "float32",
                   "HIGH_PRICE": "float32", "LOW_PRICE": "float32", "CLOSE_PRICE": "float32"})
    df = df.astype({"NET_TRDVAL": "float", "NET_TRDQTY": "float"})
    df = df.astype(
        {"TRADES": "float", "HI_52_WK": "float32", "LO_52_WK": "float32"})
    # make DATE1 column
    df.insert(0, "DATE1", value=date)
    df = df.astype({"DATE1": "datetime64[ns]"})
    df = df.rename(columns={"SECURITY": "SECURITY1"})
    # make all column headers lowercase
    df.columns = map(str.lower, df.columns)
    # make columns as categories
    df = df.astype({"security1": "string"})
    df = df.astype(
        {"mkt": "string", "series": "string", "symbol": "string"})
    df = df.astype({"ind_sec": "string", "corp_ind": "string"})
    return df


def zipfile_to_pd_df(date, zipfile_obj):
    """
    create the pandas dataframe from pd file in PR bhavcopy zip
    Pdddmmyy.csv file also contains Symbol and Series codes for
    each Security in addition to the information contained in
    the prddmmyy.csv file.
    """
    Pd_file = date.strftime("Pd%d%m%y.csv")
    Pd_file = list(filter(lambda x: x.endswith(
        Pd_file), zipfile_obj.namelist()))[0]
    df = pd.read_csv(zipfile_obj.open(Pd_file), encoding="ISO-8859-1")
    return df


def date_to_zipfile(date, folder):
    PRZipFileName = date.strftime("PR%d%m%y.zip")
    zipFilePath = os.path.join(folder, PRZipFileName)
    if not Path(zipFilePath).is_file():
        return
    z = zipfile.ZipFile(zipFilePath)
    return z


def day_to_df(day_date, folder):
    z = date_to_zipfile(day_date, folder)
    if z == None or day_date in BLACK_LIST:
        return pd.DataFrame()
    df = zipfile_to_pd_df(day_date, z)
    df = clean_df(day_date, df)
    return df


def days_to_df(days, folder):
    df = pd.concat([day_to_df(i, folder)
                   for i in tqdm(days)], ignore_index=True)
    # make columns as categories
    df = df.astype({"security1": "category"})
    df = df.astype(
        {"mkt": "category", "series": "category", "symbol": "category"})
    df = df.astype({"ind_sec": "category", "corp_ind": "category"})
    return df


def get_data_for_last_n_days(n, folder):
    d = datetime.datetime.now().date()
    days = [d - timedelta(days=i) for i in range(0, n)]
    return days_to_df(days, folder)


def get_data_for_year(year, folder):
    start = datetime.date(year, 1, 1)
    days = [start + timedelta(days=i)
            for i in range(0, 365 + calendar.isleap(start.year))]
    return days_to_df(days, folder)


def df_to_db(df: pd.DataFrame, engine, table_name="raw_data"):
    if engine.has_table(table_name):
        # remove duplicate data
        prev_dts = pd.read_sql_query(
            f"SELECT DISTINCT(date1) from {table_name}", engine)
        prev_dts["status"] = True
        df = pd.merge(df, prev_dts, on="date1", how="left")
        df = df[df.status.isna()].drop(["status"], axis=1)
    if df.empty:
        print("No new data")
    else:
        df.to_sql(table_name, engine, if_exists="append",
                  index=False, chunksize=100000)
        print(f"Added {len(df.date1.unique())} new days [{table_name}]")


def get_equity_data(symbol_name: str, conn):
    condition = f'"SYMBOL"= \'{symbol_name}\' and ("SERIES" in (\'EQ\',\'BE\'))'
    df = pd.read_sql_query(
        f'select * from raw_data where {condition} order by "DATE1" desc', conn)
    return df


def update_table(folder, engine, table_name="raw_data"):
    last_updated = datetime.date(year=2009, month=1, day=1)
    if engine.has_table(table_name):
        last_dt = pd.read_sql_query(
            f"SELECT MAX(date1) from {table_name}", engine)
        if not (last_dt.iloc[0, 0] == None):
            last_updated = last_dt.iloc[0, 0].to_pydatetime().date()
            # last_updated = datetime.date(year=2012, month=1, day=1)
    print(f"last updated {last_updated}")
    tday = datetime.datetime.today().date()
    days = [last_updated + timedelta(days=i)
            for i in range((tday-last_updated).days+1)]
    df = days_to_df(days, folder)
    df_to_db(df, engine)


def update():
    update_table(cfg.DOWNLOAD_FOLDER, cfg.SQL_CON)


def main():
    # print(timeit.timeit('get_equity_data("TCS",cfg.SQL_CON)',
    # globals=globals(), number=10))
    # df = get_equity_data("TCS", cfg.SQL_CON)
    update()


if __name__ == "__main__":
    main()
