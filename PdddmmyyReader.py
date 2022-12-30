import datetime
from datetime import timedelta
import calendar
import zipfile
import sqlite3 as db
import os.path
from pathlib import Path
import pandas as pd

import timeit

BHAV_CPY_FLDER_PTH = "C:\\Users\\TEJAS\\Downloads\\PR00_bhavcopy_data_all\\"

# zip files with errors
# 2018-01-02 same data as 2019-01-02
BLACK_LIST = [datetime.date(2018, 1, 2)]


def clean_df(date, df):
    df = df.astype(
        {"MKT": "string", "SERIES": "string", "SYMBOL": "string"})
    # remove cell with " " in MKT column
    df = df[~df["MKT"].eq(" ")]
    df = df.astype({"SECURITY": "string"})
    df = df.astype({"PREV_CL_PR": "float", "OPEN_PRICE": "float",
                   "HIGH_PRICE": "float", "LOW_PRICE": "float", "CLOSE_PRICE": "float"})
    df = df.astype({"NET_TRDVAL": "float", "NET_TRDQTY": "float"})
    df = df.astype({"IND_SEC": "string", "CORP_IND": "string"})
    df = df.astype(
        {"TRADES": "float", "HI_52_WK": "float", "LO_52_WK": "float"})
    # make DATE1 column
    df.insert(0, "DATE1", value=date)
    df = df.astype({"DATE1": "datetime64"})
    # remove Unnamed columns
    df = df.loc[:, ~df.columns.str.match('Unnamed')]
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


def date_to_zipfile(date):
    PRZipFileName = date.strftime("PR%d%m%y.zip")
    zipFilePath = os.path.join(BHAV_CPY_FLDER_PTH, PRZipFileName)
    if not Path(zipFilePath).is_file():
        return
    z = zipfile.ZipFile(zipFilePath)
    return z


def day_to_df(day_date):
    z = date_to_zipfile(day_date)
    if z == None or day_date in BLACK_LIST:
        # print(day_date, ": Not found")
        print("*", day_date)
        return pd.DataFrame()
    df = zipfile_to_pd_df(day_date, z)
    df = clean_df(day_date, df)
    # print(day_date, ": Found")
    print(".", day_date)
    #print(".", end="")
    return df


def days_to_df(days):
    return pd.concat([day_to_df(i) for i in days], ignore_index=True)


def get_data_for_last_n_days(n):
    d = datetime.datetime.now().date()
    days = [d - timedelta(days=i) for i in range(0, n)]
    return days_to_df(days)


def get_data_for_year(year):
    start = datetime.date(year, 1, 1)
    days = [start - timedelta(days=i)
            for i in range(0, 365 + calendar.isleap(start.year))]
    return days_to_df(days)


def df_to_sqlite(df, db_name: str):
    con = db.connect(db_name + ".db")
    print(f"\nWriting to {db_name}...", end="\r")
    df.to_sql(db_name, con, if_exists="replace", index=False)
    con.execute("VACUUM")
    con.close()
    print(f"\nWriting to {db_name} finished.")


def main():
    # print(timeit.timeit('get_data_for_last_n_days(100)',
    # globals=globals(), number=10))

    # df = get_data_for_year(2022)
    # df_to_sqlite(df, "2022")
    start = datetime.datetime.today().date()
    days = [start - timedelta(days=i)
            for i in range(0, 366*15)]
    df = days_to_df(days)
    df_to_sqlite(df, "data")


if __name__ == "__main__":
    main()
