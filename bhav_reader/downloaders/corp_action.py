import sqlalchemy as db
import pandas as pd
import numpy as np
from pathlib import Path
import zipfile
import os
import datetime

from config import SQL_CON, DOWNLOAD_FOLDER
from .namechange_model import NameChangeManager
from model import Base, Symbol, Series, CorpAction

from tqdm import tqdm

# zip files with errors
BLACK_LIST = [
    datetime.date(2018, 1, 2),  # 2018-01-02 same data as 2019-01-02
    datetime.date(2023, 2, 20),  # same as 22-2-2023
]

CSV_COLUMNS = [
    "SERIES",
    "SYMBOL",
    "SECURITY",
    "RECORD_DT",
    "BC_STRT_DT",
    "BC_END_DT",
    "EX_DT",
    "ND_STRT_DT",
    "ND_END_DT",
    "PURPOSE",
]


def df_to_model(df: pd.DataFrame, session):
    if df.empty:
        return
    tables = [
        {"model": Symbol, "src_col": "symbol", "dst_col": "symbol_name"},
        {"model": Series, "src_col": "series", "dst_col": "series_name"},
    ]
    df["series"] = df["series"].fillna("_")
    df["symbol"] = df["symbol"].fillna("_")
    # remove already present dates
    query = session.query(CorpAction.date1).distinct().statement
    old_dates = pd.read_sql_query(query, con=session.get_bind())
    df = df[~df["date1"].isin(old_dates["date1"])]
    # update tables
    for t in tables:
        src_col = t["src_col"]
        dst_col = t["dst_col"]
        m = t["model"]
        updated_data = pd.DataFrame(df[src_col].unique(), columns=[dst_col])
        query = session.query(m).statement
        old_data = pd.read_sql_query(query, con=session.get_bind())
        new_data = pd.merge(updated_data, old_data, on=dst_col, how="left")
        # new rows
        new_data = new_data[new_data.isna().any(axis=1)]
        print(f"    Found {len(new_data)} new value(s) in {src_col}.")
        new_data = new_data[[dst_col]]
        session.bulk_insert_mappings(m, new_data.to_dict(orient="records"))
        session.commit()
    # add new data
    for t in tables:
        src_col = t["src_col"]
        dst_col = t["dst_col"]
        m = t["model"]
        query = session.query(m).statement
        t1 = pd.read_sql_query(query, con=session.bind)
        t1 = t1.rename(columns={"id": f"{src_col}_id"})
        df = df.rename(columns={src_col: dst_col})
        df = pd.merge(df, t1, on=dst_col, how="left")
    old_dates = pd.read_sql_query(
        session.query(CorpAction.date1).distinct().statement,
        con=session.get_bind(),
    )
    if not old_dates.empty:
        # remove data with old dates and then insert
        old_dates["status"] = "old_date"
        df = pd.merge(df, old_dates, on="date1", how="left")
        df = df[df.status.isna()]
    df = df[
        [
            "date1",
            "symbol_id",
            "series_id",
            "record_dt",
            "bc_strt_dt",
            "ex_dt",
            "nd_strt_dt",
            "nd_end_dt",
            "purpose",
        ]
    ]
    print(f"Found {len(df.date1.unique())} new days")
    df = df.replace({np.nan: None})
    session.bulk_insert_mappings(CorpAction, df.to_dict(orient="records"))
    session.commit()
    # print(df)


def clean_df(df: pd.DataFrame, day):
    date_cols = [
        "record_dt",
        "bc_strt_dt",
        "bc_end_dt",
        "ex_dt",
        "nd_strt_dt",
        "nd_end_dt",
        "date1",
    ]
    # remove Unnamed columns
    df = df.loc[:, ~df.columns.str.match("Unnamed")]
    # make DATE1 column
    df.insert(0, "date1", value=day)
    # make all column names lower case
    df.columns = map(str.lower, df.columns)
    df = df.replace(to_replace=" ", value="").drop("security", axis=1)
    # changes column types
    df = df.astype({"purpose": "string", "symbol": "string", "series": "string"})
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], dayfirst=True)
    return df


def zipfile_to_bc_df(day, zip_file):
    """
    create dataframe from Bcddmmyy.csv file
    """
    bc_file = day.strftime("Bc%d%m%y.csv")
    result = list(filter(lambda x: x.endswith(bc_file), zip_file.namelist()))
    # check BC file is present in the zip
    if len(result) < 1:
        return pd.DataFrame()
    else:
        bc_file = result[0]
    # check for empty bhavcopy
    if zip_file.getinfo(bc_file).file_size == 0:
        return pd.DataFrame()
    bc_file = list(filter(lambda x: x.endswith(bc_file), zip_file.namelist()))[0]
    df = pd.read_csv(
        zip_file.open(bc_file), sep=",", encoding="ISO-8859-1", on_bad_lines="warn"
    )
    return clean_df(df, day)


def date_to_zipfile(date: datetime.date, folder: str):
    PRZipFileName = date.strftime("PR%d%m%y.zip")
    # print(PRZipFileName)
    zipFilePath = os.path.join(folder, PRZipFileName)
    if not Path(zipFilePath).is_file():
        return None
    z = zipfile.ZipFile(zipFilePath)
    return z


def day_to_df(day, folder: str):
    zip_file = date_to_zipfile(day, folder)
    if zip_file == None or day in BLACK_LIST:
        return pd.DataFrame()
    df = zipfile_to_bc_df(day, zip_file)
    return df


def days_to_df(days, folder: str):
    df = pd.concat([day_to_df(i, folder) for i in tqdm(days)], ignore_index=True)
    return df


def recreate_all_data(con, folder: str):
    start_date = datetime.date(2010, 1, 1)  # start date
    Session = db.orm.sessionmaker(bind=con)
    session = Session()
    # create corp action table
    Base.metadata.create_all(con)
    # delete corp action table
    session.query(CorpAction).delete()
    session.commit()
    # generate dates
    today = datetime.datetime.today().date()
    days = [
        today - datetime.timedelta(days=i)
        for i in range(int((today - start_date).days) + 2)
    ]
    df = days_to_df(days, folder)
    df_to_model(df, session)


def get_last_updated_date(session):
    query = session.query(db.func.max(CorpAction.date1))
    if query.all()[0][0] == None:
        return datetime.date(year=2009, month=1, day=1)
    return query.all()[0][0].date()


def update(n=None):
    print("Updating corp Actions")
    Session = db.orm.sessionmaker(bind=SQL_CON)
    session = Session()
    # create corp action table
    Base.metadata.create_all(SQL_CON)
    start_date = get_last_updated_date(session)
    today = datetime.datetime.today().date()
    if n == None:
        start_date = get_last_updated_date(session)
    else:
        start_date = today - datetime.timedelta(days=n)
    # generate dates
    days = [
        today - datetime.timedelta(days=i)
        for i in range(int((today - start_date).days) + 2)
    ]
    df = days_to_df(days, DOWNLOAD_FOLDER)
    df_to_model(df, session)


def get_corp_actions(symbol_name: str, session):
    nameChange = NameChangeManager(session.get_bind())
    ids = nameChange.get_ids_of_symbol(symbol_name)
    symbol_fliter = db.or_(CorpAction.symbol_id == i for i in ids)
    query = (
        session.query(
            CorpAction.id,
            CorpAction.date1,
            CorpAction.series_id,
            CorpAction.symbol_id,
            # CorpAction.record_dt,
            # CorpAction.bc_strt_dt,
            # CorpAction.bc_end_dt,
            CorpAction.ex_dt,
            # CorpAction.nd_strt_dt,
            # CorpAction.nd_end_dt,
            CorpAction.purpose,
        )
        .filter(symbol_fliter)
        .order_by(
            CorpAction.ex_dt,
            CorpAction.purpose,
            CorpAction.date1,
        )
        .distinct(CorpAction.ex_dt, CorpAction.purpose)
    )

    # print(query.statement)
    df = pd.read_sql_query(query.statement, session.get_bind())
    # print(df)
    # df.to_excel("corpAction.xlsx")
    return df


def main():
    update(30)


if __name__ == "__main__":
    main()
