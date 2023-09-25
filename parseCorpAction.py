import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import os
from pprint import pprint
from multiprocessing import Pool

from config import SQL_CON, DOWNLOAD_FOLDER

import sqlalchemy as db
from sqlalchemy.orm.session import Session
from sqlalchemy.orm import declarative_base

CORP_ENGINE = db.create_engine("sqlite:///corp_action.db")

# bse corp action sqlalchamy model
Base = declarative_base()


class Symbol(Base):
    __tablename__ = "symbol"
    id = db.Column(
        db.SmallInteger().with_variant(db.Integer, "sqlite"), primary_key=True
    )
    symbol_name = db.Column(db.String(10), nullable=False)
    db.UniqueConstraint(symbol_name)

    def __repr__(self) -> str:
        return f"<Symbol({self.id}, {self.symbol_name})>"


class Csv_format:
    id = db.Column(db.Integer, primary_key=True)
    scrip_code = db.Column(db.Integer())
    short_name = db.Column(db.String(10))
    Ex_date = db.Column(db.DateTime())
    Purpose = db.Column(db.String(100))
    RD_Date = db.Column(db.DateTime())
    BCRD_FROM = db.Column(db.DateTime())
    BCRD_TO = db.Column(db.DateTime())
    ND_START_DATE = db.Column(db.DateTime())
    ND_END_DATE = db.Column(db.DateTime())
    payment_date = db.Column(db.DateTime())
    exdate = db.Column(db.Integer())
    long_name = db.Column(db.String(100))


class Split(Base, Csv_format):
    __tablename__ = "split"


class Bonus(Base, Csv_format):
    __tablename__ = "bonus"


class Dividend(Base, Csv_format):
    __tablename__ = "dividend"


class BseCorpActDownloader:
    def __init__(self) -> None:
        self.baseUrl = "https://www.bseindia.com"
        self.industryUrl = (
            "https://api.bseindia.com/BseIndiaAPI/api/ddlIndustry/w?flag=0"
        )
        self.purposeUrl = "https://api.bseindia.com/BseIndiaAPI/api/ddlPurpose/w?flag=0"
        self.timeout = 30
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Origin": "https://www.bseindia.com",
            "Connection": "keep-alive",
            "Referer": "https://www.bseindia.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "TE": "trailers",
        }

    def get_data(self, url, baseUrl):
        for _ in range(5):
            try:
                session = requests.Session()
                request = session.get(
                    baseUrl, headers=self.headers, timeout=self.timeout
                )
                cookies = dict(request.cookies)
                response = session.get(
                    url, headers=self.headers, timeout=self.timeout, cookies=cookies
                )
                return response.text
            except:
                time.sleep(2)

    def pool_handler(self, func, args, poolSize=3):
        p = Pool(poolSize)
        p.starmap(func, args)

    def jsonurl2df(self, url: str):
        text = self.get_data(url, self.baseUrl)
        j = json.loads(text)
        return pd.DataFrame(j)

    def download_purpose_code(self, dwnFolder=""):
        filename = os.path.join(dwnFolder, "purpose_code.csv")
        if os.path.isfile(filename):
            print(f"{filename} is already downloaded.")
            return pd.read_csv(filename)
        df = self.jsonurl2df(self.purposeUrl)
        df.to_csv(
            filename,
            index=False,
        )
        print("Finished downloading purpose code.")
        return df

    def download_industry_code(self, dwnFolder=""):
        filename = os.path.join(dwnFolder, "industry_code.csv")
        if os.path.isfile(filename):
            print(f"{filename} is already downloaded.")
            return pd.read_csv(filename)
        df = self.jsonurl2df(self.industryUrl)
        df.to_csv(
            filename,
            index=False,
        )
        print("Finished downloading industry code.")
        return df

    def download_corpAction(self, pur_code, pur_name, dwnFolder=""):
        filename = os.path.join(dwnFolder, f'{pur_name.replace(" ","_")}.csv')
        if os.path.isfile(filename):
            print(f"{filename} is already downloaded.")
            return
        url = f"https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w?Purposecode={pur_code}&ddlcategorys=E&ddlindustrys=&scripcode=&segment=0&strSearch=S"
        df = self.jsonurl2df(url)
        df.to_csv(
            filename,
            index=False,
        )
        print(f"Finished downloading {pur_name}")

    def download_allCorpAction(self, dwnFolder=""):
        pur_data = self.download_purpose_code(dwnFolder)
        self.download_industry_code(dwnFolder)
        args = (
            pur_data[["PURPOSE_CODE", "PURPOSE_NAME"]].to_records(index=False).tolist()
        )
        args = [i + (dwnFolder,) for i in args]
        self.pool_handler(self.download_corpAction, args, 4)


class BseCorpActDBManager:
    def __init__(self, engine=CORP_ENGINE) -> None:
        self.engine = engine
        self.Session = db.orm.sessionmaker(bind=self.engine)
        self.session: Session = self.Session()
        self.create_all()

    def get_new_rows(
        self,
        model,
        col_names,
        new_data: pd.DataFrame,
    ):
        if type(col_names) != list:
            col_names = [col_names]
        new_data = pd.DataFrame(new_data[col_names], columns=col_names)
        stmt = self.session.query(model).statement
        old_data = pd.read_sql_query(stmt, con=self.session.get_bind())
        diff = pd.merge(new_data, old_data, how="left", on=col_names)
        # diff = pd.DataFrame(diff[diff.isna().any(axis=1)][col_name])
        diff = pd.DataFrame(diff[diff["id"].isna()][col_names])
        return diff

    def clean_corp_df(self, df: pd.DataFrame, model):
        date_cols = [
            "Ex_date",
            "RD_Date",
            "BCRD_FROM",
            "BCRD_TO",
            "ND_START_DATE",
            "ND_END_DATE",
            "payment_date",
        ]
        for d in date_cols:
            df[d] = pd.to_datetime(df[d], errors="coerce")
        # df.to_csv("test.csv", index=False)
        # df = df.tail(100)

        df.replace(
            {
                pd.NaT: None,
            },
            inplace=True,
        )
        # read old data
        stmt = self.session.query(model).statement
        old_data = pd.read_sql_query(stmt, con=self.session.get_bind())
        old_data.drop("id", axis=1, inplace=True)
        # remove duplicates
        diff = pd.concat([df, old_data]).drop_duplicates(keep=False)
        return diff

    def csv_to_table(self, csv_path: str, model):
        df = pd.read_csv(csv_path)
        diff = self.clean_corp_df(df, model)
        print(f"Inserted {len(diff)} rows in {model.__tablename__} table")
        self.session.bulk_insert_mappings(model, diff.to_dict(orient="records"))
        self.session.commit()

    def update_corp_split_table(self, csv_path: str):
        self.csv_to_table(csv_path, Split)

    def update_corp_bonus_table(self, csv_path: str):
        self.csv_to_table(csv_path, Bonus)

    def update_corp_divident_table(self, csv_path: str):
        self.csv_to_table(csv_path, Dividend)

    def test_insert(self):
        print("testing ...")
        new_data = pd.DataFrame(
            [
                "test8",
                "test6",
                "test7",
                "test9",
            ],
            columns=["symbol_name"],
        )
        diff = self.get_new_rows(Symbol, "symbol_name", new_data)
        print(diff.to_dict(orient="records"))
        self.session.bulk_insert_mappings(Symbol, diff.to_dict(orient="records"))
        self.session.commit()

    def update(self):
        self.update_corp_split_table(
            r"C:\Users\TEJAS\Downloads\PR00_bhavcopy_data_all\corp_data\2023-09-24\Stock__Split.csv"
        )
        self.update_corp_bonus_table(
            r"C:\Users\TEJAS\Downloads\PR00_bhavcopy_data_all\corp_data\2023-09-24\Bonus_Issue.csv"
        )
        self.update_corp_divident_table(
            r"C:\Users\TEJAS\Downloads\PR00_bhavcopy_data_all\corp_data\2023-09-24\Dividend.csv"
        )

    def get_bonus_actions(self, ticker: str):
        stmt = self.session.query(Bonus).filter(Bonus.short_name == ticker).statement
        bonus = pd.read_sql_query(stmt, self.session.get_bind())
        return bonus

    def get_split_actions(self, ticker: str):
        stmt = self.session.query(Split).filter(Split.short_name == ticker).statement
        split = pd.read_sql_query(stmt, self.session.get_bind())
        return split

    def get_dividend_actions(self, ticker: str):
        stmt = (
            self.session.query(Dividend).filter(Dividend.short_name == ticker).statement
        )
        dividend = pd.read_sql_query(stmt, self.session.get_bind())
        return dividend

    def get_corp_actions(self, ticker: str):
        bonus = self.get_bonus_actions(ticker)
        split = self.get_split_actions(ticker)
        dividend = self.get_dividend_actions(ticker)
        df = pd.concat([bonus, split, dividend])
        print(df)
        return df

    def create_all(self):
        Base.metadata.create_all(self.engine)


def parseBonus(data: pd.DataFrame):
    print(data)
    bonus_regex1 = "^BONUS (\d):(\d).*"
    t = data["purpose"].str.extract(bonus_regex1)
    print(t)


def parseCorpAction(data: pd.DataFrame):
    parseBonus(data)


def update():
    downloader = BseCorpActDownloader()
    folder = os.path.join(
        DOWNLOAD_FOLDER, datetime.date.today().strftime("corp_data/%Y-%m-%d")
    )
    os.makedirs(folder, exist_ok=True)
    downloader.download_allCorpAction(folder)


def test():
    dbmgr = BseCorpActDBManager()
    dbmgr.update()
    df = dbmgr.get_corp_actions("TITAN")
    df.to_csv("titan.csv")


def main():
    # update()
    test()


if __name__ == "__main__":
    main()
