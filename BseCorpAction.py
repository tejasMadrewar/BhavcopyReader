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
    def __init__(self, downloadFolder) -> None:
        self.dTime = datetime.datetime.now()
        self.folder = os.path.join(
            downloadFolder, self.dTime.strftime(r"corp_data\%Y-%m-%d")
        )
        # self.folder = os.path.join(downloadFolder, "corp_data/2023-09-27")
        os.makedirs(self.folder, exist_ok=True)
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
        if text == None:
            return pd.DataFrame()
        j = json.loads(text)
        return pd.DataFrame(j)

    def download_purpose_code(self):
        filename = os.path.join(self.folder, "purpose_code.csv")
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

    def download_industry_code(self):
        filename = os.path.join(self.folder, "industry_code.csv")
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

    def download_corpAction(self, pur_code, pur_name):
        filename = os.path.join(self.folder, f'{pur_name.replace(" ","_")}.csv')
        if os.path.isfile(filename):
            f = filename.split("\\")[-1]
            print(f"{f} is already downloaded.")
            return
        url = f"https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w?Purposecode={pur_code}&ddlcategorys=E&ddlindustrys=&scripcode=&segment=0&strSearch=S"
        df = self.jsonurl2df(url)
        df.to_csv(
            filename,
            index=False,
        )
        print(f"Finished downloading {pur_name}")

    def download_allCorpAction(self):
        pur_data = self.download_purpose_code()
        ind_data = self.download_industry_code()
        self.download_industry_code()
        args = (
            pur_data[["PURPOSE_CODE", "PURPOSE_NAME"]].to_records(index=False).tolist()
        )
        args = [i for i in args]
        self.pool_handler(self.download_corpAction, args, 4)


class BseCorpActDBManager:
    def __init__(self, engine=CORP_ENGINE, dwnFolder=DOWNLOAD_FOLDER) -> None:
        self.dwnldr = BseCorpActDownloader(dwnFolder)
        self.engine = engine
        self.download_folder = dwnFolder
        self.Session = db.orm.sessionmaker(bind=self.engine)  # type: ignore
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

        # read old data
        stmt = self.session.query(model).statement
        old_data = pd.read_sql_query(stmt, con=self.session.get_bind())
        old_data.drop("id", axis=1, inplace=True)
        # remove duplicates
        diff = pd.concat([df, old_data]).drop_duplicates(keep=False)
        # replace NaT and nan
        diff.replace(
            {
                np.nan: None,
            },
            inplace=True,
        )
        # filter data by the ex_date
        diff = diff[diff["Ex_date"] < self.dwnldr.dTime]
        return diff

    def csv_to_table(self, csv_path: str, model):
        df = pd.read_csv(csv_path)
        diff = self.clean_corp_df(df, model)
        # print(diff)
        # diff.to_csv(f"diff_{model.__tablename__}.csv")
        self.session.bulk_insert_mappings(model, diff.to_dict(orient="records"))  # type: ignore
        self.session.commit()
        print(f"Inserted {len(diff)} rows in {model.__tablename__} table")

    def update_corp_split_table(self, csv_path: str):
        self.csv_to_table(csv_path, Split)

    def update_corp_bonus_table(self, csv_path: str):
        self.csv_to_table(csv_path, Bonus)

    def update_corp_divident_table(self, csv_path: str):
        self.csv_to_table(csv_path, Dividend)

    def update(self):
        self.dwnldr.download_allCorpAction()
        self.update_corp_split_table(f"{self.dwnldr.folder}\\Stock__Split.csv")
        self.update_corp_bonus_table(f"{self.dwnldr.folder}\\Bonus_Issue.csv")
        self.update_corp_divident_table(f"{self.dwnldr.folder}\\Dividend.csv")

    def read_actions_from_db(self, model, ticker: str):
        stmt = self.session.query(model).filter(model.short_name == ticker).statement
        actions = pd.read_sql_query(stmt, self.session.get_bind())
        return actions

    def get_bonus_actions(self, ticker: str):
        return self.read_actions_from_db(Bonus, ticker)

    def get_split_actions(self, ticker: str):
        return self.read_actions_from_db(Split, ticker)

    def get_dividend_actions(self, ticker: str):
        return self.read_actions_from_db(Dividend, ticker)

    def get_corp_actions(self, ticker: str):
        bonus = self.get_bonus_actions(ticker)
        split = self.get_split_actions(ticker)
        dividend = self.get_dividend_actions(ticker)
        # dividend = pd.DataFrame()
        df = pd.concat([bonus, split, dividend])
        df.reset_index(inplace=True)
        df.drop("index", axis=1, inplace=True)
        ad_fact = self.parse_adjustment_factor(df)
        df = df.join(ad_fact)
        # print(df)
        return df

    def parse_adjustment_factor(self, corpAct: pd.DataFrame):
        # bonus
        bonus_regex = r"Bonus issue (\d+):(\d+)"
        bonus = corpAct["Purpose"].str.extract(bonus_regex)
        bonus.columns = ["A", "B"]
        bonus["A"] = pd.to_numeric(bonus["A"])
        bonus["B"] = pd.to_numeric(bonus["B"])
        bonus["C"] = bonus.eval("(A + B)/B")
        bonus["action"] = "bonus"
        bonus.drop(bonus.index[bonus["C"].isna()], inplace=True)

        # split
        split_regex = r"Stock  Split From Rs.(\d+)/- to Rs.(\d+)/-"
        split = corpAct["Purpose"].str.extract(split_regex)
        split.columns = ["A", "B"]
        split["A"] = pd.to_numeric(split["A"])
        split["B"] = pd.to_numeric(split["B"])
        split["C"] = split.eval("A /B")
        split["action"] = "split"
        # split = split[~split["C"].isna()]
        split.drop(split.index[split["C"].isna()], inplace=True)

        # dividend
        divi_regex = r"Dividend - Rs. - (\d+(?:.\d+)?)"
        divi = corpAct["Purpose"].str.extract(divi_regex)
        divi.columns = ["C"]
        divi["action"] = "dividend"
        divi["A"] = np.nan
        divi["B"] = np.nan
        divi["C"] = pd.to_numeric(divi["C"])
        divi.drop(divi.index[divi["C"].isna()], inplace=True)

        actions = pd.concat([bonus, split, divi])
        return actions

    def create_all(self):
        Base.metadata.create_all(self.engine)


def update():
    print("Updating BSE corp actions..")
    downloader = BseCorpActDBManager()
    downloader.update()


def test():
    dbmgr = BseCorpActDBManager()
    dbmgr.update()
    df = dbmgr.get_corp_actions("INFY")
    print(df)


def main():
    update()
    # test()


if __name__ == "__main__":
    main()
