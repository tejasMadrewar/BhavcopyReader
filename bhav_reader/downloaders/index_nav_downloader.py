from datetime import datetime, timedelta
import time

import requests
import pandas as pd
import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "TE": "trailers",
}


INDEX_NAMES = [
    "NIFTY 50",
    "NIFTY NEXT 50",
    "NIFTY MIDCAP 50",
    "NIFTY MIDCAP 100",
    "NIFTY MIDCAP 150",
    "NIFTY SMALLCAP 50",
    "NIFTY SMALLCAP 100",
    "NIFTY SMALLCAP 250",
    "NIFTY MIDSMALLCAP 400",
    "NIFTY 100",
    "NIFTY 200",
    "NIFTY500 MULTICAP 50:25:25",
    "NIFTY LARGEMIDCAP 250",
    "NIFTY MIDCAP SELECT",
    "NIFTY TOTAL MARKET",
    "NIFTY MICROCAP 250",
    "NIFTY AUTO",
    "NIFTY BANK",
    "NIFTY ENERGY",
    "NIFTY FINANCIAL SERVICES",
    "NIFTY FINANCIAL SERVICES 25/50",
    "NIFTY FMCG",
    "NIFTY IT",
    "NIFTY MEDIA",
    "NIFTY METAL",
    "NIFTY PHARMA",
    "NIFTY PSU BANK",
    "NIFTY REALTY",
    "NIFTY PRIVATE BANK",
    "NIFTY HEALTHCARE INDEX",
    "NIFTY CONSUMER DURABLES",
    "NIFTY OIL & GAS",
    "NIFTY COMMODITIES",
    "NIFTY INDIA CONSUMPTION",
    "NIFTY CPSE",
    "NIFTY INFRASTRUCTURE",
    "NIFTY MNC",
    "NIFTY GROWTH SECTORS 15",
    "NIFTY PSE",
    "NIFTY SERVICES SECTOR",
    "NIFTY100 LIQUID 15",
    "NIFTY MIDCAP LIQUID 15",
    "NIFTY INDIA DIGITAL",
    "NIFTY100 ESG",
    "NIFTY INDIA MANUFACTURING",
    "NIFTY DIVIDEND OPPORTUNITIES 50",
    "NIFTY50 VALUE 20",
    "NIFTY100 QUALITY 30",
    "NIFTY50 EQUAL WEIGHT",
    "NIFTY100 EQUAL WEIGHT",
    "NIFTY100 LOW VOLATILITY 30",
    "NIFTY ALPHA 50",
    "NIFTY200 QUALITY 30",
    "NIFTY ALPHA LOW-VOLATILITY 30",
    "NIFTY200 MOMENTUM 30",
    "NIFTY MIDCAP150 QUALITY 50",
]

TIMEOUT = 30


class NseIndexNavDownloader:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.baseurl = "https://www.nseindia.com"
        self.getAllIndexes = "https://www.nseindia.com/api/equity-master"
        self.timeout = 10
        # set cookies
        self.session.get(self.baseurl, timeout=self.timeout)

    def url2json(self, url: str):
        for i in range(5):
            resp = self.session.get(url, timeout=self.timeout)
            print(resp.status_code)
            if resp.status_code == 200:
                j = resp.json()
                if j == None:
                    print("retrying.")
                    continue
                return resp.json()
            else:
                time.sleep(2)

    def get_all_index_names(self):
        index_names = []
        master_data = self.url2json(self.getAllIndexes)
        for i in master_data:
            if i == "Others":
                continue
            index_names = index_names + master_data[i]
        return index_names

    def json_response_to_df(self, json_resp: dict):
        if json_resp["data"]["indexCloseOnlineRecords"] == []:
            return pd.DataFrame()
        turnover = pd.DataFrame(json_resp["data"]["indexTurnoverRecords"])
        turnover.drop(["_id", "TIMESTAMP"], axis=1, inplace=True)
        turnover["HIT_TIMESTAMP"] = pd.to_datetime(
            turnover["HIT_TIMESTAMP"], format="%d-%m-%Y"
        )
        ohlc_json = json_resp["data"]["indexCloseOnlineRecords"]
        ohlc = pd.DataFrame(ohlc_json)
        ohlc.drop(["_id", "TIMESTAMP"], axis=1, inplace=True)
        ohlc["EOD_TIMESTAMP"] = pd.to_datetime(ohlc["EOD_TIMESTAMP"], format="%d-%b-%Y")
        all = ohlc.join(turnover.set_index("HIT_TIMESTAMP"), on="EOD_TIMESTAMP")
        all.drop("HIT_INDEX_NAME_UPPER", axis=1, inplace=True)
        return all

    def get_index_data(self, indexName, fromDate: datetime, toDate: datetime):
        df = pd.DataFrame()
        indexes = []
        if type(indexName) == str:
            indexes = [indexName]
        if type(indexName) == list:
            indexes = indexName

        for ind in indexes:
            index = requests.utils.quote(ind)
            url = f"https://www.nseindia.com/api/historical/indicesHistory?indexType={index}&from={fromDate.strftime('%d-%m-%Y')}&to={toDate.strftime('%d-%m-%Y')}"
            # print(url)
            j = self.url2json(url)
            data = self.json_response_to_df(j)
            df = pd.concat([df, data], ignore_index=True)
        df = df.sort_values(["EOD_INDEX_NAME", "EOD_TIMESTAMP"])
        df.columns = [i.replace("EOD_", "") for i in df.columns]
        df.columns = [i.replace("HIT_", "") for i in df.columns]
        df.columns = [i.lower() for i in df.columns]
        return df

    def test(self):
        toDate = datetime.now()
        # toDate = datetime(2020, 1, 1)
        fromDate = toDate - timedelta(days=10)
        index = "NIFTY 50"
        # index = "NIFTY200 MOMENTUM 30"
        index = ["NIFTY 50", "NIFTY NEXT 50"]
        # print(self.get_all_index_names())
        df = self.get_index_data(index, fromDate, toDate)
        print(df)
        df.to_csv("temp.csv")


class NseIndexDataManager:
    def __init__(self) -> None:
        pass


def update():
    d = NseIndexNavDownloader()
    d.test()


def main():
    update()


if __name__ == "__main__":
    main()
