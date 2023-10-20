from datetime import datetime, timedelta
import time

import requests
import pandas as pd

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

    def json_response_to_df(self, json_resp):
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

    def get_index_data(self, indexName: str, fromDate: datetime, toDate: datetime):
        index = requests.utils.quote(indexName)
        url = f"https://www.nseindia.com/api/historical/indicesHistory?indexType={index}&from={fromDate.strftime('%d-%m-%Y')}&to={toDate.strftime('%d-%m-%Y')}"
        print(url)
        j = self.url2json(url)
        df = self.json_response_to_df(j)
        return df

    def test(self):
        toDate = datetime.now()
        toDate = datetime(2020, 1, 1)
        fromDate = toDate - timedelta(days=300)
        index = "NIFTY 50"
        # index = "NIFTY200 MOMENTUM 30"
        # print(self.get_all_index_names())
        df = self.get_index_data(index, fromDate, toDate)
        print(df)


def update():
    d = NseIndexNavDownloader()
    d.test()


def main():
    update()


if __name__ == "__main__":
    main()
