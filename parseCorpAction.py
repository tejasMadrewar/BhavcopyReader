import pandas as pd
import datetime
import time
import requests
import json
import os
from pprint import pprint
from multiprocessing import Pool

from config import SQL_CON, DOWNLOAD_FOLDER


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


def main():
    update()


if __name__ == "__main__":
    main()
