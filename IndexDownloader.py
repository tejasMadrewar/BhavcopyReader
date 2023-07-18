import requests
import datetime
import os
import time

import config as cfg

indexData = {
    "50": "https://archives.nseindia.com/content/indices/ind_nifty50list.csv",
    "n50": "https://www.niftyindices.com/IndexConstituent/ind_niftynext50list.csv",
    "100": "https://archives.nseindia.com/content/indices/ind_nifty100list.csv",
    "200": "https://archives.nseindia.com/content/indices/ind_nifty200list.csv",
    "500": "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
    "midcap150": "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv",
    "midcap100": "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap100list.csv",
    "midcap50": "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap50list.csv",
    "smallcap250": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv",
    "smallcap100": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap100list.csv",
    "smallcap50": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap50list.csv",
    "nifty200mom30": "https://www.niftyindices.com/IndexConstituent/ind_nifty200Momentum30_list.csv",
    "alpha50": "https://www.niftyindices.com/IndexConstituent/ind_nifty_Alpha_Index.csv",
    "All": "https://archives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv",
}

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, "
    "like Gecko) "
    "Chrome/80.0.3987.149 Safari/537.36",
    "accept-language": "en,gu;q=0.9,hi;q=0.8",
    "accept-encoding": "gzip, deflate, br",
}

timeout = 30


def get_data(url):
    session = requests.Session()
    request = session.get(url, headers=headers, timeout=timeout)
    cookies = dict(request.cookies)
    response = session.get(url, headers=headers, timeout=timeout, cookies=cookies)
    return response.text


def downloadIndex(indexName, downloadFolder):
    print(f"Downloading index data of {indexName}")
    if indexName in indexData:
        url = indexData[indexName]
        text = get_data(url)
        with open(os.path.join(downloadFolder, url.split("/")[-1]), "w") as f:
            f.write(text)
    else:
        print(f"URL for {indexName} does not exists.")


def downloadAll(downloadFolder):
    folder = os.path.join(
        downloadFolder, datetime.date.today().strftime("%Y-%m-%d_index_data")
    )
    if not os.path.exists(folder):
        os.makedirs(folder)
    for i in indexData:
        downloadIndex(i, folder)
        time.sleep(2)


def update():
    downloadAll(cfg.DOWNLOAD_FOLDER)


if __name__ == "__main__":
    update()
