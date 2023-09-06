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
    "All": "https://archives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv",
    # sector indices
    "auto": "https://www.niftyindices.com/IndexConstituent/ind_niftyautolist.csv",
    "bank": "https://www.niftyindices.com/IndexConstituent/ind_niftybanklist.csv",
    "finance": "https://www.niftyindices.com/IndexConstituent/ind_niftyfinancelist.csv",
    "finance2550": "https://www.niftyindices.com/IndexConstituent/ind_niftyfinancialservices25-50list.csv",
    "financeservicesExBank": "https://www.niftyindices.com/IndexConstituent/ind_niftyfinancialservicesexbank_list.xlsx",
    "fmcg": "https://www.niftyindices.com/IndexConstituent/ind_niftyfmcglist.csv",
    "healthCare": "https://www.niftyindices.com/IndexConstituent/ind_niftyhealthcarelist.csv",
    "it": "https://www.niftyindices.com/IndexConstituent/ind_niftyitlist.csv",
    "media": "https://www.niftyindices.com/IndexConstituent/ind_niftymedialist.csv",
    "metal": "https://www.niftyindices.com/IndexConstituent/ind_niftymetallist.csv",
    "pharma": "https://www.niftyindices.com/IndexConstituent/ind_niftypharmalist.csv",
    "privateBank": "https://www.niftyindices.com/IndexConstituent/ind_nifty_privatebanklist.csv",
    "psuBank": "https://www.niftyindices.com/IndexConstituent/ind_niftypsubanklist.csv",
    "realty": "https://www.niftyindices.com/IndexConstituent/ind_niftyrealtylist.csv",
    "consuDura": "https://www.niftyindices.com/IndexConstituent/ind_niftyconsumerdurableslist.csv",
    "oilGas": "https://www.niftyindices.com/IndexConstituent/ind_niftyoilgaslist.csv",
    "midSmlFinServ": "https://www.niftyindices.com/IndexConstituent/ind_niftymidsmallfinancailservice_list.csv",
    "midSmlHealth": "https://www.niftyindices.com/IndexConstituent/ind_niftymidsmallhealthcare_list.csv",
    "midSmlItTele": "https://www.niftyindices.com/IndexConstituent/ind_niftymidsmallitAndtelecom_list.csv",
    # strategy
    "100eq": "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv",
    "100lowVol": "https://www.niftyindices.com/IndexConstituent/ind_Nifty100LowVolatility30list.csv",
    "200mom30": "https://www.niftyindices.com/IndexConstituent/ind_nifty200Momentum30_list.csv",
    "200alpha30": "https://www.niftyindices.com/IndexConstituent/ind_nifty200alpha30_list.csv",
    "100alpha30": "https://www.niftyindices.com/IndexConstituent/ind_nifty100Alpha30list.csv",
    "alpha50": "https://www.niftyindices.com/IndexConstituent/ind_nifty_Alpha_Index.csv",
    "alphalowvol30": "https://www.niftyindices.com/IndexConstituent/ind_nifty_alpha_lowvol30list.csv",
    "qualowvol30": "https://www.niftyindices.com/IndexConstituent/ind_nifty_alpha_quality_lowvol30list.csv",
    "alphaquavallowvol30": "https://www.niftyindices.com/IndexConstituent/ind_nifty_alpha_quality_value_lowvol30list.csv",
    "div50": "https://www.niftyindices.com/IndexConstituent/ind_niftydivopp50list.csv",
    "growth15": "https://www.niftyindices.com/IndexConstituent/ind_NiftyGrowth_Sectors15_Index.csv",
    "highBeta50": "https://www.niftyindices.com/IndexConstituent/nifty_High_Beta50_Index.csv",
    "lowvol50": "https://www.niftyindices.com/IndexConstituent/nifty_low_Volatility50_Index.csv",
    "100qua30": "https://www.niftyindices.com/IndexConstituent/ind_nifty100Quality30list.csv",
    "mid150mom50": "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150momentum50_list.csv",
    "mid150qua50": "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150quality50list.csv",
    "sml250qua50": "https://www.niftyindices.com/IndexConstituent/ind_niftySmallcap250_Quality50_list.csv",
    "qualowvol30": "https://www.niftyindices.com/IndexConstituent/ind_nifty_quality_lowvol30list.csv",
    "50eq": "https://www.niftyindices.com/IndexConstituent/ind_Nifty50EqualWeight.csv",
    "50val20": "https://www.niftyindices.com/IndexConstituent/ind_Nifty50_Value20.csv",
    "500val50": "https://www.niftyindices.com/IndexConstituent/ind_nifty500Value50_list.csv",
    "500val50": "https://www.niftyindices.com/IndexConstituent/ind_nifty500Value50_list.csv",
    "200qua30": "https://www.niftyindices.com/IndexConstituent/ind_nifty200Quality30_list.csv",
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
    print(f"Downloading index data of '{indexName}'")
    if indexName in indexData:
        url = indexData[indexName]
        filename = url.split("/")[-1]
        os.makedirs(downloadFolder, exist_ok=True)
        if os.path.isfile(os.path.join(downloadFolder, filename)):
            print(f"'{filename}' file already exists. Skipping download..")
            return
        time.sleep(1)
        text = get_data(url)
        with open(os.path.join(downloadFolder, filename), "w") as f:
            f.write(text)
    else:
        print(f"URL for {indexName} does not exists.")


def downloadAll(downloadFolder):
    folder = os.path.join(
        downloadFolder, datetime.date.today().strftime("index_data/%Y-%m-%d")
    )
    for i in indexData:
        downloadIndex(i, folder)


def update():
    downloadAll(cfg.DOWNLOAD_FOLDER)


if __name__ == "__main__":
    update()
