import datetime
import os.path
import requests

import PdddmmyyReader as PRReader


def PRZip_download_for_day(day, folder_location):
    PRZip_filename = day.strftime("PR%d%m%y.zip")
    # print(PRZip_filename)
    file_path = os.path.join(folder_location, PRZip_filename)
    # print(file_path)
    if os.path.isfile(file_path):
        print(f"{PRZip_filename} already exists.")
        return
    # https://www1.nseindia.com/archives/equities/bhavcopy/pr/PR061222.zip
    url = f"https://www1.nseindia.com/archives/equities/bhavcopy/pr/{PRZip_filename}"
    try:
        r = requests.get(url, timeout=2)
        if r.status_code == 404:
            print(f"check if its a Holiday {PRZip_filename}")
            return
        with open(file_path, "wb") as f:
            f.write(r.content)
        print("Success: ", url, r.status_code)
    except Exception as e:
        print("%s" % (format(e)) + " Or Check if Its a Holiday")


def PRZip_download_for_days(days, folder_location):
    for day in days:
        PRZip_download_for_day(day, folder_location)


def PRZip_download_last_n_days(n, folder_location):
    today = datetime.datetime.today().date()
    days = [today - datetime.timedelta(days=i)
            for i in range(n+1)]
    PRZip_download_for_days(days, folder_location)


def main():
    day = datetime.datetime(year=2022, month=12, day=29)
    day = datetime.datetime(year=2022, month=12, day=30)
    # PRZip_download_for_day(day, PRReader.BHAV_CPY_FLDER_PTH)
    PRZip_download_last_n_days(10, PRReader.BHAV_CPY_FLDER_PTH)


if __name__ == "__main__":
    main()
