import datetime
import os.path
import requests
from multiprocessing import Pool


def PRZip_download_for_day(day, download_folder):
    PRZip_filename = day.strftime("PR%d%m%y.zip")
    file_path = os.path.join(download_folder, PRZip_filename)
    if os.path.isfile(file_path):
        print(f"\t{PRZip_filename} already exists.")
        return
    # https://www1.nseindia.com/archives/equities/bhavcopy/pr/PR061222.zip
    url = f"https://www1.nseindia.com/archives/equities/bhavcopy/pr/{PRZip_filename}"
    url = (
        f"https://archives.nseindia.com/archives/equities/bhavcopy/pr/{PRZip_filename}"
    )
    for i in range(5):
        try:
            r = requests.get(url, timeout=5, allow_redirects=False)
            if r.status_code == 404 or r.status_code == 302 or r.status_code != 200:
                print(f"\tCheck if its a Holiday {PRZip_filename} {r.status_code}")
                return
            with open(file_path, "wb") as f:
                f.write(r.content)
            print("\tSuccess: ", url, r.status_code)
            break
        except Exception as e:
            print("%s" % (format(e)) + " Or Check if Its a Holiday. " + str(i))


def pool_handler(func, args, poolSize=5):
    p = Pool(poolSize)
    p.starmap(func, args)


def PRZip_download_for_days(days: int, folder_location: str):
    print(f"Downloading PR zip files for {len(days)} days..")
    # for day in days:
    # PRZip_download_for_day(day, folder_location)
    args = [(day, folder_location) for day in days]
    pool_handler(PRZip_download_for_day, args)
    print(f"Downloading Finished.")


def PRZip_download_last_n_days(days: int, folder: str):
    today = datetime.datetime.today().date()
    dates = [today - datetime.timedelta(days=i) for i in range(days + 1)]
    PRZip_download_for_days(dates, folder)


def update(days: int, download_folder: str):
    PRZip_download_last_n_days(days, download_folder)
