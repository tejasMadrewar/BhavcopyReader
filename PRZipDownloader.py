import datetime
import os.path
import requests
from multiprocessing import Pool

import config as cfg


def PRZip_download_for_day(day, folder_location):
    PRZip_filename = day.strftime("PR%d%m%y.zip")
    file_path = os.path.join(folder_location, PRZip_filename)
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
            if r.status_code == 404 or r.status_code == 302:
                print(f"\tCheck if its a Holiday {PRZip_filename}")
                return
            with open(file_path, "wb") as f:
                f.write(r.content)
            print("\tSuccess: ", url, r.status_code)
            break
        except Exception as e:
            print("%s" % (format(e)) + " Or Check if Its a Holiday. " + str(i))


def pool_handler(func, args, poolSize=4):
    p = Pool(poolSize)
    p.starmap(func, args)


def PRZip_download_for_days(days, folder_location):
    print(f"Downloading PR zip files for {len(days)} days..")
    # for day in days:
    # PRZip_download_for_day(day, folder_location)
    args = [(day, folder_location) for day in days]
    pool_handler(PRZip_download_for_day, args)
    print(f"Downloading Finished.")


def PRZip_download_last_n_days(n, folder_location):
    today = datetime.datetime.today().date()
    days = [today - datetime.timedelta(days=i) for i in range(n + 1)]
    PRZip_download_for_days(days, folder_location)


def update(n=30):
    PRZip_download_last_n_days(n, cfg.DOWNLOAD_FOLDER)


def main():
    update()


if __name__ == "__main__":
    main()
