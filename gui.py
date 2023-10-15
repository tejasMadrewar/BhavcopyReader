from bhav_reader.bhavgui import bhavgui
from config import SQL_CON, DOWNLOAD_FOLDER

if __name__ == "__main__":
    bhavgui.run(SQL_CON, DOWNLOAD_FOLDER)
