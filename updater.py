from bhav_reader.update import update_all
from config import DEFAULT_ENGINE, DOWNLOAD_FOLDER


if __name__ == "__main__":
    update_all(30, DEFAULT_ENGINE, DOWNLOAD_FOLDER)
