from bhav_reader.downloaders import (
    przip_downloader,
    index_downloader,
    pdddmmyy_reader,
    pd_move_to_model,
    namechange_model,
    corp_action,
    bse_corpaction,
)

from config import DEFAULT_ENGINE, DOWNLOAD_FOLDER


def update_all(days: int, engine, download_folder: str):
    # download PR files
    przip_downloader.update(days, download_folder)
    przip_downloader.update(days, download_folder)
    # update pd file data
    pdddmmyy_reader.update(days, engine, download_folder)
    pd_move_to_model.update(days)
    # update corp actions
    corp_action.update(days, engine, download_folder)
    # update name change
    namechange_model.update(engine)
    # download Index data
    index_downloader.update(download_folder)
    # update bse corp action
    bse_corpaction.update(engine, download_folder)


if __name__ == "__main__":
    update_all(30, DEFAULT_ENGINE, DOWNLOAD_FOLDER)
