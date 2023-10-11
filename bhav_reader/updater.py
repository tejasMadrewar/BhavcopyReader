from downloaders import (
    przip_downloader,
    index_downloader,
    pdddmmyy_reader,
    pd_move_to_model,
    namechange_model,
    corp_action,
    bse_corpaction,
)


def main():
    # download PR files
    n = 60
    przip_downloader.update(n)
    przip_downloader.update(n)
    # update pd file data
    pdddmmyy_reader.update(n)
    pd_move_to_model.update(n)
    # update corp actions
    corp_action.update(n)
    # update name change
    namechange_model.update()
    # download Index data
    index_downloader.update()
    # update bse corp action
    bse_corpaction.update()


if __name__ == "__main__":
    main()
