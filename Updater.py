import PRZipDownloader
import IndexDownloader
import PdddmmyyReader
import pd_move_to_model
import nameChangeModel
import corpAction


def main():
    # download PR files
    n = 250
    PRZipDownloader.update(n)
    PRZipDownloader.update(n)
    # update pd file data
    PdddmmyyReader.update(n)
    pd_move_to_model.update(n)
    # update corp actions
    corpAction.update(n)
    # update name change
    # nameChangeModel.update()
    # download Index data
    IndexDownloader.update()


if __name__ == "__main__":
    main()
