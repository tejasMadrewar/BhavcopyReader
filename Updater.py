import PRZipDownloader
import IndexDownloader
import PdddmmyyReader
import pd_move_to_model
import nameChangeModel
import corpAction


def main():
    # download PR files
    PRZipDownloader.update()
    PRZipDownloader.update()
    # update pd file data
    PdddmmyyReader.update()
    pd_move_to_model.update()
    # update corp actions
    corpAction.update()
    # update name change
    # nameChangeModel.update()
    # download Index data
    IndexDownloader.update()


if __name__ == "__main__":
    main()
